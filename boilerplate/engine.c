/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Implements:
 *   - control-plane IPC via UNIX domain socket
 *   - container lifecycle and metadata synchronization
 *   - clone + namespace setup for each container
 *   - producer/consumer behavior for log buffering
 *   - signal handling and graceful shutdown
 *   - child reaping via SIGCHLD
 *   - CMD_START, CMD_RUN, CMD_PS, CMD_LOGS, CMD_STOP
 */

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE           (1024 * 1024)
#define CONTAINER_ID_LEN     32
#define CONTROL_PATH         "/tmp/mini_runtime.sock"
#define LOG_DIR              "logs"
#define CONTROL_MESSAGE_LEN  4096   /* enlarged so ps/logs fit many containers */
#define CHILD_COMMAND_LEN    256
#define LOG_CHUNK_SIZE       4096
#define LOG_BUFFER_CAPACITY  16
#define DEFAULT_SOFT_LIMIT   (40UL << 20)
#define DEFAULT_HARD_LIMIT   (64UL << 20)

/* ------------------------------------------------------------------ */
/*  Enumerations                                                        */
/* ------------------------------------------------------------------ */

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

/* ------------------------------------------------------------------ */
/*  Data structures                                                     */
/* ------------------------------------------------------------------ */

typedef struct container_record {
    char               id[CONTAINER_ID_LEN];
    pid_t              host_pid;
    time_t             started_at;
    container_state_t  state;
    unsigned long      soft_limit_bytes;
    unsigned long      hard_limit_bytes;
    int                exit_code;
    int                exit_signal;
    char               log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char   container_id[CONTAINER_ID_LEN];
    size_t length;
    char   data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t        items[LOG_BUFFER_CAPACITY];
    size_t            head;
    size_t            tail;
    size_t            count;
    int               shutting_down;
    pthread_mutex_t   mutex;
    pthread_cond_t    not_empty;
    pthread_cond_t    not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t  kind;
    char            container_id[CONTAINER_ID_LEN];
    char            rootfs[PATH_MAX];
    char            command[CHILD_COMMAND_LEN];
    unsigned long   soft_limit_bytes;
    unsigned long   hard_limit_bytes;
    int             nice_value;
} control_request_t;

typedef struct {
    int  status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char  id[CONTAINER_ID_LEN];
    char  rootfs[PATH_MAX];
    char  command[CHILD_COMMAND_LEN];
    int   nice_value;
    int   log_write_fd;
} child_config_t;

typedef struct {
    int              server_fd;
    int              monitor_fd;
    volatile int     should_stop;
    pthread_t        logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t  metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

/* Global pointer used by the SIGCHLD / SIGTERM handlers */
static supervisor_ctx_t *g_ctx = NULL;

/* ------------------------------------------------------------------ */
/*  Usage / argument parsing                                            */
/* ------------------------------------------------------------------ */

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run   <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                           const char *value,
                           unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }
    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }
    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                 int argc, char *argv[],
                                 int start_index)
{
    int i;
    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long  nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }
        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }
        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }
        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }
        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }
    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/*  Helpers                                                             */
/* ------------------------------------------------------------------ */

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING: return "starting";
    case CONTAINER_RUNNING:  return "running";
    case CONTAINER_STOPPED:  return "stopped";
    case CONTAINER_KILLED:   return "killed";
    case CONTAINER_EXITED:   return "exited";
    default:                 return "unknown";
    }
}

/* ------------------------------------------------------------------ */
/*  Bounded buffer                                                      */
/* ------------------------------------------------------------------ */

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;
    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0) return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) { pthread_mutex_destroy(&buffer->mutex); return rc; }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }
    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

/*
 * Producer: insert a log chunk into the buffer.
 * Blocks while full (unless shutdown is in progress).
 * Returns 0 on success, -1 if shutdown was triggered.
 */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * Consumer: remove a log chunk from the buffer.
 * Blocks while empty (unless shutdown is in progress).
 * Drains remaining items even after shutdown begins.
 * Returns 0 on success, -1 when shutdown AND buffer is empty.
 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    /* Fully drained during shutdown */
    if (buffer->count == 0 && buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/* ------------------------------------------------------------------ */
/*  Logging consumer thread                                             */
/* ------------------------------------------------------------------ */

/*
 * Runs for the lifetime of the supervisor.
 * Pops log chunks and appends them to per-container log files.
 * Exits only after shutdown is signalled AND the buffer is drained.
 */
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;

    mkdir(LOG_DIR, 0755);

    while (bounded_buffer_pop(&ctx->log_buffer, &item) == 0) {
        char filepath[PATH_MAX];
        snprintf(filepath, sizeof(filepath), "%s/%s.log", LOG_DIR, item.container_id);

        FILE *f = fopen(filepath, "a");
        if (f) {
            fwrite(item.data, 1, item.length, f);
            fclose(f);
        }
    }

    return NULL;
}

/* ------------------------------------------------------------------ */
/*  Container child entrypoint (runs inside the new namespaces)         */
/* ------------------------------------------------------------------ */

/*
 * Runs inside the cloned child process.
 *  - Sets isolated hostname (UTS namespace)
 *  - Chroots into the container rootfs (Mount namespace)
 *  - Mounts /proc so process tools work
 *  - Applies nice value if requested
 *  - Redirects stdout/stderr to the logging pipe
 *  - Exec's the requested command via /bin/sh -c
 */
int child_fn(void *arg)
{
    child_config_t *config = (child_config_t *)arg;

    /* 1. Set container hostname */
    if (sethostname(config->id, strlen(config->id)) != 0)
        perror("sethostname");

    /* 2. Chroot into the container rootfs */
    if (chroot(config->rootfs) != 0) {
        perror("chroot failed");
        return 1;
    }
    if (chdir("/") != 0) {
        perror("chdir / failed");
        return 1;
    }

    /* 3. Mount /proc inside the container */
    if (mount("proc", "/proc", "proc", 0, NULL) != 0)
        perror("Warning: mount /proc failed");

    /* 4. Apply nice value if set */
    if (config->nice_value != 0) {
        if (nice(config->nice_value) == -1)
            perror("Warning: nice failed");
    }

    /* 5. Redirect stdout/stderr to the log pipe */
    if (config->log_write_fd >= 0) {
        if (dup2(config->log_write_fd, STDOUT_FILENO) < 0)
            perror("dup2 stdout");
        if (dup2(config->log_write_fd, STDERR_FILENO) < 0)
            perror("dup2 stderr");
        close(config->log_write_fd);
    }

    /* 6. Execute the command */
    char *argv[] = { "/bin/sh", "-c", config->command, NULL };
    execvp("/bin/sh", argv);

    /* execvp only returns on failure */
    perror("execvp failed");
    return 1;
}

/* ------------------------------------------------------------------ */
/*  Container launch helper                                             */
/* ------------------------------------------------------------------ */

/*
 * Allocates a stack, clones a child with PID/UTS/Mount namespace isolation,
 * and returns the child PID on the host.
 *
 * NOTE: The stack is intentionally not freed here because the child process
 * continues to use it after clone() returns in the parent.  We free it only
 * after the child is reaped (see the SIGCHLD handler).  To keep the demo
 * simple we store the stack pointer in child_config_t so the reaper can
 * free it.
 */
static pid_t launch_container(child_config_t *config, void **stack_out)
{
    void *stack = malloc(STACK_SIZE);
    if (!stack) {
        perror("malloc stack");
        return -1;
    }

    void *stack_top = (char *)stack + STACK_SIZE;
    int   flags     = CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD;

    pid_t pid = clone(child_fn, stack_top, flags, config);
    if (pid == -1) {
        perror("clone failed");
        free(stack);
        return -1;
    }

    *stack_out = stack;   /* caller is responsible for freeing after reap */
    return pid;
}

/* ------------------------------------------------------------------ */
/*  Kernel monitor helpers                                              */
/* ------------------------------------------------------------------ */

int register_with_monitor(int monitor_fd,
                           const char *container_id,
                           pid_t host_pid,
                           unsigned long soft_limit_bytes,
                           unsigned long hard_limit_bytes)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid               = host_pid;
    req.soft_limit_bytes  = soft_limit_bytes;
    req.hard_limit_bytes  = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;
    return 0;
}

int unregister_from_monitor(int monitor_fd,
                             const char *container_id,
                             pid_t host_pid)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;
    return 0;
}

/* ------------------------------------------------------------------ */
/*  Log producer thread (one per container)                            */
/* ------------------------------------------------------------------ */

typedef struct {
    int    read_fd;
    char   container_id[CONTAINER_ID_LEN];
    supervisor_ctx_t *ctx;
} producer_args_t;

void *producer_thread(void *arg)
{
    producer_args_t *pargs = (producer_args_t *)arg;
    log_item_t       item;
    ssize_t          bytes_read;

    memset(&item, 0, sizeof(item));
    strncpy(item.container_id, pargs->container_id, sizeof(item.container_id) - 1);

    while ((bytes_read = read(pargs->read_fd, item.data, LOG_CHUNK_SIZE)) > 0) {
        item.length = (size_t)bytes_read;
        if (bounded_buffer_push(&pargs->ctx->log_buffer, &item) != 0)
            break;
    }

    close(pargs->read_fd);
    free(pargs);
    return NULL;
}

/* ------------------------------------------------------------------ */
/*  Signal handlers                                                     */
/* ------------------------------------------------------------------ */

/*
 * SIGCHLD handler: reap all finished children and update their
 * container_record state to CONTAINER_EXITED (or CONTAINER_KILLED).
 * Uses WNOHANG so it never blocks.
 */
static void sigchld_handler(int sig)
{
    (void)sig;
    int   status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        if (!g_ctx) continue;

        pthread_mutex_lock(&g_ctx->metadata_lock);
        container_record_t *rec = g_ctx->containers;
        while (rec) {
            if (rec->host_pid == pid) {
                if (WIFEXITED(status)) {
                    rec->exit_code  = WEXITSTATUS(status);
                    rec->exit_signal = 0;
                    /* Only move to EXITED if it wasn't deliberately stopped */
                    if (rec->state != CONTAINER_STOPPED)
                        rec->state = CONTAINER_EXITED;
                } else if (WIFSIGNALED(status)) {
                    rec->exit_signal = WTERMSIG(status);
                    rec->exit_code   = -1;
                    if (rec->state != CONTAINER_STOPPED)
                        rec->state = CONTAINER_KILLED;
                }

                /* Unregister from the kernel monitor */
                if (g_ctx->monitor_fd >= 0)
                    unregister_from_monitor(g_ctx->monitor_fd, rec->id, pid);

                break;
            }
            rec = rec->next;
        }
        pthread_mutex_unlock(&g_ctx->metadata_lock);
    }
}

/*
 * SIGTERM / SIGINT handler: ask the supervisor event loop to stop.
 */
static void sigterm_handler(int sig)
{
    (void)sig;
    if (g_ctx)
        g_ctx->should_stop = 1;

    /* Wake the accept() call if it is blocked */
    if (g_ctx && g_ctx->server_fd >= 0)
        shutdown(g_ctx->server_fd, SHUT_RDWR);
}

/* ------------------------------------------------------------------ */
/*  Supervisor: handle a single CMD_START request                       */
/* ------------------------------------------------------------------ */

static void handle_start(supervisor_ctx_t *ctx,
                          const control_request_t *req,
                          control_response_t *resp)
{
    child_config_t config;
    memset(&config, 0, sizeof(config));
    strncpy(config.id,      req->container_id, CONTAINER_ID_LEN - 1);
    strncpy(config.rootfs,  req->rootfs,        PATH_MAX - 1);
    strncpy(config.command, req->command,       CHILD_COMMAND_LEN - 1);
    config.nice_value = req->nice_value;

    int log_pipe[2];
    if (pipe(log_pipe) != 0) {
        snprintf(resp->message, sizeof(resp->message),
                 "ERROR: pipe() failed: %s\n", strerror(errno));
        return;
    }
    config.log_write_fd = log_pipe[1];

    void  *stack = NULL;
    pid_t  pid   = launch_container(&config, &stack);
    if (pid <= 0) {
        close(log_pipe[0]);
        close(log_pipe[1]);
        snprintf(resp->message, sizeof(resp->message),
                 "ERROR: Failed to start container '%s'\n", req->container_id);
        return;
    }

    /* Register with kernel monitor */
    if (ctx->monitor_fd >= 0)
        register_with_monitor(ctx->monitor_fd, config.id, pid,
                               req->soft_limit_bytes, req->hard_limit_bytes);

    /* Close write-end in supervisor — only the child writes to it */
    close(log_pipe[1]);

    /* Spin up the log producer thread for this container */
    producer_args_t *pargs = malloc(sizeof(producer_args_t));
    if (pargs) {
        pargs->read_fd = log_pipe[0];
        strncpy(pargs->container_id, config.id, CONTAINER_ID_LEN - 1);
        pargs->ctx = ctx;
        pthread_t prod_tid;
        if (pthread_create(&prod_tid, NULL, producer_thread, pargs) == 0)
            pthread_detach(prod_tid);
        else {
            free(pargs);
            close(log_pipe[0]);
        }
    } else {
        close(log_pipe[0]);
    }

    /* Record container metadata */
    container_record_t *rec = calloc(1, sizeof(container_record_t));
    if (rec) {
        strncpy(rec->id, config.id, CONTAINER_ID_LEN - 1);
        rec->host_pid          = pid;
        rec->started_at        = time(NULL);
        rec->state             = CONTAINER_RUNNING;
        rec->soft_limit_bytes  = req->soft_limit_bytes;
        rec->hard_limit_bytes  = req->hard_limit_bytes;
        snprintf(rec->log_path, PATH_MAX, "%s/%s.log", LOG_DIR, config.id);

        pthread_mutex_lock(&ctx->metadata_lock);
        rec->next      = ctx->containers;
        ctx->containers = rec;
        pthread_mutex_unlock(&ctx->metadata_lock);
    }

    snprintf(resp->message, sizeof(resp->message),
             "SUCCESS: Container '%s' started with Host PID: %d\n",
             config.id, pid);
    resp->status = 0;
}

/* ------------------------------------------------------------------ */
/*  Supervisor: handle a CMD_RUN request (foreground — wait for exit)  */
/* ------------------------------------------------------------------ */

static void handle_run(supervisor_ctx_t *ctx,
                        const control_request_t *req,
                        control_response_t *resp,
                        int client_fd)
{
    /*
     * CMD_RUN starts the container identically to CMD_START but then
     * blocks in waitpid() until the container exits, and sends the
     * exit status back to the client.
     */
    handle_start(ctx, req, resp);
    if (resp->status != 0) {
        /* start already failed — response is already set */
        return;
    }

    /* Find the PID we just recorded */
    pid_t pid = -1;
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *rec = ctx->containers;
    while (rec) {
        if (strcmp(rec->id, req->container_id) == 0) {
            pid = rec->host_pid;
            break;
        }
        rec = rec->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (pid <= 0) {
        snprintf(resp->message, sizeof(resp->message),
                 "ERROR: Could not find PID for container '%s'\n",
                 req->container_id);
        resp->status = 1;
        return;
    }

    /*
     * Send an interim "started" message so the client knows we launched,
     * then block until the container exits.
     */
    snprintf(resp->message, sizeof(resp->message),
             "INFO: Container '%s' (PID %d) running — waiting for exit...\n",
             req->container_id, pid);
    (void)write(client_fd, resp, sizeof(*resp));

    int   wait_status;
    pid_t reaped = waitpid(pid, &wait_status, 0);

    /* Update state in metadata */
    pthread_mutex_lock(&ctx->metadata_lock);
    rec = ctx->containers;
    while (rec) {
        if (rec->host_pid == pid) {
            if (WIFEXITED(wait_status)) {
                rec->exit_code   = WEXITSTATUS(wait_status);
                rec->exit_signal = 0;
                rec->state       = CONTAINER_EXITED;
            } else if (WIFSIGNALED(wait_status)) {
                rec->exit_signal = WTERMSIG(wait_status);
                rec->exit_code   = -1;
                rec->state       = CONTAINER_KILLED;
            }
            if (ctx->monitor_fd >= 0)
                unregister_from_monitor(ctx->monitor_fd, rec->id, pid);
            break;
        }
        rec = rec->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (reaped > 0 && WIFEXITED(wait_status)) {
        snprintf(resp->message, sizeof(resp->message),
                 "Container '%s' exited with code %d\n",
                 req->container_id, WEXITSTATUS(wait_status));
        resp->status = WEXITSTATUS(wait_status);
    } else if (reaped > 0 && WIFSIGNALED(wait_status)) {
        snprintf(resp->message, sizeof(resp->message),
                 "Container '%s' killed by signal %d\n",
                 req->container_id, WTERMSIG(wait_status));
        resp->status = 1;
    } else {
        snprintf(resp->message, sizeof(resp->message),
                 "Container '%s' wait failed: %s\n",
                 req->container_id, strerror(errno));
        resp->status = 1;
    }
}

/* ------------------------------------------------------------------ */
/*  Supervisor: handle a CMD_PS request                                 */
/* ------------------------------------------------------------------ */

static void handle_ps(supervisor_ctx_t *ctx, control_response_t *resp)
{
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *curr = ctx->containers;

    int offset = 0;
    offset += snprintf(resp->message + offset, sizeof(resp->message) - offset,
                       "%-20s %-8s %-10s %-10s %-10s\n",
                       "CONTAINER ID", "PID", "STATE", "SOFT(MB)", "HARD(MB)");
    offset += snprintf(resp->message + offset, sizeof(resp->message) - offset,
                       "%-20s %-8s %-10s %-10s %-10s\n",
                       "--------------------", "--------",
                       "----------", "----------", "----------");

    if (!curr) {
        snprintf(resp->message + offset, sizeof(resp->message) - offset,
                 "(no containers)\n");
    }

    while (curr && offset < (int)sizeof(resp->message) - 1) {
        offset += snprintf(resp->message + offset, sizeof(resp->message) - offset,
                           "%-20s %-8d %-10s %-10lu %-10lu\n",
                           curr->id,
                           curr->host_pid,
                           state_to_string(curr->state),
                           curr->soft_limit_bytes / (1024 * 1024),
                           curr->hard_limit_bytes / (1024 * 1024));
        curr = curr->next;
    }

    pthread_mutex_unlock(&ctx->metadata_lock);
    resp->status = 0;
}

/* ------------------------------------------------------------------ */
/*  Supervisor: handle a CMD_LOGS request                               */
/* ------------------------------------------------------------------ */

/*
 * Reads the on-disk log file for the given container and streams its
 * content back to the client in CONTROL_MESSAGE_LEN-sized chunks.
 * The client receives multiple response packets; an empty message
 * signals end-of-file.
 */
static void handle_logs(supervisor_ctx_t *ctx,
                         const control_request_t *req,
                         control_response_t *resp,
                         int client_fd)
{
    /* Verify the container exists */
    char log_path[PATH_MAX];
    int  found = 0;

    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *rec = ctx->containers;
    while (rec) {
        if (strcmp(rec->id, req->container_id) == 0) {
            strncpy(log_path, rec->log_path, PATH_MAX - 1);
            found = 1;
            break;
        }
        rec = rec->next;
    }
    pthread_mutex_unlock(&ctx->metadata_lock);

    if (!found) {
        /* Fall back to the standard log path even if metadata is missing */
        snprintf(log_path, PATH_MAX, "%s/%s.log", LOG_DIR, req->container_id);
    }

    FILE *f = fopen(log_path, "r");
    if (!f) {
        snprintf(resp->message, sizeof(resp->message),
                 "ERROR: No logs found for container '%s' (tried: %s)\n",
                 req->container_id, log_path);
        resp->status = 1;
        (void)write(client_fd, resp, sizeof(*resp));
        return;
    }

    /* Stream the file in chunks */
    size_t bytes;
    while ((bytes = fread(resp->message, 1, sizeof(resp->message) - 1, f)) > 0) {
        resp->message[bytes] = '\0';
        resp->status = 0;
        (void)write(client_fd, resp, sizeof(*resp));
    }
    fclose(f);

    /* Send an empty terminator packet */
    memset(resp, 0, sizeof(*resp));
    resp->status = 0;
    resp->message[0] = '\0';
    (void)write(client_fd, resp, sizeof(*resp));
}

/* ------------------------------------------------------------------ */
/*  Supervisor: handle a CMD_STOP request                               */
/* ------------------------------------------------------------------ */

static void handle_stop(supervisor_ctx_t *ctx,
                         const control_request_t *req,
                         control_response_t *resp)
{
    pthread_mutex_lock(&ctx->metadata_lock);
    container_record_t *curr = ctx->containers;
    int found = 0;

    while (curr) {
        if (strcmp(curr->id, req->container_id) == 0 &&
            curr->state == CONTAINER_RUNNING) {
            curr->state = CONTAINER_STOPPED;
            kill(curr->host_pid, SIGKILL);
            found = 1;
            snprintf(resp->message, sizeof(resp->message),
                     "SUCCESS: Stopped container '%s' (PID %d)\n",
                     req->container_id, curr->host_pid);
            break;
        }
        curr = curr->next;
    }

    if (!found) {
        snprintf(resp->message, sizeof(resp->message),
                 "ERROR: Container '%s' not found or not running.\n",
                 req->container_id);
        resp->status = 1;
    }

    pthread_mutex_unlock(&ctx->metadata_lock);
}

/* ------------------------------------------------------------------ */
/*  Supervisor main loop                                                */
/* ------------------------------------------------------------------ */

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;

    (void)rootfs; /* base rootfs available for future overlay use */

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd  = -1;
    ctx.monitor_fd = -1;
    g_ctx = &ctx;

    /* --- mutex init --- */
    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) { errno = rc; perror("pthread_mutex_init"); return 1; }

    /* --- bounded buffer init --- */
    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc; perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    /* --- kernel monitor (optional) --- */
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR | O_CLOEXEC);
    if (ctx.monitor_fd < 0)
        fprintf(stderr,
                "Warning: /dev/container_monitor unavailable — "
                "memory limits won't be enforced.\n");

    /* --- install signal handlers --- */
    struct sigaction sa_chld, sa_term;
    memset(&sa_chld, 0, sizeof(sa_chld));
    sa_chld.sa_handler = sigchld_handler;
    sa_chld.sa_flags   = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa_chld, NULL);

    memset(&sa_term, 0, sizeof(sa_term));
    sa_term.sa_handler = sigterm_handler;
    sigaction(SIGTERM, &sa_term, NULL);
    sigaction(SIGINT,  &sa_term, NULL);

    /* --- UNIX domain socket --- */
    unlink(CONTROL_PATH);

    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) { perror("socket"); return 1; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind"); return 1;
    }
    if (listen(ctx.server_fd, 10) < 0) {
        perror("listen"); return 1;
    }
    printf("[Supervisor] Listening on %s\n", CONTROL_PATH);

    /* --- start logging consumer thread --- */
    if (pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx) != 0) {
        perror("pthread_create logger"); return 1;
    }

    /* ----------------------------------------------------------------
     * Event loop
     * ---------------------------------------------------------------- */
    while (!ctx.should_stop) {
        int client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR || errno == EBADF)
                continue; /* interrupted by signal or shutdown */
            perror("accept");
            continue;
        }

        control_request_t  req;
        control_response_t resp;
        memset(&resp, 0, sizeof(resp));

        ssize_t n = read(client_fd, &req, sizeof(req));
        if (n != (ssize_t)sizeof(req)) {
            close(client_fd);
            continue;
        }

        switch (req.kind) {
        case CMD_START:
            handle_start(&ctx, &req, &resp);
            (void)write(client_fd, &resp, sizeof(resp));
            break;

        case CMD_RUN:
            /* handle_run streams multiple packets itself */
            handle_run(&ctx, &req, &resp, client_fd);
            (void)write(client_fd, &resp, sizeof(resp));
            break;

        case CMD_PS:
            handle_ps(&ctx, &resp);
            (void)write(client_fd, &resp, sizeof(resp));
            break;

        case CMD_LOGS:
            /* handle_logs streams multiple packets itself */
            handle_logs(&ctx, &req, &resp, client_fd);
            break;

        case CMD_STOP:
            handle_stop(&ctx, &req, &resp);
            (void)write(client_fd, &resp, sizeof(resp));
            break;

        default:
            snprintf(resp.message, sizeof(resp.message),
                     "ERROR: Unknown command %d\n", req.kind);
            resp.status = 1;
            (void)write(client_fd, &resp, sizeof(resp));
            break;
        }

        close(client_fd);
    }

    /* ----------------------------------------------------------------
     * Graceful shutdown
     * ---------------------------------------------------------------- */
    printf("[Supervisor] Shutting down...\n");

    /* Kill all still-running containers */
    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *rec = ctx.containers;
    while (rec) {
        if (rec->state == CONTAINER_RUNNING) {
            kill(rec->host_pid, SIGKILL);
            rec->state = CONTAINER_STOPPED;
        }
        rec = rec->next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);

    /* Wait for remaining children */
    while (waitpid(-1, NULL, WNOHANG) > 0)
        ;

    /* Drain and stop the log buffer */
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);
    bounded_buffer_destroy(&ctx.log_buffer);

    /* Free container records */
    pthread_mutex_lock(&ctx.metadata_lock);
    rec = ctx.containers;
    while (rec) {
        container_record_t *tmp = rec->next;
        free(rec);
        rec = tmp;
    }
    ctx.containers = NULL;
    pthread_mutex_unlock(&ctx.metadata_lock);

    pthread_mutex_destroy(&ctx.metadata_lock);

    close(ctx.server_fd);
    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);
    unlink(CONTROL_PATH);

    printf("[Supervisor] Clean exit.\n");
    return 0;
}

/* ------------------------------------------------------------------ */
/*  Client-side helpers                                                 */
/* ------------------------------------------------------------------ */

static int send_control_request(const control_request_t *req)
{
    int sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock < 0) { perror("socket"); return 1; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr,
                "Failed to connect to supervisor at %s. Is it running?\n",
                CONTROL_PATH);
        close(sock);
        return 1;
    }

    if (write(sock, req, sizeof(*req)) != (ssize_t)sizeof(*req)) {
        perror("write request");
        close(sock);
        return 1;
    }

    /*
     * Read responses until the connection closes (multi-packet commands
     * like CMD_LOGS and CMD_RUN send several packets ending with an
     * empty-message terminator).
     */
    control_response_t resp;
    ssize_t n;
    while ((n = read(sock, &resp, sizeof(resp))) > 0) {
        if (resp.message[0] != '\0')
            printf("%s", resp.message);
        else
            break; /* empty-message terminator */
    }

    close(sock);
    return 0;
}

/* ------------------------------------------------------------------ */
/*  CLI command handlers                                                */
/* ------------------------------------------------------------------ */

static int cmd_start(int argc, char *argv[])
{
    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <rootfs> <command> "
                "[--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,       argv[3], sizeof(req.rootfs)        - 1);
    strncpy(req.command,      argv[4], sizeof(req.command)       - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <rootfs> <command> "
                "[--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,       argv[3], sizeof(req.rootfs)        - 1);
    strncpy(req.command,      argv[4], sizeof(req.command)       - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
}

static int cmd_stop(int argc, char *argv[])
{
    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
}

/* ------------------------------------------------------------------ */
/*  main                                                                */
/* ------------------------------------------------------------------ */

int main(int argc, char *argv[])
{
    if (argc < 2) { usage(argv[0]); return 1; }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }
    if (strcmp(argv[1], "start") == 0) return cmd_start(argc, argv);
    if (strcmp(argv[1], "run")   == 0) return cmd_run(argc, argv);
    if (strcmp(argv[1], "ps")    == 0) return cmd_ps();
    if (strcmp(argv[1], "logs")  == 0) return cmd_logs(argc, argv);
    if (strcmp(argv[1], "stop")  == 0) return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
