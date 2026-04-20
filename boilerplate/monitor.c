/*
 * monitor.c - Container Memory Monitor (Linux Kernel Module)
 *
 * Exposes /dev/container_monitor as a character device.
 * The user-space supervisor registers container PIDs via ioctl.
 * A kernel timer fires periodically and checks RSS for each tracked
 * container.  When RSS exceeds the soft limit a warning is logged to
 * dmesg.  When RSS exceeds the hard limit the process is sent SIGKILL.
 *
 * Build:  see Makefile
 * Load:   sudo insmod monitor.ko
 * Unload: sudo rmmod monitor
 */

#include <linux/cdev.h>
#include <linux/fs.h>
#include <linux/init.h>
#include <linux/kernel.h>
#include <linux/list.h>
#include <linux/mm.h>
#include <linux/module.h>
#include <linux/mutex.h>
#include <linux/sched.h>
#include <linux/sched/signal.h>
#include <linux/slab.h>
#include <linux/timer.h>
#include <linux/uaccess.h>
#include <linux/version.h>

#include "monitor_ioctl.h"

MODULE_LICENSE("GPL");
MODULE_AUTHOR("container-runtime-team");
MODULE_DESCRIPTION("Per-container RSS soft/hard limit monitor");
MODULE_VERSION("1.0");

/* ------------------------------------------------------------------ */
/*  Module parameters                                                   */
/* ------------------------------------------------------------------ */

/* How often (in milliseconds) to poll RSS for all tracked containers */
static unsigned int poll_interval_ms = 2000;
module_param(poll_interval_ms, uint, 0644);
MODULE_PARM_DESC(poll_interval_ms, "RSS polling interval in milliseconds (default 2000)");

/* ------------------------------------------------------------------ */
/*  Per-container tracking entry                                        */
/* ------------------------------------------------------------------ */

struct container_entry {
    struct list_head  list;
    pid_t             pid;
    unsigned long     soft_limit_bytes;
    unsigned long     hard_limit_bytes;
    int               soft_warned;        /* 1 after first soft-limit warning */
    char              container_id[CONTAINER_ID_LEN];
};

/* Global linked list of tracked containers and its protecting mutex */
static LIST_HEAD(container_list);
static DEFINE_MUTEX(container_list_mutex);

/* ------------------------------------------------------------------ */
/*  Character device bookkeeping                                        */
/* ------------------------------------------------------------------ */

#define DEVICE_NAME "container_monitor"
#define CLASS_NAME  "container_monitor"

static int            major_number;
static struct class  *monitor_class  = NULL;
static struct device *monitor_device = NULL;
static struct cdev    monitor_cdev;

/* ------------------------------------------------------------------ */
/*  RSS helper                                                          */
/* ------------------------------------------------------------------ */

/*
 * Return the Resident Set Size of process @pid in bytes, or 0 if the
 * process no longer exists or the task struct cannot be found.
 *
 * get_task_mm / mm_struct / get_mm_rss give us the current RSS page
 * count without needing /proc.
 */
static unsigned long get_rss_bytes(pid_t pid)
{
    struct task_struct *task;
    struct mm_struct   *mm;
    unsigned long       rss_pages = 0;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (!task) {
        rcu_read_unlock();
        return 0;
    }

    mm = get_task_mm(task);
    rcu_read_unlock();

    if (!mm)
        return 0;

    rss_pages = get_mm_rss(mm);
    mmput(mm);

    return rss_pages << PAGE_SHIFT;
}

/* ------------------------------------------------------------------ */
/*  Send a signal to a PID safely                                      */
/* ------------------------------------------------------------------ */

static int send_signal_to_pid(pid_t pid, int sig)
{
    struct task_struct *task;
    int ret = -ESRCH;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (task) {
        ret = send_sig(sig, task, 0);
    }
    rcu_read_unlock();

    return ret;
}

/* ------------------------------------------------------------------ */
/*  Periodic RSS poll timer                                             */
/* ------------------------------------------------------------------ */

static struct timer_list rss_poll_timer;

static void rss_poll_callback(struct timer_list *t)
{
    struct container_entry *entry, *tmp;
    unsigned long rss;

    mutex_lock(&container_list_mutex);

    list_for_each_entry_safe(entry, tmp, &container_list, list) {
        rss = get_rss_bytes(entry->pid);

        /* Process no longer exists — remove stale entry */
        if (rss == 0 && entry->pid > 0) {
            struct task_struct *task;
            rcu_read_lock();
            task = pid_task(find_vpid(entry->pid), PIDTYPE_PID);
            rcu_read_unlock();

            if (!task) {
                pr_info("container_monitor: container '%s' (pid %d) "
                        "no longer exists, removing entry\n",
                        entry->container_id, entry->pid);
                list_del(&entry->list);
                kfree(entry);
                continue;
            }
        }

        /* Hard limit: kill the process */
        if (rss > entry->hard_limit_bytes) {
            pr_warn("container_monitor: HARD LIMIT EXCEEDED — "
                    "container '%s' (pid %d) RSS %lu bytes > hard limit %lu bytes. "
                    "Sending SIGKILL.\n",
                    entry->container_id, entry->pid,
                    rss, entry->hard_limit_bytes);
            send_signal_to_pid(entry->pid, SIGKILL);
            /* Leave the entry in the list; SIGCHLD handling will unregister it */
            continue;
        }

        /* Soft limit: warn once */
        if (rss > entry->soft_limit_bytes && !entry->soft_warned) {
            pr_warn("container_monitor: SOFT LIMIT WARNING — "
                    "container '%s' (pid %d) RSS %lu bytes > soft limit %lu bytes.\n",
                    entry->container_id, entry->pid,
                    rss, entry->soft_limit_bytes);
            entry->soft_warned = 1;
        }
    }

    mutex_unlock(&container_list_mutex);

    /* Re-arm the timer */
    mod_timer(&rss_poll_timer,
              jiffies + msecs_to_jiffies(poll_interval_ms));
}

/* ------------------------------------------------------------------ */
/*  ioctl handler                                                       */
/* ------------------------------------------------------------------ */

static long monitor_ioctl(struct file *filp,
                           unsigned int cmd,
                           unsigned long arg)
{
    struct monitor_request req;
    struct container_entry *entry, *tmp;

    /* Copy the request structure from user space */
    if (copy_from_user(&req, (struct monitor_request __user *)arg,
                       sizeof(req))) {
        return -EFAULT;
    }

    /* Ensure the container_id string is NUL-terminated */
    req.container_id[CONTAINER_ID_LEN - 1] = '\0';

    switch (cmd) {

    case MONITOR_REGISTER:
        /* Reject obviously bad PIDs */
        if (req.pid <= 0) {
            pr_err("container_monitor: REGISTER rejected — invalid pid %d\n",
                   req.pid);
            return -EINVAL;
        }
        if (req.soft_limit_bytes > req.hard_limit_bytes) {
            pr_err("container_monitor: REGISTER rejected — "
                   "soft limit %lu > hard limit %lu for container '%s'\n",
                   req.soft_limit_bytes, req.hard_limit_bytes,
                   req.container_id);
            return -EINVAL;
        }

        entry = kmalloc(sizeof(*entry), GFP_KERNEL);
        if (!entry)
            return -ENOMEM;

        entry->pid               = req.pid;
        entry->soft_limit_bytes  = req.soft_limit_bytes;
        entry->hard_limit_bytes  = req.hard_limit_bytes;
        entry->soft_warned       = 0;
        strncpy(entry->container_id, req.container_id, CONTAINER_ID_LEN - 1);
        entry->container_id[CONTAINER_ID_LEN - 1] = '\0';

        mutex_lock(&container_list_mutex);
        list_add_tail(&entry->list, &container_list);
        mutex_unlock(&container_list_mutex);

        pr_info("container_monitor: registered container '%s' "
                "pid=%d soft=%lu hard=%lu\n",
                entry->container_id, entry->pid,
                entry->soft_limit_bytes, entry->hard_limit_bytes);
        break;

    case MONITOR_UNREGISTER:
        mutex_lock(&container_list_mutex);
        list_for_each_entry_safe(entry, tmp, &container_list, list) {
            if (entry->pid == req.pid) {
                pr_info("container_monitor: unregistered container '%s' "
                        "pid=%d\n",
                        entry->container_id, entry->pid);
                list_del(&entry->list);
                kfree(entry);
                break;
            }
        }
        mutex_unlock(&container_list_mutex);
        break;

    default:
        return -ENOTTY;
    }

    return 0;
}

/* ------------------------------------------------------------------ */
/*  File operations for /dev/container_monitor                         */
/* ------------------------------------------------------------------ */

static int     monitor_open   (struct inode *i, struct file *f) { return 0; }
static int     monitor_release(struct inode *i, struct file *f) { return 0; }

static const struct file_operations monitor_fops = {
    .owner          = THIS_MODULE,
    .open           = monitor_open,
    .release        = monitor_release,
    .unlocked_ioctl = monitor_ioctl,
};

/* ------------------------------------------------------------------ */
/*  Module init / exit                                                  */
/* ------------------------------------------------------------------ */

static int __init monitor_init(void)
{
    dev_t dev;
    int   ret;

    /* Allocate a major number dynamically */
    ret = alloc_chrdev_region(&dev, 0, 1, DEVICE_NAME);
    if (ret < 0) {
        pr_err("container_monitor: alloc_chrdev_region failed: %d\n", ret);
        return ret;
    }
    major_number = MAJOR(dev);

    /* Initialise and add the cdev */
    cdev_init(&monitor_cdev, &monitor_fops);
    monitor_cdev.owner = THIS_MODULE;
    ret = cdev_add(&monitor_cdev, dev, 1);
    if (ret < 0) {
        pr_err("container_monitor: cdev_add failed: %d\n", ret);
        unregister_chrdev_region(dev, 1);
        return ret;
    }

    /* Create the device class */
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 4, 0)
    monitor_class = class_create(CLASS_NAME);
#else
    monitor_class = class_create(THIS_MODULE, CLASS_NAME);
#endif
    if (IS_ERR(monitor_class)) {
        pr_err("container_monitor: class_create failed\n");
        cdev_del(&monitor_cdev);
        unregister_chrdev_region(dev, 1);
        return PTR_ERR(monitor_class);
    }

    /* Create /dev/container_monitor */
    monitor_device = device_create(monitor_class, NULL,
                                    MKDEV(major_number, 0),
                                    NULL, DEVICE_NAME);
    if (IS_ERR(monitor_device)) {
        pr_err("container_monitor: device_create failed\n");
        class_destroy(monitor_class);
        cdev_del(&monitor_cdev);
        unregister_chrdev_region(dev, 1);
        return PTR_ERR(monitor_device);
    }

    /* Arm the periodic RSS poll timer */
    timer_setup(&rss_poll_timer, rss_poll_callback, 0);
    mod_timer(&rss_poll_timer,
              jiffies + msecs_to_jiffies(poll_interval_ms));

    pr_info("container_monitor: loaded — /dev/%s (major %d), "
            "polling every %u ms\n",
            DEVICE_NAME, major_number, poll_interval_ms);
    return 0;
}

static void __exit monitor_exit(void)
{
    struct container_entry *entry, *tmp;

    /* Stop the timer before touching the list */
#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 11, 0)
    timer_delete_sync(&rss_poll_timer);
#else
    del_timer_sync(&rss_poll_timer);
#endif

    /* Free all remaining tracked entries */
    mutex_lock(&container_list_mutex);
    list_for_each_entry_safe(entry, tmp, &container_list, list) {
        pr_info("container_monitor: freeing entry for container '%s' "
                "pid=%d on unload\n",
                entry->container_id, entry->pid);
        list_del(&entry->list);
        kfree(entry);
    }
    mutex_unlock(&container_list_mutex);

    /* Tear down the character device */
    device_destroy(monitor_class, MKDEV(major_number, 0));
    class_destroy(monitor_class);
    cdev_del(&monitor_cdev);
    unregister_chrdev_region(MKDEV(major_number, 0), 1);

    pr_info("container_monitor: unloaded\n");
}

module_init(monitor_init);
module_exit(monitor_exit);
