/*
 * Thin C wrapper around SPDK's NVMe bdev attach API. Uses the
 * spdk_bdev_nvme_create function from SPDK's bdev_nvme module
 * to attach a local NVMe device by its PCIe BDF address.
 *
 * Before calling SPDK's attach, this wrapper unbinds the NVMe device
 * from the kernel nvme driver and binds it to uio_pci_generic, which
 * is required for SPDK's user-space NVMe driver.
 */
#include <spdk/stdinc.h>
#include <spdk/bdev.h>
#include <spdk/nvme.h>
#include <spdk/env.h>
#include <spdk/string.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>

struct spdk_nvme_transport_id;

typedef void (*spdk_bdev_nvme_create_cb)(void *ctx, size_t bdev_count,
                                          int rc);

extern int spdk_bdev_nvme_create(struct spdk_nvme_transport_id *trid,
                                  const char *base_name,
                                  const char **names, uint32_t count,
                                  spdk_bdev_nvme_create_cb cb_fn, void *cb_ctx,
                                  void *drv_opts, void *bdev_opts);

/*
 * Write a string to a sysfs file. Returns 0 on success, -errno on failure.
 */
static int sysfs_write(const char *path, const char *value)
{
    int fd = open(path, O_WRONLY);
    if (fd < 0) return -errno;
    ssize_t n = write(fd, value, strlen(value));
    close(fd);
    return (n < 0) ? -errno : 0;
}

/*
 * Unbind an NVMe device from the kernel driver and bind to uio_pci_generic.
 * This is required because SPDK needs direct user-space access to the NVMe
 * controller via PCI BAR mapping.
 *
 * Steps:
 *   1. Load uio_pci_generic module (modprobe, best-effort)
 *   2. Unbind from kernel nvme driver
 *   3. Override driver to uio_pci_generic
 *   4. Bind to uio_pci_generic
 *
 * Returns 0 on success (or if already unbound), -errno on failure.
 */
static int unbind_nvme_for_spdk(const char *pcie_addr)
{
    char path[256];
    int rc;

    /* Check if already bound to a user-space driver */
    char driver_link[256];
    snprintf(path, sizeof(path), "/sys/bus/pci/devices/%s/driver", pcie_addr);
    ssize_t len = readlink(path, driver_link, sizeof(driver_link) - 1);
    if (len > 0) {
        driver_link[len] = '\0';
        if (strstr(driver_link, "uio_pci_generic") != NULL ||
            strstr(driver_link, "vfio-pci") != NULL) {
            return 0; /* Already bound to a user-space driver */
        }
    } else {
        return 0; /* No driver bound — SPDK can probe directly */
    }

    /* vfio-pci module is pre-loaded by the entrypoint script.
     * Unbind from current driver (e.g., nvme) */
    snprintf(path, sizeof(path), "/sys/bus/pci/devices/%s/driver/unbind", pcie_addr);
    rc = sysfs_write(path, pcie_addr);
    if (rc != 0 && rc != -ENOENT) {
        return rc;
    }

    /* Set driver_override to vfio-pci */
    snprintf(path, sizeof(path), "/sys/bus/pci/devices/%s/driver_override", pcie_addr);
    rc = sysfs_write(path, "vfio-pci");
    if (rc != 0) {
        return rc;
    }

    /* Bind to vfio-pci */
    rc = sysfs_write("/sys/bus/pci/drivers/vfio-pci/bind", pcie_addr);
    if (rc != 0 && rc != -ENODEV) {
        return rc;
    }

    /* Small delay for kernel to settle */
    usleep(100000); /* 100ms */

    return 0;
}

/*
 * Synchronous wrapper: unbinds the NVMe device from the kernel driver,
 * then attaches it via SPDK's bdev_nvme module.
 *
 * Returns 0 on success, negative errno on failure.
 */
int novastor_attach_nvme_bdev(const char *controller_name,
                               const char *pcie_addr)
{
    /* First, ensure the device is unbound from the kernel driver */
    int unbind_rc = unbind_nvme_for_spdk(pcie_addr);
    if (unbind_rc != 0) {
        fprintf(stderr, "novastor: failed to unbind NVMe %s from kernel driver: %d\n",
                pcie_addr, unbind_rc);
        return unbind_rc;
    }

    struct spdk_nvme_transport_id trid;
    memset(&trid, 0, sizeof(trid));

    trid.trtype = SPDK_NVME_TRANSPORT_PCIE;
    snprintf(trid.traddr, sizeof(trid.traddr), "%s", pcie_addr);

    int rc = spdk_bdev_nvme_create(&trid, controller_name,
                                    NULL, 0,   /* names, count */
                                    NULL, NULL, /* cb_fn, cb_ctx */
                                    NULL, NULL  /* drv_opts, bdev_opts */);
    return rc;
}
