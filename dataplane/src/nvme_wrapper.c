/*
 * Thin C wrapper around SPDK's NVMe bdev attach API. Uses the
 * bdev_nvme_create RPC-style function from SPDK's bdev_nvme module
 * to attach a local NVMe device by its PCIe BDF address.
 *
 * The actual NVMe attach API in SPDK is complex (uses spdk_nvme_transport_id
 * and an asynchronous callback). This wrapper simplifies the interface to
 * just (controller_name, pcie_address) for Rust FFI.
 */
#include <spdk/stdinc.h>
#include <spdk/bdev.h>
#include <spdk/nvme.h>
#include <spdk/env.h>
#include <spdk/string.h>
#include <string.h>

/*
 * Forward declaration of the bdev_nvme attach controller function.
 * This is from module/bdev/nvme/bdev_nvme.h in SPDK and exported
 * from libspdk_bdev_nvme.a. The function attaches an NVMe controller
 * using the given transport ID and creates bdevs for each namespace.
 *
 * Weak symbol: if SPDK was built without the NVMe bdev module, this
 * stub returns -ENOTSUP.
 */

struct spdk_nvme_transport_id;

typedef void (*bdev_nvme_attach_cb)(void *ctx, size_t bdev_count,
                                     struct spdk_bdev **bdevs, int rc);

__attribute__((weak))
int bdev_nvme_create(struct spdk_nvme_transport_id *trid,
                     const char *base_name,
                     const char **names, uint32_t count,
                     bdev_nvme_attach_cb cb_fn, void *cb_ctx,
                     void *drv_opts, void *bdev_opts)
{
    (void)trid; (void)base_name; (void)names; (void)count;
    (void)cb_fn; (void)cb_ctx; (void)drv_opts; (void)bdev_opts;
    return -95; /* ENOTSUP */
}

/*
 * Synchronous wrapper: attaches an NVMe device identified by PCIe BDF
 * address. SPDK will enumerate all namespaces and create bdevs named
 * <controller_name>n<ns_id>.
 *
 * Returns 0 on success, negative errno on failure.
 */
int novastor_attach_nvme_bdev(const char *controller_name,
                               const char *pcie_addr)
{
    struct spdk_nvme_transport_id trid;
    memset(&trid, 0, sizeof(trid));

    trid.trtype = SPDK_NVME_TRANSPORT_PCIE;
    snprintf(trid.traddr, sizeof(trid.traddr), "%s", pcie_addr);

    /* We pass NULL for the callback (synchronous path) and NULL for
     * driver/bdev options (use defaults). The function will block on
     * the SPDK reactor thread until the controller is attached. */
    int rc = bdev_nvme_create(&trid, controller_name,
                               NULL, 0,   /* names, count */
                               NULL, NULL, /* cb_fn, cb_ctx */
                               NULL, NULL  /* drv_opts, bdev_opts */);
    return rc;
}
