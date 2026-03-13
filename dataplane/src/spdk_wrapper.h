/* SPDK header wrapper for bindgen */
#include <spdk/stdinc.h>
#include <spdk/env.h>
#include <spdk/event.h>
#include <spdk/bdev.h>
#include <spdk/bdev_module.h>
#include <spdk/blob.h>
#include <spdk/blob_bdev.h>
#include <spdk/lvol.h>
#include <spdk/nvmf.h>
#include <spdk/nvmf_spec.h>
#include <spdk/nvmf_transport.h>
#include <spdk/nvme.h>
#include <spdk/json.h>
#include <spdk/jsonrpc.h>
#include <spdk/rpc.h>
#include <spdk/thread.h>
#include <spdk/log.h>
#include <spdk/string.h>

/* Forward declarations for SPDK malloc bdev module internals.
 * These functions are part of the SPDK malloc bdev module
 * (module/bdev/malloc/bdev_malloc.h) but are not in the public include path.
 * They are exported from libspdk_bdev_malloc.a and stable across SPDK 23.x+.
 */
struct malloc_bdev_opts {
	char *name;
	uint64_t num_blocks;
	uint32_t block_size;
	uint32_t optimal_io_boundary;
	uint32_t md_size;
	bool md_interleave;
};

int create_malloc_disk(struct spdk_bdev **bdev, const struct malloc_bdev_opts *opts);
void delete_malloc_disk(struct spdk_bdev *bdev, spdk_bdev_unregister_cb cb_fn, void *cb_arg);

/* Forward declarations for SPDK lvol vbdev module internals.
 * These functions are in module/bdev/lvol/vbdev_lvol.h and exported from
 * libspdk_bdev_lvol.a. They provide the higher-level lvol creation API that
 * accepts a bdev name string rather than an spdk_bs_dev pointer.
 */
int vbdev_lvs_create(const char *bdev_name, const char *lvs_name, uint32_t cluster_sz,
		     enum lvs_clear_method clear_method, uint32_t num_md_pages_per_cluster_ratio,
		     void (*cb_fn)(void *cb_arg, struct spdk_lvol_store *lvs, int lvserrno),
		     void *cb_arg);

int vbdev_lvol_create(struct spdk_lvol_store *lvs, const char *name, uint64_t sz,
		      bool thin_provision, enum lvol_clear_method clear_method,
		      void (*cb_fn)(void *cb_arg, struct spdk_lvol *lvol, int lvolerrno),
		      void *cb_arg);

void vbdev_lvol_create_snapshot(struct spdk_lvol *lvol, const char *snapshot_name,
				void (*cb_fn)(void *cb_arg, struct spdk_lvol *lvol, int lvolerrno),
				void *cb_arg);

void vbdev_lvol_create_clone(struct spdk_lvol *lvol, const char *clone_name,
			     void (*cb_fn)(void *cb_arg, struct spdk_lvol *lvol, int lvolerrno),
			     void *cb_arg);

void vbdev_lvol_resize(struct spdk_lvol *lvol, uint64_t sz,
		       void (*cb_fn)(void *cb_arg, int lvolerrno), void *cb_arg);

void vbdev_lvol_destroy(struct spdk_lvol *lvol,
			void (*cb_fn)(void *cb_arg, int lvolerrno), void *cb_arg);
