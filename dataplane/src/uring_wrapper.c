/*
 * Thin C wrapper around SPDK's create_uring_bdev which uses a struct
 * containing spdk_uuid by value — difficult to bind directly with
 * Rust's bindgen. This wrapper takes simple scalar parameters.
 *
 * When SPDK is built without --with-uring (e.g. due to old liburing),
 * create_uring_bdev is not available. In that case we provide a stub
 * that returns NULL so the binary still links.
 */
#include <spdk/stdinc.h>
#include <spdk/bdev.h>
#include <spdk/uuid.h>
#include <string.h>

/* Forward declaration matching module/bdev/uring/bdev_uring.h */
struct bdev_uring_opts {
    const char *name;
    const char *filename;
    uint32_t block_size;
    struct spdk_uuid uuid;
};

/*
 * Weak symbol: if SPDK was built with uring, the real create_uring_bdev
 * from libspdk_bdev_uring.a will override this. Otherwise, this stub is
 * used and novastor_create_uring_bdev returns NULL.
 */
__attribute__((weak))
struct spdk_bdev *create_uring_bdev(const struct bdev_uring_opts *opts)
{
    (void)opts;
    return NULL;
}

struct spdk_bdev *novastor_create_uring_bdev(const char *name,
                                              const char *filename,
                                              uint32_t block_size)
{
    struct bdev_uring_opts opts;
    memset(&opts, 0, sizeof(opts));
    opts.name = name;
    opts.filename = filename;
    opts.block_size = block_size;
    /* uuid left zeroed — SPDK will auto-generate */
    return create_uring_bdev(&opts);
}
