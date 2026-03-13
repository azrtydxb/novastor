/*
 * Thin C wrapper around SPDK's create_uring_bdev which uses a struct
 * containing spdk_uuid by value — difficult to bind directly with
 * Rust's bindgen. This wrapper takes simple scalar parameters.
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

struct spdk_bdev *create_uring_bdev(const struct bdev_uring_opts *opts);

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
