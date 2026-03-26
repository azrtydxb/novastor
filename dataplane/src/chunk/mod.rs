//! Chunk engine — translates volume I/O into content-addressed chunk operations.

pub mod engine;
pub mod ndp_pool;
#[cfg(feature = "spdk-sys")]
pub mod reactor_ndp;
pub mod sync;
pub mod write_cache;
