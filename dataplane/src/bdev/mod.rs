//! Custom SPDK bdev modules for NovaStor.
pub mod chunk_io;
pub mod erasure;
pub mod novastor_bdev;
pub mod novastor_replica_bdev;
pub mod replica;
// write_buffer module removed — the full-chunk optimization is already in
// rmw_write() (line 263: skip read when write covers entire chunk).
// The WriteBuffer was dead code that added complexity without being wired
// into the I/O path.
