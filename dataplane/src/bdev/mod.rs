//! Custom SPDK bdev modules for NovaStor.
pub mod chunk_io;
pub mod erasure;
pub mod novastor_bdev;
pub mod novastor_replica_bdev;
pub mod replica;
pub mod sub_block;
// write_buffer module removed — the full sub-block optimization is already in
// sub_block_write() (skip read when write covers entire 64KB sub-block).
// The WriteBuffer was dead code that added complexity without being wired
// into the I/O path.
