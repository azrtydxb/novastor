//! Sub-block I/O primitives.
//!
//! Each 4MB chunk is divided into 64 × 64KB sub-blocks.
//! This module provides constants, offset math, and dirty bitmap operations.

/// 4MB chunk size in bytes.
pub const CHUNK_SIZE: usize = 4 * 1024 * 1024;

/// 64KB sub-block size in bytes.
pub const SUB_BLOCK_SIZE: usize = 64 * 1024;

/// Number of sub-blocks per chunk (64).
pub const SUB_BLOCKS_PER_CHUNK: usize = CHUNK_SIZE / SUB_BLOCK_SIZE;

/// Calculate which chunk a volume byte offset falls in.
pub fn chunk_index(volume_offset: u64) -> u64 {
    volume_offset / CHUNK_SIZE as u64
}

/// Calculate which sub-block within a chunk a byte offset falls in.
pub fn sub_block_index(volume_offset: u64) -> usize {
    ((volume_offset % CHUNK_SIZE as u64) / SUB_BLOCK_SIZE as u64) as usize
}

/// Calculate the byte offset within a sub-block.
pub fn offset_in_sub_block(volume_offset: u64) -> usize {
    (volume_offset % SUB_BLOCK_SIZE as u64) as usize
}

/// Calculate the backend byte offset for a sub-block.
/// `chunk_base` is the backend offset where this chunk's data starts.
pub fn backend_sub_block_offset(chunk_base: u64, sb_index: usize) -> u64 {
    chunk_base + (sb_index as u64 * SUB_BLOCK_SIZE as u64)
}

/// Set a bit in the dirty bitmap.
pub fn bitmap_set(bitmap: &mut u64, sb_index: usize) {
    *bitmap |= 1u64 << sb_index;
}

/// Clear a single sub-block bit in the dirty bitmap.
pub fn bitmap_clear(bitmap: &mut u64, sb_index: usize) {
    *bitmap &= !(1u64 << sb_index);
}

/// Check if a bit is set in the dirty bitmap.
pub fn bitmap_is_set(bitmap: u64, sb_index: usize) -> bool {
    (bitmap >> sb_index) & 1 == 1
}

/// Count dirty sub-blocks.
pub fn bitmap_count(bitmap: u64) -> u32 {
    bitmap.count_ones()
}

/// Check if all 64 sub-blocks are dirty.
pub fn bitmap_is_full(bitmap: u64) -> bool {
    bitmap == u64::MAX
}

/// Iterator over set bit indices.
pub fn bitmap_dirty_indices(bitmap: u64) -> impl Iterator<Item = usize> {
    (0..SUB_BLOCKS_PER_CHUNK).filter(move |i| bitmap_is_set(bitmap, *i))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_and_sub_block_math() {
        assert_eq!(chunk_index(0), 0);
        assert_eq!(sub_block_index(0), 0);
        assert_eq!(offset_in_sub_block(0), 0);

        assert_eq!(chunk_index(65536), 0);
        assert_eq!(sub_block_index(65536), 1);
        assert_eq!(offset_in_sub_block(65536), 0);

        assert_eq!(chunk_index(4 * 1024 * 1024), 1);
        assert_eq!(sub_block_index(4 * 1024 * 1024), 0);

        let off = 4 * 1024 * 1024 + 100 * 1024;
        assert_eq!(chunk_index(off), 1);
        assert_eq!(sub_block_index(off), 1);
        assert_eq!(offset_in_sub_block(off), 36864);
    }

    #[test]
    fn bitmap_operations() {
        let mut bm: u64 = 0;
        assert!(!bitmap_is_set(bm, 0));
        bitmap_set(&mut bm, 0);
        assert!(bitmap_is_set(bm, 0));
        assert_eq!(bitmap_count(bm), 1);

        bitmap_set(&mut bm, 63);
        assert!(bitmap_is_set(bm, 63));
        assert_eq!(bitmap_count(bm), 2);
        assert!(!bitmap_is_full(bm));

        bm = u64::MAX;
        assert!(bitmap_is_full(bm));
        assert_eq!(bitmap_count(bm), 64);
    }

    #[test]
    fn dirty_indices_iterator() {
        let bm: u64 = 0b1010_0001;
        let indices: Vec<usize> = bitmap_dirty_indices(bm).collect();
        assert_eq!(indices, vec![0, 5, 7]);
    }

    #[test]
    fn backend_offset_calculation() {
        let chunk_base = 1024 * 1024;
        assert_eq!(backend_sub_block_offset(chunk_base, 0), chunk_base);
        assert_eq!(backend_sub_block_offset(chunk_base, 1), chunk_base + 65536);
        assert_eq!(
            backend_sub_block_offset(chunk_base, 63),
            chunk_base + 63 * 65536
        );
    }
}
