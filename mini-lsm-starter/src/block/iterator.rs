use std::sync::Arc;

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self { block, idx: 0 }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        Self::new(block)
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: &[u8]) -> Self {
        let mut s = Self::new(block);
        s.seek_to_key(key);
        s
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> &[u8] {
        let offset = self.block.offsets[self.idx] as usize;
        let key_len =
            u16::from_ne_bytes([self.block.data[offset], self.block.data[offset + 1]]) as usize;
        &self.block.data[offset + 2..offset + key_len + 2]
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        let offset = self.block.offsets[self.idx] as usize;
        let key_len =
            u16::from_ne_bytes([self.block.data[offset], self.block.data[offset + 1]]) as usize;
        let offset = offset + key_len + 2;
        let value_len =
            u16::from_ne_bytes([self.block.data[offset], self.block.data[offset + 1]]) as usize;
        &self.block.data[offset + 2..offset + value_len + 2]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        self.idx < self.block.offsets.len()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.idx = 0;
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by callers.
    pub fn seek_to_key(&mut self, key: &[u8]) {
        let block = &self.block;
        let mut l: usize = 0;
        let mut r = block.offsets.len();

        while l < r {
            let mid = (l + r) / 2;
            let offset = block.offsets[mid] as usize;
            let mid_key_len =
                u16::from_ne_bytes([block.data[offset], block.data[offset + 1]]) as usize;
            let mid_key = &block.data[offset + 2..offset + mid_key_len + 2];
            let order = mid_key.cmp(key);
            if order.is_ge() {
                r = mid;
            } else {
                l = mid + 1;
            }
        }
        self.idx = l;
    }
}
