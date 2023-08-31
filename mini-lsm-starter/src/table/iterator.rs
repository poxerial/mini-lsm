#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    block_iter: BlockIterator,
    block_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let block_idx = 0_usize;
        let block = table.read_block(block_idx)?;
        let block_iter = BlockIterator::create_and_seek_to_first(block);
        Ok(Self {
            table,
            block_iter,
            block_idx,
        })
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.block_idx = 0;
        let block = self.table.read_block(self.block_idx)?;
        self.block_iter = BlockIterator::create_and_seek_to_first(block);
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: &[u8]) -> Result<Self> {
        let block_idx = table.find_block_idx(key);
        let block = table.read_block(block_idx)?;
        let block_iter = BlockIterator::create_and_seek_to_key(block, key);
        let mut ret = Self {
            table,
            block_iter,
            block_idx,
        };
        if !ret.block_iter.is_valid() {
            ret.next()?;
        }
        Ok(ret)
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing this function.
    pub fn seek_to_key(&mut self, key: &[u8]) -> Result<()> {
        self.block_idx = self.table.find_block_idx(key);
        let block = self.table.read_block(self.block_idx)?;
        self.block_iter = BlockIterator::create_and_seek_to_key(block, key);
        if !self.is_valid() {
            self.next()?;
        }
        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> &[u8] {
        self.block_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.block_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.block_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.block_iter.next();
        if !self.block_iter.is_valid() {
            self.block_idx += 1;
            if self.block_idx >= self.table.block_metas.len() {
                return Ok(());
            }
            let block = self.table.read_block(self.block_idx)?;
            self.block_iter = BlockIterator::create_and_seek_to_first(block);
        }
        Ok(())
    }
}
