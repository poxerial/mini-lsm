#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;
use std::{collections::BTreeMap, path::Path};

use anyhow::Result;
use bytes::BufMut;

use super::{BlockMeta, FileObject, SsTable};
use crate::{block::BlockBuilder, lsm_storage::BlockCache};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    pub(super) index: BTreeMap<Vec<u8>, usize>, // first key -> index
    blocks: Vec<BlockBuilder>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        let mut index = BTreeMap::new();
        index.insert(vec![], 0);
        let blocks = vec![BlockBuilder::new(block_size)];
        Self {
            index,
            blocks,
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable.
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may be of help here)
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        let key_vec = Vec::from(key);
        let idx = *if self.index.contains_key(key) {
            self.index.get(&key_vec).expect("not none")
        } else {
            self.index.range(..key_vec).last().expect("not none").1
        };

        let block = &mut self.blocks[idx];
        if block.add(key, value) {
            return;
        }

        let mut new_block: BlockBuilder;
        let split_key: Vec<u8>;

        if block.len() == 1 {
            new_block = BlockBuilder::new(self.block_size);
            split_key = key.into();
        } else {
            (split_key, new_block) = block.split();
        }

        let order = key.cmp(&split_key[..]);
        if order.is_ge() {
            assert!(new_block.add(key, value));
        } else {
            assert!(block.add(key, value));
        }

        let new_idx = self.blocks.len();

        self.index.insert(split_key, new_idx);
        self.blocks.push(new_block);
    }

    /// Get the estimated size of the SSTable.
    /// Since the data blocks contain much more data than meta blocks, just return the size of data blocks here.
    pub fn estimated_size(&self) -> usize {
        let mut size = 0_usize;
        for block in &self.blocks {
            size += block.size();
        }
        size
    }

    /// Builds the SSTable and writes it to the given path. No need to actually write to disk until
    /// chapter 4 block cache.
    pub fn build(
        self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        let mut builder = self;
        let mut block_metas = Vec::new();
        let mut size = 0_usize;
        let mut data = Vec::new();
        while let Some(kv) = builder.index.pop_first() {
            let (first_key, idx) = kv;
            block_metas.push(BlockMeta {
                offset: size,
                first_key,
            });
            size += builder.blocks[idx].size();
            let bytes = builder.blocks[idx].build().encode();
            data.append(&mut bytes[..].into());
        }
        // assert!(data.len() == size);
        let mut buf = Vec::new();
        BlockMeta::encode_block_meta(&block_metas[..], &mut buf);
        data.append(&mut buf);
        data.put_u32_ne(size as u32);

        let file = FileObject::create(path.as_ref(), data)?;
        Ok(SsTable {
            file,
            block_metas,
            block_meta_offset: size,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
