#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut, Bytes, BytesMut};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::lsm_storage::BlockCache;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block, mainly used for index purpose.
    pub first_key: Vec<u8>,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(
        block_meta: &[BlockMeta],
        #[allow(clippy::ptr_arg)] // remove this allow after you finish
        buf: &mut Vec<u8>,
    ) {
        for meta in block_meta {
            buf.put_u32_ne(meta.offset as u32);
            buf.put_u16_ne(meta.first_key.len() as u16);
            buf.append(&mut meta.first_key.clone());
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(buf: impl Buf) -> Vec<BlockMeta> {
        let mut buf = buf;
        let mut block_metas = Vec::new();
        while buf.remaining() != 0 {
            let offset = buf.get_u32_ne() as usize;
            let key_len = buf.get_u16_ne();
            let mut first_key = Vec::new();
            for _ in 0..key_len {
                first_key.push(buf.get_u8());
            }
            block_metas.push(BlockMeta { offset, first_key });
        }
        block_metas
    }
}

/// A file object.
pub struct FileObject(Bytes);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        Ok(self.0[offset as usize..(offset + len) as usize].to_vec())
    }

    pub fn size(&self) -> u64 {
        self.0.len() as u64
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        let mut file = File::create(path)?;
        let len = file.write(&data[..])?;
        if len != data.len() {
            panic!("");
        }
        Ok(Self(Bytes::from(data)))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let mut file = File::open(path)?;
        let mut content = Vec::new();
        file.read_to_end(&mut content)?;
        Ok(Self(Bytes::from(content)))
    }
}

/// -------------------------------------------------------------------------------------------------------
/// |              Data Block             |             Meta Block              |          Extra          |
/// -------------------------------------------------------------------------------------------------------
/// | Data Block #1 | ... | Data Block #N | Meta Block #1 | ... | Meta Block #N | Meta Block Offset (u32) |
/// -------------------------------------------------------------------------------------------------------
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    file: FileObject,
    /// The meta blocks that hold info for data blocks.
    block_metas: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    block_meta_offset: usize,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let len = file.0.len();
        let block_meta_offset = BytesMut::from(&file.0[len - 4..]).get_u32_ne() as usize;
        let block_metas = BlockMeta::decode_block_meta(&file.0[block_meta_offset..len - 4]);
        Ok(SsTable {
            file,
            block_metas,
            block_meta_offset,
        })
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let start = self.block_metas[block_idx].offset;
        let end = if block_idx < self.block_metas.len() - 1 {
            self.block_metas[block_idx + 1].offset
        } else {
            self.block_meta_offset
        };
        let data = &self.file.0[start..end];
        let block = Block::decode(data);
        Ok(Arc::from(block))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        unimplemented!()
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: &[u8]) -> usize {
        let (mut l, mut r) = (0_usize, self.num_of_blocks());
        while l < r {
            let mid = (l + r) / 2;
            let mid_key = &self.block_metas[mid].first_key;
            let order = mid_key[..].cmp(key);
            if order.is_ge() {
                r = mid;
            } else {
                l = mid + 1;
            }
        }
        l - 1
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_metas.len()
    }
}

#[cfg(test)]
mod tests;
