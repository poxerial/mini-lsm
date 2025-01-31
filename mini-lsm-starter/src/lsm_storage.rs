#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use parking_lot::RwLock;

use crate::block::Block;
use crate::iterators::StorageIterator;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::mem_table::MemTable;
use crate::table::{SsTable, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

#[derive(Clone)]
pub struct LsmStorageInner {
    /// The current memtable.
    memtable: Arc<MemTable>,
    /// Immutable memTables, from earliest to latest.
    imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SsTables, from earliest to latest.
    l0_sstables: Vec<Arc<SsTable>>,
    /// L1 - L6 SsTables, sorted by key range.
    #[allow(dead_code)]
    levels: Vec<Vec<Arc<SsTable>>>,
    /// The next SSTable ID.
    next_sst_id: usize,
}

impl LsmStorageInner {
    fn create() -> Self {
        Self {
            memtable: Arc::new(MemTable::create()),
            imm_memtables: vec![],
            l0_sstables: vec![],
            levels: vec![],
            next_sst_id: 1,
        }
    }
}

/// The storage interface of the LSM tree.
pub struct LsmStorage {
    inner: Arc<RwLock<Arc<LsmStorageInner>>>,
}

impl LsmStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(RwLock::new(Arc::new(LsmStorageInner::create()))),
        })
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let guard = self.inner.read();
        let inner = guard.clone();
        drop(guard);

        let val = inner.memtable.get(key);
        match val {
            Some(value) => return Ok(Some(value)),
            _ => {}
        };

        for memtable in inner.imm_memtables.iter().rev() {
            let val = inner.memtable.get(key);
            match val {
                Some(value) => return Ok(Some(value)),
                _ => {}
            };
        }

        for l0_sst in inner.l0_sstables.iter().rev() {
            let iter = SsTableIterator::create_and_seek_to_key(l0_sst, key)?;
            if iter.is_valid() && iter.key() == key {
                return Ok(Some(iter.value()))
            }
        }

        for level in inner.levels {
            let idx = level.partition_point(|table|{
                let iter = SsTableIterator::create_and_seek_to_key(table, key)?;
                iter.is_valid()  
            }).saturating_sub(1);
            let table = level[idx];
            let iter = SsTableIterator::create_and_seek_to_key(table, key)?;
            if iter.is_valid() && iter.key() == key {
                return Ok(Some(iter.value()))
            }
        }

        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        assert!(!value.is_empty(), "value cannot be empty");
        assert!(!key.is_empty(), "key cannot be empty");
        let guard = self.inner.write();
        guard.as_ref().memtable.put(key, value);
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        let guard = self.inner.write();
        guard.as_ref().memtable.put(key, &[]);
        Ok(())
    }

    /// Persist data to disk.
    ///
    /// In day 3: flush the current memtable to disk as L0 SST.
    /// In day 6: call `fsync` on WAL.
    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        unimplemented!()
    }
}
