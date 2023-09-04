use std::cmp::{self};
use std::collections::BinaryHeap;
use std::mem::swap;

use anyhow::{Ok, Result};

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        match self.1.key().cmp(other.1.key()) {
            cmp::Ordering::Greater => Some(cmp::Ordering::Greater),
            cmp::Ordering::Less => Some(cmp::Ordering::Less),
            cmp::Ordering::Equal => self.0.partial_cmp(&other.0),
        }
        .map(|x| x.reverse())
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, choose the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut biters = BinaryHeap::<HeapWrapper<I>>::new();
        let mut iters = iters;
        let mut i = 0_usize;
        for iter in iters.drain(..) {
            if iter.is_valid() {
                biters.push(HeapWrapper(i, iter));
                i += 1;
            }
        }
        let current = if i != 0 {
            Some(biters.pop().unwrap())
        } else {
            None
        };
        Self {
            iters: biters,
            current,
        }
    }

    fn step(&mut self) {
        if self.current.is_none() {
            return;
        }

        if self.current.as_mut().unwrap().1.next().is_err()
            || !self.current.as_ref().unwrap().1.is_valid()
        {
            self.current = self.iters.pop();
            return;
        }

        let mut iter = self.iters.pop();
        if iter.is_none() {
            return;
        }

        if iter.as_ref().unwrap().gt(self.current.as_ref().unwrap()) {
            swap(&mut iter, &mut self.current);
        }

        self.iters.push(iter.unwrap());
    }
}

impl<I: StorageIterator> StorageIterator for MergeIterator<I> {
    fn key(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map_or_else(|| false, |iter| iter.1.is_valid())
    }

    fn next(&mut self) -> Result<()> {
        if !self.is_valid() {
            return Ok(());
        }
        let curr_key = Vec::from(self.key());

        self.step();

        while self.is_valid() && curr_key[..].eq(self.key()) {
            self.step();
        }
        Ok(())
    }
}
