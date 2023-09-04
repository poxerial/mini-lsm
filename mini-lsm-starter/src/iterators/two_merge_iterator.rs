#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::Result;

use super::StorageIterator;

enum Which {
    A,
    B,
}

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    which: Which,
}

impl<A: StorageIterator, B: StorageIterator> TwoMergeIterator<A, B> {
    pub fn create(a: A, b: B) -> Result<Self> {
        let mut b = b;
        let which = if !a.is_valid() || !b.is_valid() {
            if a.is_valid() {
                Which::A
            } else {
                Which::B
            }
        } else {
            while b.is_valid() && a.key().cmp(b.key()).is_eq() {
                b.next()?;
            }
            if !b.is_valid() {
                Which::A
            } else if a.key().cmp(b.key()).is_gt() {
                Which::B
            } else {
                Which::A
            }
        };
        Ok(Self { a, b, which })
    }
}

impl<A: StorageIterator, B: StorageIterator> TwoMergeIterator<A, B> {
    fn is_a(&self) -> bool {
        match self.which {
            Which::A => true,
            Which::B => false,
        }
    }

    fn set_which(&mut self) -> Result<()> {
        if !self.a.is_valid() || !self.b.is_valid() {
            self.which = if self.a.is_valid() {
                Which::A
            } else {
                Which::B
            };
        } else {
            while self.b.is_valid() && self.a.key().cmp(self.b.key()).is_eq() {
                self.b.next()?;
            }
            if !self.b.is_valid() {
                self.which = Which::A;
                return Ok(());
            }
            self.which = if self.a.key().cmp(self.b.key()).is_gt() {
                Which::B
            } else {
                Which::A
            }
        }
        Ok(())
    }
}

impl<A: StorageIterator, B: StorageIterator> StorageIterator for TwoMergeIterator<A, B> {
    fn key(&self) -> &[u8] {
        if self.is_a() {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        if self.is_a() {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        if self.is_a() {
            self.a.is_valid()
        } else {
            self.b.is_valid()
        }
    }

    fn next(&mut self) -> Result<()> {
        if self.is_a() {
            self.a.next()?;
        } else {
            self.b.next()?;
        }
        self.set_which()
    }
}
