use super::Block;

type KVPair = (Vec<u8>, Vec<u8>);

/// Builds a block.
pub struct BlockBuilder {
    kvs: Vec<KVPair>,
    size: usize,
    block_size: usize,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            kvs: Vec::new(),
            size: 2, // size of num_of_elements
            block_size,
        }
    }

    /// Return the number of key-value entres
    pub fn len(&self) -> usize {
        self.kvs.len()
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        // size_of(value_len) + size_of(key_len) + size_of(offset)
        let size_add = 2 + 2 + 2 + key.len() + value.len();
        if self.is_empty() || self.size + size_add <= self.block_size {
            self.kvs.push((Vec::from(key), Vec::from(value)));
            self.size += size_add;
            return true;
        }
        if self.kvs.len() == 1 && self.kvs[0].0[..].cmp(key).is_eq() {
            self.kvs[0].1 = Vec::from(value);
            return true;
        }
        false
    }

    /// Choose the middle key as the split key and split self to two BlockBuilders on the key.
    /// Return the split key and the one with larger keys.
    pub fn split(&mut self) -> (Vec<u8>, Self) {
        self.kvs
            .sort_unstable_by(|a: &KVPair, b: &KVPair| a.1.cmp(&b.1));

        let split_idx = self.kvs.len() / 2;
        let split_key = self.kvs[split_idx].0.clone();
        let mut split_kv: Option<KVPair> = None;

        let mut new_kvs = Vec::new();

        let mut kvs = Vec::new();
        let mut size = 0;

        for kv in self.kvs.drain(..) {
            let order = kv.0.cmp(&split_key);
            if order.is_gt() {
                size += 6 + kv.0.len() + kv.1.len();
                kvs.push(kv);
            } else if order.is_lt() {
                new_kvs.push(kv);
            } else {
                if let Some(dropped_kv) = split_kv {
                    // drop the key-value directly
                    self.size -= 6 + dropped_kv.0.len() + dropped_kv.1.len();
                }
                split_kv = Some(kv)
            }
        }

        let split_kv = split_kv.expect("must have some");
        size += 6 + split_kv.0.len() + split_kv.1.len();
        kvs.push(split_kv);

        self.kvs = new_kvs;
        self.size -= size;

        size += 2;

        let block_size = self.block_size;
        (
            split_key,
            Self {
                kvs,
                size,
                block_size,
            },
        )
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.size == 2
    }

    pub fn size(&self) -> usize {
        self.size
    }

    /// Finalize the block.
    pub fn build(&mut self) -> Block {
        self.kvs
            .sort_unstable_by(|a: &KVPair, b: &KVPair| a.1.cmp(&b.1));

        let mut data = Vec::new();
        let mut offsets = Vec::new();

        for (key, value) in &mut self.kvs {
            offsets.push(data.len() as u16);

            data.append(&mut Vec::from(&(key.len() as u16).to_ne_bytes()[..]));
            data.append(key);
            data.append(&mut Vec::from(&(value.len() as u16).to_ne_bytes()[..]));
            data.append(value);
        }

        Block { data, offsets }
    }
}
