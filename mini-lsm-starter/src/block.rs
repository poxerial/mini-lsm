mod builder;
mod iterator;

pub use builder::BlockBuilder;
/// You may want to check `bytes::BufMut` out when manipulating continuous chunks of memory
use bytes::{BufMut, Bytes};
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree.
/// It is a collection of sorted key-value pairs.
/// The `actual` storage format is as below (After `Block::encode`):
///
/// ----------------------------------------------------------------------------------------------------
/// |             Data Section             |              Offset Section             |      Extra      |
/// ----------------------------------------------------------------------------------------------------
/// | Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
/// ----------------------------------------------------------------------------------------------------
pub struct Block {
    data: Vec<u8>,
    offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut buf = bytes::BytesMut::from(self.data.as_slice());
        for offset in &self.offsets {
            buf.put_u16_ne(*offset);
        }
        buf.put_u16_ne(self.offsets.len() as u16);
        Bytes::from(buf)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let num_of_elm = u16::from_ne_bytes([data[data.len() - 2], data[data.len() - 1]]) as usize;

        let boundary_index = data.len() - num_of_elm * 2 - 2;
        let kvdata = Vec::from(&data[..boundary_index]);
        let mut offsets = Vec::new();
        for offset_index in 0..num_of_elm {
            let i = boundary_index + offset_index * 2;
            offsets.push(u16::from_ne_bytes([data[i], data[i + 1]]))
        }
        Self {
            data: kvdata,
            offsets,
        }
    }
}

#[cfg(test)]
mod tests;
