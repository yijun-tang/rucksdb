//! The wrapper of murmur3 hash function.

use std::io::Cursor;
use murmur3::murmur3_32;

pub(crate) fn hash(data: &[u8], seed: u32) -> u32 {
    murmur3_32(&mut Cursor::new(data), seed).unwrap()
}
