//! Endian-neutral encoding:
//! * Fixed-length numbers are encoded with least-significant byte first
//! * In addition we support variable length "varint" encoding
//! * Strings are encoded prefixed by their length in varint format

use std::io::Write;
use crate::slice::Slice;

static B:u32 = 128;

pub(crate) fn put_fixed32(dst: &mut Vec<u8>, value: u32) {
    let _ = dst.write_all(&encode_fixed32(value));
}

pub(crate) fn put_fixed64(dst: &mut Vec<u8>, value: u64) {
    let _ = dst.write_all(&encode_fixed64(value));
}

pub(crate) fn put_varint32(dst: &mut Vec<u8>, v: u32) {
    let _ = dst.append(&mut encode_varint32(v));
}

pub(crate) fn put_varint64(dst: &mut Vec<u8>, v: u64) {
    let _ = dst.append(&mut encode_varint64(v));
}

pub(crate) fn put_length_prefixed_slice(dst: &mut Vec<u8>, value: &Slice) {
    put_varint64(dst, value.size() as u64);
    dst.extend(value.data());
}

pub(crate) fn get_length_prefixed_slice<'a>(input: &'a mut Slice) -> Option<Slice<'a>> {
    match get_varint64(input) {
        Some(len) => {
            if input.size() >= len as usize {
                let prefix = input.advance(len as usize);
                return Some(prefix);
            }
        },
        None => {},
    }
    None
}

pub(crate) fn get_varint32(input: &mut Slice) -> Option<u32> {
    let (next, value) = get_varint32_idx(input.data(), 0);
    if next == -1 {
        None
    } else {
        input.advance(next as usize);
        Some(value)
    }
}

pub(crate) fn get_varint64(input: &mut Slice) -> Option<u64> {
    let (next, value) = get_varint64_idx(input.data(), 0);
    if next == -1 {
        None
    } else {
        input.advance(next as usize);
        Some(value)
    }
}

pub(crate) fn get_varint32_idx(bytes: &[u8], idx: isize) -> (isize, u32) {
    if (idx as usize) < bytes.len() {
        let result = bytes[idx as usize] as u32;
        if result & B == 0 {
            return (idx + 1, result);
        }
    }
    get_varint32_idx_fallback(bytes, idx)
}

/// Return the next index of bytes and current u64 value.
fn get_varint64_idx(bytes: &[u8], mut idx: isize) -> (isize, u64) {
    let mut result = 0u64;
    let mut shift = 0;
    while shift <= 63 && ((idx as usize) < bytes.len()) {
        let byte = bytes[idx as usize] as u64;
        idx += 1;
        if (byte & (B as u64)) != 0 {
            result |= (byte & ((B as u64) - 1)) << shift;
        } else {
            result |= byte << shift;
            return (idx, result);
        }
        shift += 7;
    }
    (-1, 0)
}

pub(crate) fn varint_length(mut v: u64) -> usize {
    let mut len = 1usize;
    while v >= B as u64 {
        v >>= 7;
        len += 1;
    }
    len
}

fn get_varint32_idx_fallback(bytes: &[u8], mut idx: isize) -> (isize, u32) {
    let mut result = 0u32;
    let mut shift = 0;
    while shift <= 28 && ((idx as usize) < bytes.len()) {
        let byte = bytes[idx as usize] as u32;
        idx += 1;
        if byte & B != 0 {
            // More bytes are present
            result |= (byte & (B - 1)) << shift;
        } else {
            result |= byte << shift;
            return (idx, result);
        }
        shift += 7;
    }
    (-1, 0)
}

#[inline]
pub(crate) fn encode_fixed32(value: u32) -> [u8; 4] {
    value.to_le_bytes()
}

#[inline]
fn decode_fixed32(bytes: [u8; 4]) -> u32 {
    u32::from_le_bytes(bytes)
}

#[inline]
fn encode_fixed64(value: u64) -> [u8; 8] {
    value.to_le_bytes()
}

#[inline]
fn decode_fixed64(bytes: [u8; 8]) -> u64 {
    u64::from_le_bytes(bytes)
}

/// Encoding u32 as bytes of variable size.
/// 
/// 0xxxxxxx:                                           v < 1 << 7, 1 byte
/// 1xxxxxxx 0xxxxxxx:                                  v < 1 << 14, 2 bytes
/// 1xxxxxxx 1xxxxxxx 0xxxxxxx:                         v < 1 << 21, 3 bytes
/// 1xxxxxxx 1xxxxxxx 1xxxxxxx 0xxxxxxx:                v < 1 << 28, 4 bytes
/// 1xxxxxxx 1xxxxxxx 1xxxxxxx 1xxxxxxx 0xxxxxxx:       v >= 1 << 28, 5 bytes
fn encode_varint32(v: u32) -> Vec<u8> {
    let mut bytes: Vec<u8> = Vec::new();
    // Operate on characters as unsigneds
    if v < (1 << 7) {
        bytes.push(v as u8);
    } else if v < (1 << 14) {
        bytes.push((v | B) as u8);
        bytes.push((v >> 7) as u8);
    } else if v < (1 << 21) {
        bytes.push((v | B) as u8);
        bytes.push(((v >> 7) | B) as u8);
        bytes.push((v >> 14) as u8);
    } else if v < (1 << 28) {
        bytes.push((v | B) as u8);
        bytes.push(((v >> 7) | B) as u8);
        bytes.push(((v >> 14) | B) as u8);
        bytes.push((v >> 21) as u8);
    } else {
        bytes.push((v | B) as u8);
        bytes.push(((v >> 7) | B) as u8);
        bytes.push(((v >> 14) | B) as u8);
        bytes.push(((v >> 21) | B) as u8);
        bytes.push((v >> 28) as u8);
    }
    bytes
}

/// Actually, this function contains the scenario of encode_varint32.
fn encode_varint64(mut v: u64) -> Vec<u8> {
    let mut bytes: Vec<u8> = Vec::new();
    while v >= B as u64 {
        bytes.push((v | B as u64) as u8);
        v >>= 7;
    }
    bytes.push(v as u8);
    bytes
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed32_test() {
        let mut s = Vec::new();
        for i in 0..100_000u32 {
            put_fixed32(&mut s, i);
        }

        for i in 0..100_000u32 {
            let idx = (i * 4) as usize;
            assert_eq!(i, decode_fixed32([s[idx], s[idx + 1], s[idx + 2], s[idx + 3]]));
        }
    }

    #[test]
    fn fixed64_test() {
        let mut s = Vec::new();
        for power in 0..64 {
            let v = 1u64 << power;
            put_fixed64(&mut s, v - 1);
            put_fixed64(&mut s, v + 0);
            put_fixed64(&mut s, v + 1);
        }

        for power in 0..64 {
            let v = 1u64 << power;
            let idx = (power * 24) as usize;
            assert_eq!(v - 1, decode_fixed64([s[idx], s[idx + 1], s[idx + 2], s[idx + 3],
                                            s[idx + 4], s[idx + 5], s[idx + 6], s[idx + 7]]));
            assert_eq!(v + 0, decode_fixed64([s[idx + 8], s[idx + 9], s[idx + 10], s[idx + 11],
                                            s[idx + 12], s[idx + 13], s[idx + 14], s[idx + 15]]));
            assert_eq!(v + 1, decode_fixed64([s[idx + 16], s[idx + 17], s[idx + 18], s[idx + 19],
                                            s[idx + 20], s[idx + 21], s[idx + 22], s[idx + 23]]));
        }
    }

    #[test]
    fn encoding_output_test() {
        // Test that encoding routines generate little-endian encodings
        let mut dst = Vec::new();
        put_fixed32(&mut dst, 0x04030201);
        assert_eq!(4, dst.len());
        assert_eq!(0x01, dst[0]);
        assert_eq!(0x02, dst[1]);
        assert_eq!(0x03, dst[2]);
        assert_eq!(0x04, dst[3]);

        dst.clear();
        put_fixed64(&mut dst, 0x0807060504030201);
        assert_eq!(8, dst.len());
        assert_eq!(0x01, dst[0]);
        assert_eq!(0x02, dst[1]);
        assert_eq!(0x03, dst[2]);
        assert_eq!(0x04, dst[3]);
        assert_eq!(0x05, dst[4]);
        assert_eq!(0x06, dst[5]);
        assert_eq!(0x07, dst[6]);
        assert_eq!(0x08, dst[7]);
    }

    #[test]
    fn varint32_test() {
        let mut s = Vec::new();
        for i in 0..(32u32 * 32u32) {
            let v = i / 32 << i % 32;
            put_varint32(&mut s, v);
        }

        let mut idx = 0isize;
        for i in 0..(32u32 * 32u32) {
            let expected = i / 32 << i % 32;
            let start = idx;
            let (next, actual) = get_varint32_idx(&s, idx);
            idx = next;
            assert!(idx != -1);
            assert_eq!(expected, actual);
            assert_eq!(varint_length(actual as u64), (idx - start) as usize);
        }
        assert_eq!(idx as usize, s.len());
    }

    #[test]
    fn varint64_test() {
        // Construct the list of values to check
        let mut values = Vec::new();
        // Some special values
        values.push(0u64);
        values.push(100);
        values.push(u64::MAX);
        values.push(u64::MAX - 1);
        for i in 0..64 {
            let v = 1u64 << i;
            values.push(v);
            values.push(v - 1);
            values.push(v + 1);
        }

        let mut s = Vec::new();
        for v in &values {
            put_varint64(&mut s, *v);
        }

        let mut idx = 0isize;
        for v in &values {
            assert!((idx as usize) < s.len());
            let start = idx;
            let (next, actual) = get_varint64_idx(&s, idx);
            idx = next;
            assert!(idx != -1);
            assert_eq!(*v, actual);
            assert_eq!(varint_length(actual), (idx - start) as usize);
        }
        assert_eq!(idx as usize, s.len());
    }

    #[test]
    fn varint32_overflow_test() {
        let input = vec![0x81, 0x82, 0x83, 0x84, 0x85, 0x11];
        let (next, _) = get_varint32_idx(&input, 0);
        assert!(next == -1);
    }

    #[test]
    fn varint32_truncation_test() {
        let large_value = (1u32 << 31) + 100;
        let mut s = Vec::new();
        put_varint32(&mut s, large_value);
        for i in 0..(s.len() - 1) {
            let s_: Vec<u8> = s.iter().take(i).map(|e| *e).collect();
            let (next, _) = get_varint32_idx(&s_, 0);
            assert!(next == -1);
        }
        let (next, result) = get_varint32_idx(&s, 0);
        assert!(next != -1);
        assert_eq!(large_value, result);
    }

    #[test]
    fn varint64_overflow_test() {
        let input = vec![0x81, 0x82, 0x83, 0x84, 0x85, 0x81, 0x82, 0x83, 0x84, 0x85, 0x11];
        let (next, _) = get_varint64_idx(&input, 0);
        assert!(next == -1);
    }

    #[test]
    fn varint64_truncation_test() {
        let large_value = (1u64 << 63) + 100;
        let mut s = Vec::new();
        put_varint64(&mut s, large_value);
        for i in 0..(s.len() - 1) {
            let s_: Vec<u8> = s.iter().take(i).map(|e| *e).collect();
            let (next, _) = get_varint64_idx(&s_, 0);
            assert!(next == -1);
        }
        let (next, result) = get_varint64_idx(&s, 0);
        assert!(next != -1);
        assert_eq!(large_value, result);
    }

    #[test]
    fn strings_test() {
        let mut s = Vec::new();
        put_length_prefixed_slice(&mut s, &Slice::new(b""));
        put_length_prefixed_slice(&mut s, &Slice::new(b"foo"));
        put_length_prefixed_slice(&mut s, &Slice::new(b"bar"));
        put_length_prefixed_slice(&mut s, &Slice::new(&['x' as u8; 200]));

        let mut input = Slice::new(&s);
        let v = get_length_prefixed_slice(&mut input);
        assert!(v.is_some());
        assert!(v.unwrap() == b"");
        let v = get_length_prefixed_slice(&mut input);
        assert!(v.is_some());
        assert!(v.unwrap() == b"foo");
        let v = get_length_prefixed_slice(&mut input);
        assert!(v.is_some());
        assert!(v.unwrap() == b"bar");
        let v = get_length_prefixed_slice(&mut input);
        assert!(v.is_some());
        assert!(v.unwrap() == &['x' as u8; 200]);
        assert!(input.size() == 0);
    }
}