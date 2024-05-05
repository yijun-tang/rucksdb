use crc32c::{crc32c, crc32c_append};

static MASK_DELTA: u32 = 0xa282ead8;

/// Return the crc32c of concat(A, data[0,n-1]) where init_crc is the
/// crc32c of some string A.  Extend() is often used to maintain the
/// crc32c of a stream of data.
#[inline]
pub(crate) fn extend(init_crc: u32, data: &[u8]) -> u32 {
    crc32c_append(init_crc, data)
}

/// Return the crc32c of data[0,n-1]
#[inline]
pub(crate) fn value(data: &[u8]) -> u32 {
    crc32c(data)
}

/// Return a masked representation of crc.
/// 
/// Motivation: it is problematic to compute the CRC of a string that
/// contains embedded CRCs.  Therefore we recommend that CRCs stored
/// somewhere (e.g., in files) should be masked before being stored.
#[inline]
pub(crate) fn mask(crc: u32) -> u32 {
    // Rotate right by 15 bits and add a constant.
    (crc >> 15 | crc << 17) + MASK_DELTA
}

/// Return the crc whose masked representation is masked_crc.
#[inline]
pub(crate) fn unmask(masked_crc: u32) -> u32 {
    let rot = masked_crc - MASK_DELTA;
    rot >> 17 | rot << 15
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_results_test() {
        // From rfc3720 section B.4.
        let mut buf = [0u8; 32];
        assert_eq!(0x8a9136aa, value(&buf));

        buf = [0xff; 32];
        assert_eq!(0x62a8ab43, value(&buf));

        for i in 0..buf.len() {
            buf[i] = i as u8;
        }
        assert_eq!(0x46dd794e, value(&buf));

        for i in 0..buf.len() {
            buf[i] = 31 - i as u8;
        }
        assert_eq!(0x113fdb5c, value(&buf));

        let data = [
            0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00,
            0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x18, 0x28, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        ];
        assert_eq!(0xd9963a56, value(&data));
    }

    #[test]
    fn values_test() {
        assert_ne!(value(b"a"), value(b"foo"));
    }

    #[test]
    fn extend_test() {
        assert_eq!(value(b"hello world"), extend(value(b"hello "), b"world"));
    }

    #[test]
    fn mask_test() {
        let crc = value(b"foo");
        assert_ne!(crc, mask(crc));
        assert_ne!(crc, mask(mask(crc)));
        assert_eq!(crc, unmask(mask(crc)));
        assert_eq!(crc, unmask(unmask(mask(mask(crc)))));
    }
}
