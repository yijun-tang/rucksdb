//! Log format information shared by reader and writer.
//! See ../doc/log_format.md for more detail.

pub(crate) const MAX_RECORD_TYPE: u8 = RecordType::last_type().0;
pub(crate) static BLOCK_SIZE: usize = 32768;

// Header is checksum (4 bytes), length (2 bytes), type (1 byte).
pub(crate) static HEADER_SIZE: usize = 4 + 2 + 1;

pub(crate) struct RecordType(u8);

impl RecordType {
    // Zero is reserved for preallocated files
    pub(crate) fn zero_type() -> Self { Self(0) }

    pub(crate) fn full_type() -> Self { Self(1) }

    // For fragments
    pub(crate) fn first_type() -> Self { Self(2) }
    pub(crate) fn middle_type() -> Self { Self(3) }
    pub(crate) const fn last_type() -> Self { Self(4) }
}
