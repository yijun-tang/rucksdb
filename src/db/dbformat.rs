use crate::{slice::Slice, util::coding::put_fixed64};

use super::version_edit::SequenceNumber;

// Grouping of constants.  We may want to make some of these
// parameters set via options.
pub(crate) static NUM_LEVELS: i32 = 7;

// We leave eight bits empty at the bottom so a type and sequence#
// can be packed together into 64-bits.
pub(crate) static MAX_SEQUENCE_NUMBER: SequenceNumber = (1u64 << 56) - 1;

fn pack_sequence_and_type(seq: SequenceNumber, t: ValueType) -> u64 {
    debug_assert!(seq <= MAX_SEQUENCE_NUMBER);
    debug_assert!(t <= VALUE_TYPE_FOR_SEEK);
    (seq << 8) | (t.0 as u64)
}

pub(crate) fn append_internal_key(result: &mut Vec<u8>, key: &ParsedInternalKey) {
    result.extend(key.user_key.data());
    put_fixed64(result, pack_sequence_and_type(key.sequence, key.type_));
}

/// Value types encoded as the last component of internal keys.
/// DO NOT CHANGE THESE ENUM VALUES: they are embedded in the on-disk
/// data structures.
#[derive(PartialEq, PartialOrd, Clone, Copy)]
pub(crate) struct ValueType(u8);
impl ValueType {
    pub(crate) fn type_deletion() -> Self { Self(0) }
    pub(crate) const fn type_value() -> Self { Self(1) }
}
// kValueTypeForSeek defines the ValueType that should be passed when
// constructing a ParsedInternalKey object for seeking to a particular
// sequence number (since we sort sequence numbers in decreasing order
// and the value type is embedded as the low 8 bits in the sequence
// number in internal keys, we need to use the highest-numbered
// ValueType, not the lowest).
pub(crate) static VALUE_TYPE_FOR_SEEK: ValueType = ValueType::type_value();

pub(crate) struct ParsedInternalKey<'a> {
    user_key: Slice<'a>,
    sequence: SequenceNumber,
    type_: ValueType,
}
impl<'a> ParsedInternalKey<'a> {
    pub(crate) fn new(u: &'a Slice, seq: &SequenceNumber, t: ValueType) -> Self {
        Self { user_key: u.clone(), sequence: *seq, type_: t }
    }
}

/// Modules in this directory should keep internal keys wrapped inside
/// the following class instead of plain strings so that we do not
/// incorrectly use string comparisons instead of an InternalKeyComparator.
#[derive(Clone)]
pub(crate) struct InternalKey {
    rep_: Vec<u8>,
}

impl InternalKey {
    pub(crate) fn new() -> Self {
        // Leave rep_ as empty to indicate it is invalid
        Self { rep_: Vec::new() }
    }

    pub(crate) fn new_from(user_key: &Slice, s: SequenceNumber, t: ValueType) -> Self {
        let mut key = Self::new();
        append_internal_key(&mut key.rep_, &ParsedInternalKey::new(user_key, &s, t));
        key
    }

    pub(crate) fn encode(&self) -> Slice {
        debug_assert!(!self.rep_.is_empty());
        Slice::new(&self.rep_)
    }

    pub(crate) fn decode_from(s: &Slice) -> Self {
        Self { rep_: s.data().to_vec() }
    }
}
