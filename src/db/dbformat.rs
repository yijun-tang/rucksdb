use std::{cmp::Ordering, sync::Arc};

use crate::{comparator::Comparator, slice::Slice, util::coding::{decode_fixed64, decode_fixed64_bytes, put_fixed64}};

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

/// Returns the user key portion of an internal key.
#[inline]
fn extract_user_key<'a>(internal_key: &'a [u8]) -> Slice<'a> {
    debug_assert!(internal_key.len() >= 8);
    Slice::new_with_range(internal_key, 0, internal_key.len() - 8)
}

pub(crate) struct InternalKeyComparator {
    user_comparator_: Arc<dyn Comparator>,
}
impl InternalKeyComparator {
    pub(crate) fn new(cmp: Arc<dyn Comparator>) -> Self {
        Self { user_comparator_: cmp }
    }
    pub(crate) fn user_comparator(&self) -> Arc<dyn Comparator> {
        self.user_comparator_.clone()
    }
}
impl Comparator for InternalKeyComparator {
    fn name(&self) -> &'static str {
        return "leveldb.InternalKeyComparator"
    }

    fn compare(&self, a: &Slice, b: &Slice) -> std::cmp::Ordering {
        // Order by:
        //    increasing user key (according to user-supplied comparator)
        //    decreasing sequence number
        //    decreasing type (though sequence# should be enough to disambiguate)
        let mut r = self.user_comparator_.compare(&extract_user_key(a.data()), &extract_user_key(b.data()));
        if r == Ordering::Equal {
            let anum = decode_fixed64_bytes(&a.data()[(a.size() - 8)..]);
            let bnum = decode_fixed64_bytes(&b.data()[(b.size() - 8)..]);
            if anum > bnum {
                r = Ordering::Less;
            } else if anum < bnum {
                r = Ordering::Greater;
            }
        }
        r
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

    pub(crate) fn user_key(&self) -> Slice {
        extract_user_key(&self.rep_)
    }
}
