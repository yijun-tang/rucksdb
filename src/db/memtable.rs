use std::{cmp::Ordering, sync::Arc};

use crate::{comparator::Comparator, db::skiplist::Iter, slice::Slice, status::Status, util::{arena::Arena, coding::{decode_fixed64_bytes, encode_fixed64_to, encode_varint32_to, get_varint32_idx, varint_length}}};

use super::{dbformat::{InternalKeyComparator, LookupKey, ValueType}, skiplist::{self, SkipList}, version_edit::SequenceNumber};

type Table = Arc<SkipList<Vec<u8, Arena>, KeyComparator>, Arena>;

pub(crate) struct MemTable {
    comparator_: KeyComparator,
    refs_: i32,
    arena_: Arena,
    table_: Table,
}

impl MemTable {
    /// MemTables are reference counted.  The initial reference count
    /// is zero and the caller must call Ref() at least once.
    pub(crate) fn new(comparator: &InternalKeyComparator) -> Self {
        let cmp = KeyComparator { comparator: comparator.clone() };
        let arena = Arena::new();
        let key: Vec<u8, Arena> = Vec::new_in(arena.clone());
        Self {
            comparator_: cmp.clone(),
            refs_: 0,
            arena_: arena.clone(),
            table_: Arc::new_in(SkipList::new_in(key, cmp, arena.clone()), arena),
        }
    }

    /// Increase reference count.
    pub(crate) fn ref_(&mut self) {
        self.refs_ += 1;
    } 

    /// Add an entry into memtable that maps key to value at the
    /// specified sequence number and with the specified type.
    /// Typically value will be empty if type==kTypeDeletion.
    pub(crate) fn add(&self, seq: SequenceNumber, type_: ValueType, key: &Slice, value: &Slice) {
        // Format of an entry is concatenation of:
        //  key_size     : varint32 of internal_key.size()
        //  key bytes    : char[internal_key.size()]
        //  tag          : uint64((sequence << 8) | type)
        //  value_size   : varint32 of value.size()
        //  value bytes  : char[value.size()]
        let key_size = key.size();
        let val_size = value.size();
        let internal_key_size = key_size + 8;
        let encoded_len = varint_length(internal_key_size as u64) + 
                                internal_key_size + varint_length(val_size as u64) + val_size;
        let mut buf: Vec<u8, Arena> = Vec::with_capacity_in(encoded_len, self.arena_.clone());
        encode_varint32_to(&mut buf, internal_key_size as u32);
        buf.extend(key.data());
        encode_fixed64_to(&mut buf, (seq << 8) | (type_.value() as u64));
        encode_varint32_to(&mut buf, val_size as u32);
        buf.extend(value.data());
        debug_assert!(buf.len() == encoded_len);
        self.table_.insert(buf);
    }

    /// If memtable contains a value for key, store it in *value and return true.
    /// If memtable contains a deletion for key, store a NotFound() error
    /// in *status and return true.
    /// Else, return false.
    pub(crate) fn get(&self, key: &LookupKey) -> (Option<Vec<u8>>, Option<Status>, bool) {
        let memkey = key.memtable_key();
        let mut iter = Iter::new(self.table_.clone());
        iter.seek(&memkey.data().to_vec_in(self.arena_.clone()));
        if iter.valid() {
            let entry = iter.key();
            let (next, n) = get_varint32_idx(&entry, 0);
            if self.comparator_.comparator.user_comparator()
                    .compare(&Slice::new_with_range(&entry, next as usize, (next as usize) + (n as usize) - 8),
                            &key.user_key()) == Ordering::Equal {
                // Correct user key
                let tag = decode_fixed64_bytes(&entry[((next as usize) + (n as usize) - 8)..((next as usize) + (n as usize))]);
                let vt = (tag & 0xff) as u8;
                if vt == ValueType::type_value().value() {
                    let v = get_length_prefixed_slice(&entry[((next as usize) + (n as usize))..]);
                    return (Some(v.data().to_vec()), None, true);
                } else if vt == ValueType::type_deletion().value() {
                    return (None, Some(Status::not_found("", "")), true);
                }
            }
        }
        (None, None, false)
    }

    /// Returns an estimate of the number of bytes of data in use by this
    /// data structure. It is safe to call when MemTable is being modified.
    pub(crate) fn approximate_memory_usage(&self) -> usize {
        return self.arena_.memory_usage()
    }
}

#[derive(Clone)]
struct KeyComparator {
    comparator: InternalKeyComparator,
}

impl skiplist::Comparator<Vec<u8, Arena>> for KeyComparator {
    fn compare(&self, left: &Vec<u8, Arena>, right: &Vec<u8, Arena>) -> std::cmp::Ordering {
        // Internal keys are encoded as length-prefixed strings.
        let a = get_length_prefixed_slice(left);
        let b = get_length_prefixed_slice(right);
        self.comparator.compare(&a, &b)
    }
}

fn get_length_prefixed_slice(data: &[u8]) -> Slice {
    let (next, n) = get_varint32_idx(data, 0);
    Slice::new_with_range(&data, next as usize, next as usize + n as usize)
}
