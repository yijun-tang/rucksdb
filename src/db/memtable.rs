use crate::util::arena::Arena;

use super::{dbformat::InternalKeyComparator, skiplist::SkipList};


pub(crate) struct MemTable {
    comparator_: KeyComparator,
    refs_: i32,
    arena_: Arena,
    // table_: Table,
}

impl MemTable {
    /// MemTables are reference counted.  The initial reference count
    /// is zero and the caller must call Ref() at least once.
    pub(crate) fn new(comparator: &InternalKeyComparator) -> Self {
        todo!()
    }
}

struct KeyComparator {
    comparator: InternalKeyComparator,
}

impl KeyComparator {
    fn new(c: &InternalKeyComparator) -> Self {
        Self { comparator: c.clone() }
    }
}
