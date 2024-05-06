use std::{cmp::Ordering, sync::Arc};

use crate::{slice::Slice, util::comparator::BYTEWISE_COMPARATOR};


/// A Comparator object provides a total order across slices that are
/// used as keys in an sstable or a database.  A Comparator implementation
/// must be thread-safe since leveldb may invoke its methods concurrently
/// from multiple threads.
pub trait Comparator {
    /// The name of the comparator.  Used to check for comparator
    /// mismatches (i.e., a DB created with one comparator is
    /// accessed using a different comparator.
    /// 
    /// The client of this package should switch to a new name whenever
    /// the comparator implementation changes in a way that will cause
    /// the relative ordering of any two keys to change.
    /// 
    /// Names starting with "leveldb." are reserved and should not be used
    /// by any clients of this package.
    fn name(&self) -> &'static str;

    /// Three-way comparison.  Returns value:
    ///   < 0 iff "a" < "b",
    ///   == 0 iff "a" == "b",
    ///   > 0 iff "a" > "b"
    fn compare(&self, a: &Slice, b: &Slice) -> Ordering;

    // Advanced functions: these are used to reduce the space requirements
    // for internal data structures like index blocks.


}

/// Return a builtin comparator that uses lexicographic byte-wise
/// ordering.  The result remains the property of this module and
/// must not be deleted.
pub fn bytewise_comparator() -> Arc<dyn Comparator> {
    BYTEWISE_COMPARATOR.clone()
}
