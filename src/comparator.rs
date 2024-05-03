
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
}

struct BytewiseComparatorImpl;

impl Comparator for BytewiseComparatorImpl {
    fn name(&self) -> &'static str {
        "leveldb.BytewiseComparator"
    }
}
