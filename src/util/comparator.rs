use std::sync::Arc;

use once_cell::sync::Lazy;

use crate::comparator::Comparator;

pub(crate) static BYTEWISE_COMPARATOR: Lazy<Arc<dyn Comparator + Sync + Send>> = Lazy::new(|| {
    Arc::new(BytewiseComparator) as Arc<dyn Comparator + Sync + Send>
});

pub(crate) struct BytewiseComparator;

impl Comparator for BytewiseComparator {
    fn name(&self) -> &'static str { "leveldb.BytewiseComparator" }
    
    fn compare(&self, a: &crate::slice::Slice, b: &crate::slice::Slice) -> std::cmp::Ordering {
        a.compare(b)
    }
}
