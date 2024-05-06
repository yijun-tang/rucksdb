//! The representation of a DB consists of a set of Versions.  The
//! newest version is called "current".  Older versions may be kept
//! around to provide a consistent view to live iterators.
//! 
//! Each Version keeps track of a set of Table files per level.  The
//! entire set of versions is maintained in a VersionSet.
//! 
//! Version,VersionSet are thread-compatible, but require external
//! synchronization on all accesses.

use std::{cmp::Ordering, sync::Arc};

use crate::{comparator::Comparator, db::dbformat::{InternalKey, MAX_SEQUENCE_NUMBER, VALUE_TYPE_FOR_SEEK}, slice::Slice};

use super::{dbformat::InternalKeyComparator, version_edit::FileMetaData};

fn find_file(cmp: &InternalKeyComparator, files: &Vec<FileMetaData>, key: &Slice) -> usize {
    let mut left = 0;
    let mut right = files.len();
    while left < right {
        let mid = (left + right) / 2;
        let f = &files[mid];
        if cmp.compare(&f.largest.encode(), key) == Ordering::Less {
            // Key at "mid.largest" is < "target".  Therefore all
            // files at or before "mid" are uninteresting.
            left = mid + 1;
        } else {
            // Key at "mid.largest" is >= "target".  Therefore all files
            // after "mid" are uninteresting.
            right = mid;
        }
    }
    right
}

fn after_file(cmp: &Arc<dyn Comparator>, user_key: &Slice, f: &FileMetaData) -> bool {
    // null user_key occurs before all keys and is therefore never after *f
    !user_key.is_empty() &&
    cmp.compare(user_key, &f.largest.user_key()) == Ordering::Greater
}

fn before_file(cmp: &Arc<dyn Comparator>, user_key: &Slice, f: &FileMetaData) -> bool {
    // null user_key occurs after all keys and is therefore never before *f
    !user_key.is_empty() &&
    cmp.compare(user_key, &f.smallest.user_key()) == Ordering::Less
}

/// Return true iff there exists at least one file overlaps with range
/// [smallest_user_key, largest_user_key].
fn some_file_overlaps_range(cmp: &InternalKeyComparator, disjoint_sorted_files: bool,
                            files: &Vec<FileMetaData>,
                            smallest_user_key: &Slice, largest_user_key: &Slice) -> bool {
    let ucmp = cmp.user_comparator();
    if !disjoint_sorted_files {
        // Need to check against all files
        for file in files {
            if after_file(&ucmp, smallest_user_key, file) ||
                before_file(&ucmp, largest_user_key, file) {
                // No overlap
            } else {
                return true;
            }
        }
        return false;
    }

    // Binary search over file list
    let mut index = 0;
    if !smallest_user_key.is_empty() {
        // Find the earliest possible internal key for smallest_user_key
        let small_key = InternalKey::new_from(smallest_user_key, MAX_SEQUENCE_NUMBER, VALUE_TYPE_FOR_SEEK);
        index = find_file(cmp, files, &small_key.encode());
    }

    if index >= files.len() {
        // beginning of range is after all files, so no overlap.
        return false;
    }
    
    !before_file(&ucmp, largest_user_key, &files[index])
}

pub(crate) struct VersionSet {

}

impl VersionSet {
    pub(crate) fn new() -> Self {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::{comparator::bytewise_comparator, db::{dbformat::{InternalKey, InternalKeyComparator, ValueType}, version_edit::{FileMetaData, SequenceNumber}}, slice::Slice};

    use super::*;

    struct FindFileTest {
        disjoint_sorted_files_: bool,
        files_: Vec<FileMetaData>,
    }
    impl FindFileTest {
        fn new() -> Self {
            Self { disjoint_sorted_files_: true, files_: Vec::new() }
        }
        fn add(&mut self, smallest: &str, largest: &str, smallest_seq: SequenceNumber, largest_seq: SequenceNumber) {
            let mut f = FileMetaData::new();
            f.number = self.files_.len() as u64 + 1;
            f.smallest = InternalKey::new_from(&Slice::new(smallest.as_bytes()), smallest_seq, ValueType::type_value());
            f.largest = InternalKey::new_from(&Slice::new(largest.as_bytes()), largest_seq, ValueType::type_value());
            self.files_.push(f);
        }
        fn find(&self, key: &str) -> usize {
            let target = InternalKey::new_from(&Slice::new(key.as_bytes()), 
                                                            100, ValueType::type_value());
            let cmp = InternalKeyComparator::new(bytewise_comparator());
            find_file(&cmp, &self.files_, &target.encode())
        }
        fn overlaps(&self, smallest: &str, largest: &str) -> bool {
            let cmp = InternalKeyComparator::new(bytewise_comparator());
            some_file_overlaps_range(&cmp, self.disjoint_sorted_files_, &self.files_,
            &Slice::new(smallest.as_bytes()), &Slice::new(largest.as_bytes()))
        }
    }

    #[test]
    fn empty_test() {
        let t = FindFileTest::new();
        assert_eq!(0, t.find("foo"));
        assert!(!t.overlaps("a", "z"));
        assert!(!t.overlaps("", "z"));
        assert!(!t.overlaps("a", ""));
        assert!(!t.overlaps("", ""));
    }

    #[test]
    fn single_test() {
        let mut t = FindFileTest::new();
        t.add("p", "q", 100, 100);
        assert_eq!(0, t.find("a"));
        assert_eq!(0, t.find("p"));
        assert_eq!(0, t.find("p1"));
        assert_eq!(0, t.find("q"));
        assert_eq!(1, t.find("q1"));
        assert_eq!(1, t.find("z"));

        assert!(!t.overlaps("a", "b"));
        assert!(!t.overlaps("z1", "z2"));
        assert!(t.overlaps("a", "p"));
        assert!(t.overlaps("a", "q"));
        assert!(t.overlaps("a", "z"));
        assert!(t.overlaps("p", "p1"));
        assert!(t.overlaps("p", "q"));
        assert!(t.overlaps("p", "z"));
        assert!(t.overlaps("p1", "p2"));
        assert!(t.overlaps("p1", "z"));
        assert!(t.overlaps("q", "q"));
        assert!(t.overlaps("q", "q1"));

        assert!(!t.overlaps("", "j"));
        assert!(!t.overlaps("r", ""));
        assert!(t.overlaps("", "p"));
        assert!(t.overlaps("", "p1"));
        assert!(t.overlaps("q", ""));
        assert!(t.overlaps("", ""));
    }

    #[test]
    fn multiple_test() {
        let t = FindFileTest::new();
    }

    #[test]
    fn test() {
        let t = FindFileTest::new();
    }
}
