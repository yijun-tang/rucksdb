//! The representation of a DB consists of a set of Versions.  The
//! newest version is called "current".  Older versions may be kept
//! around to provide a consistent view to live iterators.
//! 
//! Each Version keeps track of a set of Table files per level.  The
//! entire set of versions is maintained in a VersionSet.
//! 
//! Version,VersionSet are thread-compatible, but require external
//! synchronization on all accesses.

use std::{cmp::Ordering, rc::{Rc, Weak}, sync::Arc};

use crate::{comparator::Comparator, db::dbformat::{InternalKey, MAX_SEQUENCE_NUMBER, VALUE_TYPE_FOR_SEEK}, slice::Slice, status::Status};

use super::{dbformat::{InternalKeyComparator, NUM_LEVELS}, version_edit::FileMetaData};

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

pub(crate) struct Version {
    vset_: Rc<VersionSet>,  // VersionSet to which this Version belongs
    next_: Rc<Version>,     // Next version in linked list
    prev_: Weak<Version>,   // Previous version in linked list
    refs_: i32,             // Number of live refs to this version

    // List of files per level
    files_: Vec<FileMetaData>,

    // Next file to compact based on seek stats.
    file_to_compact_: FileMetaData,
    file_to_compact_level_: i32,

    // Level that should be compacted next and its compaction score.
    // Score < 1 means compaction is not strictly needed.  These fields
    // are initialized by Finalize().
    compaction_score_: f64,
    compaction_level_: i32,
}
pub(crate) struct GetStats {
    pub(crate) seek_file: FileMetaData,
    pub(crate) seek_file_level: i32,
}
impl Version {
    fn new(vset: Rc<VersionSet>) -> Self {
        Self {
            vset_: vset,
            next_: todo!(),
            prev_: todo!(),
            refs_: 0,
            files_: vec![FileMetaData::new(); NUM_LEVELS as usize],
            file_to_compact_: FileMetaData::new(),
            file_to_compact_level_: -1,
            compaction_score_: -1.0,
            compaction_level_: -1,
        }
    }
}

pub(crate) struct VersionSet {

    next_file_number_: u64,
}
impl VersionSet {
    pub(crate) fn new() -> Self {
        todo!()
    }

    /// Recover the last saved descriptor from persistent storage.
    pub(crate) fn recover(&mut self) -> Result<bool, Status> {
        todo!()
    }

    /// Allocate and return a new file number
    pub(crate) fn new_file_number(&mut self) -> u64 {
        let file_number = self.next_file_number_;
        self.next_file_number_ += 1;
        file_number
    }
}

/// A Compaction encapsulates information about a compaction.
pub(crate) struct Compaction {

}
impl Compaction {
    fn new() -> Self {
        todo!()
    }
}

/// Finds the largest key in a vector of files. Returns None if files is empty.
fn find_largest_key(icmp: &InternalKeyComparator, files: &Vec<FileMetaData>) -> Option<InternalKey> {
    if files.is_empty() {
        return None;
    }
    let mut largest_key = &files[0].largest;
    for i in 1..files.len() {
        let f = &files[i];
        if icmp.compare2(&f.largest, largest_key) == Ordering::Greater {
            largest_key = &f.largest;
        }
    }
    Some(largest_key.clone())
}

/// Finds minimum file b2=(l2, u2) in level file for which l2 > u1 and
/// user_key(l2) = user_key(u1)
fn find_smallest_boundary_file(icmp: &InternalKeyComparator, 
                                level_files: &Vec<FileMetaData>, 
                                largest_key: &InternalKey) -> Option<FileMetaData> {
    let mut smallest_boundary_file: Option<&FileMetaData> = None;
    let user_cmp = icmp.user_comparator();
    for f in level_files {
        // boundary
        if icmp.compare2(&f.smallest, largest_key) == Ordering::Greater &&
            user_cmp.compare(&f.smallest.user_key(), &largest_key.user_key()) == Ordering::Equal {
            // smallest
            if smallest_boundary_file.is_none() ||
                icmp.compare2(&f.smallest, &smallest_boundary_file.unwrap().smallest) == Ordering::Less {
                smallest_boundary_file = Some(f);
            }
        }
    }
    smallest_boundary_file.cloned()
}

/// Extracts the largest file b1 from |compaction_files| and then searches for a
/// b2 in |level_files| for which user_key(u1) = user_key(l2). If it finds such a
/// file b2 (known as a boundary file) it adds it to |compaction_files| and then
/// searches again using this new upper bound.
/// 
/// If there are two blocks, b1=(l1, u1) and b2=(l2, u2) and
/// user_key(u1) = user_key(l2), and if we compact b1 but not b2 then a
/// subsequent get operation will yield an incorrect result because it will
/// return the record from b2 in level i rather than from b1 because it searches
/// level by level for records matching the supplied user key.
/// 
/// parameters:
///   in     level_files:      List of files to search for boundary files.
///   in/out compaction_files: List of files to extend by adding boundary files.
fn add_boundary_inputs(icmp: &InternalKeyComparator, 
                        level_files: &Vec<FileMetaData>, 
                        compaction_files: &mut Vec<FileMetaData>) {
    // Quick return if compaction_files is empty.
    match find_largest_key(icmp, compaction_files) {
        Some(mut largest_key) => {
            let mut continue_searching = true;
            while continue_searching {
                // If a boundary file was found advance largest_key, otherwise we're done.
                match find_smallest_boundary_file(icmp, level_files, &largest_key) {
                    Some(smallest_boundary_file) => {
                        compaction_files.push(smallest_boundary_file);
                        largest_key = compaction_files.last().unwrap().largest.clone();
                    },
                    None => { continue_searching = false; },
                }
            }
        },
        None => { return; },
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
        let mut t = FindFileTest::new();
        t.add("150", "200", 100, 100);
        t.add("200", "250", 100, 100);
        t.add("300", "350", 100, 100);
        t.add("400", "450", 100, 100);
        assert_eq!(0, t.find("100"));
        assert_eq!(0, t.find("150"));
        assert_eq!(0, t.find("151"));
        assert_eq!(0, t.find("199"));
        assert_eq!(0, t.find("200"));
        assert_eq!(1, t.find("201"));
        assert_eq!(1, t.find("249"));
        assert_eq!(1, t.find("250"));
        assert_eq!(2, t.find("251"));
        assert_eq!(2, t.find("299"));
        assert_eq!(2, t.find("300"));
        assert_eq!(2, t.find("349"));
        assert_eq!(2, t.find("350"));
        assert_eq!(3, t.find("351"));
        assert_eq!(3, t.find("400"));
        assert_eq!(3, t.find("450"));
        assert_eq!(4, t.find("451"));

        assert!(!t.overlaps("100", "149"));
        assert!(!t.overlaps("251", "299"));
        assert!(!t.overlaps("451", "500"));
        assert!(!t.overlaps("351", "399"));

        assert!(t.overlaps("100", "150"));
        assert!(t.overlaps("100", "200"));
        assert!(t.overlaps("100", "300"));
        assert!(t.overlaps("100", "400"));
        assert!(t.overlaps("100", "500"));
        assert!(t.overlaps("375", "400"));
        assert!(t.overlaps("450", "450"));
        assert!(t.overlaps("450", "500"));
    }

    #[test]
    fn multiple_null_boundaries_test() {
        let mut t = FindFileTest::new();
        t.add("150", "200", 100, 100);
        t.add("200", "250", 100, 100);
        t.add("300", "350", 100, 100);
        t.add("400", "450", 100, 100);

        assert!(!t.overlaps("", "149"));
        assert!(!t.overlaps("451", ""));
        assert!(t.overlaps("", ""));
        assert!(t.overlaps("", "150"));
        assert!(t.overlaps("", "199"));
        assert!(t.overlaps("", "200"));
        assert!(t.overlaps("", "201"));
        assert!(t.overlaps("", "400"));
        assert!(t.overlaps("", "800"));
        assert!(t.overlaps("100", ""));
        assert!(t.overlaps("200", ""));
        assert!(t.overlaps("449", ""));
        assert!(t.overlaps("450", ""));
    }

    #[test]
    fn overlap_sequence_checks_test() {
        let mut t = FindFileTest::new();
        t.add("200", "200", 5000, 3000);
        assert!(!t.overlaps("199", "199"));
        assert!(!t.overlaps("201", "300"));
        assert!(t.overlaps("200", "200"));
        assert!(t.overlaps("190", "200"));
        assert!(t.overlaps("200", "210"));
    }

    #[test]
    fn overlapping_files_test() {
        let mut t = FindFileTest::new();
        t.add("150", "600", 100, 100);
        t.add("400", "500", 100, 100);
        t.disjoint_sorted_files_ = false;
        assert!(!t.overlaps("100", "149"));
        assert!(!t.overlaps("601", "700"));
        assert!(t.overlaps("100", "150"));
        assert!(t.overlaps("100", "200"));
        assert!(t.overlaps("100", "300"));
        assert!(t.overlaps("100", "400"));
        assert!(t.overlaps("100", "500"));
        assert!(t.overlaps("375", "400"));
        assert!(t.overlaps("450", "450"));
        assert!(t.overlaps("450", "500"));
        assert!(t.overlaps("450", "700"));
        assert!(t.overlaps("600", "700"));
    }

    struct AddBoundaryInputsTest {
        level_files_: Vec<FileMetaData>,
        compaction_files_: Vec<FileMetaData>,
        all_files_: Vec<FileMetaData>,
        icmp_: InternalKeyComparator,
    }
    impl AddBoundaryInputsTest {
        fn new() -> Self {
            let icmp_ = InternalKeyComparator::new(bytewise_comparator());
            Self {
                level_files_: Vec::new(),
                compaction_files_: Vec::new(),
                all_files_: Vec::new(),
                icmp_,
            }
        }
        fn create_file_meta_data(&mut self, number: u64, smallest: InternalKey, 
                                largest: InternalKey) -> FileMetaData {
            let mut f = FileMetaData::new();
            f.number = number;
            f.smallest = smallest;
            f.largest = largest;
            self.all_files_.push(f.clone());
            f
        }
    }

    #[test]
    fn empty_file_sets_test() {
        let mut t = AddBoundaryInputsTest::new();
        add_boundary_inputs(&t.icmp_, &t.level_files_, &mut t.compaction_files_);
        assert!(t.compaction_files_.is_empty());
        assert!(t.level_files_.is_empty());
    }

    #[test]
    fn empty_level_files_test() {
        let mut t = AddBoundaryInputsTest::new();
        let f1 = t.create_file_meta_data(1, 
                                InternalKey::new_from(&Slice::new(b"100"), 2, ValueType::type_value()),
                                InternalKey::new_from(&Slice::new(b"100"), 1, ValueType::type_value()));
        t.compaction_files_.push(f1.clone());

        add_boundary_inputs(&t.icmp_, &t.level_files_, &mut t.compaction_files_);
        assert_eq!(1, t.compaction_files_.len());
        assert_eq!(f1, t.compaction_files_[0]);
        assert!(t.level_files_.is_empty());
    }

    #[test]
    fn empty_compaction_files_test() {
        let mut t = AddBoundaryInputsTest::new();
        let f1 = t.create_file_meta_data(1, 
                                InternalKey::new_from(&Slice::new(b"100"), 2, ValueType::type_value()),
                                InternalKey::new_from(&Slice::new(b"100"), 1, ValueType::type_value()));
        t.level_files_.push(f1.clone());

        add_boundary_inputs(&t.icmp_, &t.level_files_, &mut t.compaction_files_);
        assert!(t.compaction_files_.is_empty());
        assert_eq!(1, t.level_files_.len());
        assert_eq!(f1, t.level_files_[0]);
    }

    #[test]
    fn no_boundary_files_test() {
        let mut t = AddBoundaryInputsTest::new();
        let f1 = t.create_file_meta_data(1, 
                                InternalKey::new_from(&Slice::new(b"100"), 2, ValueType::type_value()),
                                InternalKey::new_from(&Slice::new(b"100"), 1, ValueType::type_value()));
        let f2 = t.create_file_meta_data(1, 
                                InternalKey::new_from(&Slice::new(b"200"), 2, ValueType::type_value()),
                                InternalKey::new_from(&Slice::new(b"200"), 1, ValueType::type_value()));
        let f3 = t.create_file_meta_data(1, 
                                InternalKey::new_from(&Slice::new(b"300"), 2, ValueType::type_value()),
                                InternalKey::new_from(&Slice::new(b"300"), 1, ValueType::type_value()));
        
        t.level_files_.push(f3.clone());
        t.level_files_.push(f2.clone());
        t.level_files_.push(f1);
        t.compaction_files_.push(f2);
        t.compaction_files_.push(f3);
        add_boundary_inputs(&t.icmp_, &t.level_files_, &mut t.compaction_files_);
        assert_eq!(2, t.compaction_files_.len());
    }

    #[test]
    fn one_boundary_file_test() {
        let mut t = AddBoundaryInputsTest::new();
        let f1 = t.create_file_meta_data(1, 
                                InternalKey::new_from(&Slice::new(b"100"), 3, ValueType::type_value()),
                                InternalKey::new_from(&Slice::new(b"100"), 2, ValueType::type_value()));
        let f2 = t.create_file_meta_data(1, 
                                InternalKey::new_from(&Slice::new(b"100"), 1, ValueType::type_value()),
                                InternalKey::new_from(&Slice::new(b"200"), 3, ValueType::type_value()));
        let f3 = t.create_file_meta_data(1, 
                                InternalKey::new_from(&Slice::new(b"300"), 2, ValueType::type_value()),
                                InternalKey::new_from(&Slice::new(b"300"), 1, ValueType::type_value()));
        
        t.level_files_.push(f3);
        t.level_files_.push(f2.clone());
        t.level_files_.push(f1.clone());
        t.compaction_files_.push(f1.clone());
        add_boundary_inputs(&t.icmp_, &t.level_files_, &mut t.compaction_files_);
        assert_eq!(2, t.compaction_files_.len());
        assert_eq!(f1, t.compaction_files_[0]);
        assert_eq!(f2, t.compaction_files_[1]);
    }

    #[test]
    fn two_boundary_files_test() {
        let mut t = AddBoundaryInputsTest::new();
        let f1 = t.create_file_meta_data(1, 
                                InternalKey::new_from(&Slice::new(b"100"), 6, ValueType::type_value()),
                                InternalKey::new_from(&Slice::new(b"100"), 5, ValueType::type_value()));
        let f2 = t.create_file_meta_data(1, 
                                InternalKey::new_from(&Slice::new(b"100"), 2, ValueType::type_value()),
                                InternalKey::new_from(&Slice::new(b"300"), 1, ValueType::type_value()));
        let f3 = t.create_file_meta_data(1, 
                                InternalKey::new_from(&Slice::new(b"100"), 4, ValueType::type_value()),
                                InternalKey::new_from(&Slice::new(b"100"), 3, ValueType::type_value()));
        
        t.level_files_.push(f3.clone());
        t.level_files_.push(f2.clone());
        t.level_files_.push(f1.clone());
        t.compaction_files_.push(f1.clone());
        add_boundary_inputs(&t.icmp_, &t.level_files_, &mut t.compaction_files_);
        assert_eq!(3, t.compaction_files_.len());
        assert_eq!(f1, t.compaction_files_[0]);
        assert_eq!(f3, t.compaction_files_[1]);
        assert_eq!(f2, t.compaction_files_[2]);
    }

    #[test]
    fn disjoint_file_pointers_test() {
        let mut t = AddBoundaryInputsTest::new();
        let f1 = t.create_file_meta_data(1, 
                                InternalKey::new_from(&Slice::new(b"100"), 6, ValueType::type_value()),
                                InternalKey::new_from(&Slice::new(b"100"), 5, ValueType::type_value()));
        let f2 = t.create_file_meta_data(1, 
                                InternalKey::new_from(&Slice::new(b"100"), 6, ValueType::type_value()),
                                InternalKey::new_from(&Slice::new(b"100"), 5, ValueType::type_value()));
        let f3 = t.create_file_meta_data(1, 
                                InternalKey::new_from(&Slice::new(b"100"), 2, ValueType::type_value()),
                                InternalKey::new_from(&Slice::new(b"300"), 1, ValueType::type_value()));
        let f4 = t.create_file_meta_data(1, 
                                InternalKey::new_from(&Slice::new(b"100"), 4, ValueType::type_value()),
                                InternalKey::new_from(&Slice::new(b"100"), 3, ValueType::type_value()));
        
        t.level_files_.push(f2.clone());
        t.level_files_.push(f3.clone());
        t.level_files_.push(f4.clone());
        t.compaction_files_.push(f1.clone());
        add_boundary_inputs(&t.icmp_, &t.level_files_, &mut t.compaction_files_);
        assert_eq!(3, t.compaction_files_.len());
        assert_eq!(f1, t.compaction_files_[0]);
        assert_eq!(f4, t.compaction_files_[1]);
        assert_eq!(f3, t.compaction_files_[2]);
    }
}
