use std::collections::HashSet;

use super::dbformat::InternalKey;


type SequenceNumber = u64;
type DeletedFileSet = HashSet<(i32, u64)>;

pub(crate) struct VersionEdit {
    comparator_: String,
    log_number_: u64,
    prev_log_number_: u64,
    next_file_number_: u64,
    last_sequence_: SequenceNumber,
    has_comparator_: bool,
    has_log_number_: bool,
    has_prev_log_number_: bool,
    has_next_file_number_: bool,
    has_last_sequence_: bool,
    compact_pointers_: Vec<(i32, InternalKey)>,
    deleted_files_: DeletedFileSet,
    new_files_: Vec<(i32, FileMetaData)>,
}

impl VersionEdit {
    pub(crate) fn new() -> Self {
        Self {
            comparator_: String::new(),
            log_number_: 0,
            prev_log_number_: 0,
            next_file_number_: 0,
            last_sequence_: 0,
            has_comparator_: false,
            has_log_number_: false,
            has_prev_log_number_: false,
            has_next_file_number_: false,
            has_last_sequence_: false,
            compact_pointers_: Vec::new(),
            deleted_files_: HashSet::new(),
            new_files_: Vec::new(),
        }
    }
}

pub(crate) struct FileMetaData {

}
