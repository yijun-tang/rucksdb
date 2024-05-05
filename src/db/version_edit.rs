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

    pub(crate) fn encode_to(&self) {
        if self.has_comparator_ {
            
        }
    }

    pub(crate) fn set_comparator_name(&mut self, name: &str) {
        self.has_comparator_ = true;
        self.comparator_ = name.to_string();
    }

    pub(crate) fn set_log_number(&mut self, num: u64) {
        self.has_log_number_ = true;
        self.log_number_ = num;
    }

    pub(crate) fn set_prev_log_number(&mut self, num: u64) {
        self.has_prev_log_number_ = true;
        self.prev_log_number_ = num;
    }

    pub(crate) fn set_next_file(&mut self, num: u64) {
        self.has_next_file_number_ = true;
        self.next_file_number_ = num;
    }

    pub(crate) fn set_last_sequence(&mut self, seq: SequenceNumber) {
        self.has_last_sequence_ = true;
        self.last_sequence_ = seq;
    }

    pub(crate) fn set_compact_pointer(&mut self, level: i32, key: InternalKey) {
        self.compact_pointers_.push((level, key));
    }
}

pub(crate) struct FileMetaData {

}
