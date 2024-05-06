use std::collections::BTreeSet;

use crate::{db::dbformat::NUM_LEVELS, slice::Slice, status::Status, util::coding::{get_length_prefixed_slice, get_varint32, get_varint64, put_length_prefixed_slice, put_varint32, put_varint64}};

use super::dbformat::InternalKey;

// Tag numbers for serialized VersionEdit.  These numbers are written to
// disk and should not be changed.
const COMPARATOR: u8 = 1;
const LOG_NUMBER: u8 = 2;
const NEXT_FILE_NUMBER: u8 = 3;
const LAST_SEQUENCE: u8 = 4;
const COMPACT_POINTER: u8 = 5;
const DELETED_FILE: u8 = 6;
const NEW_FILE: u8 = 7;
// 8 was used for large value refs
const PREV_LOG_NUMBER: u8 = 9;

pub(crate) type SequenceNumber = u64;
type DeletedFileSet = BTreeSet<(i32, u64)>;

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
            deleted_files_: BTreeSet::new(),
            new_files_: Vec::new(),
        }
    }

    pub(crate) fn encode_to(&self, dst:&mut Vec<u8>) {
        if self.has_comparator_ {
            put_varint32(dst, COMPARATOR as u32);
            put_length_prefixed_slice(dst, &Slice::new(self.comparator_.as_bytes()));
        }
        if self.has_log_number_ {
            put_varint32(dst, LOG_NUMBER as u32);
            put_varint64(dst, self.log_number_);
        }
        if self.has_prev_log_number_ {
            put_varint32(dst, PREV_LOG_NUMBER as u32);
            put_varint64(dst, self.prev_log_number_);
        }
        if self.has_next_file_number_ {
            put_varint32(dst, NEXT_FILE_NUMBER as u32);
            put_varint64(dst, self.next_file_number_);
        }
        if self.has_last_sequence_ {
            put_varint32(dst, LAST_SEQUENCE as u32);
            put_varint64(dst, self.last_sequence_);
        }

        for pointer in &self.compact_pointers_ {
            put_varint32(dst, COMPACT_POINTER as u32);
            put_varint32(dst, pointer.0 as u32);    // level
            put_length_prefixed_slice(dst, &pointer.1.encode());
        }
        for file in &self.deleted_files_ {
            put_varint32(dst, DELETED_FILE as u32);
            put_varint32(dst, file.0 as u32);   // level
            put_varint64(dst, file.1);          // file number
        }
        for file in &self.new_files_ {
            let meta = &file.1;
            put_varint32(dst, NEW_FILE as u32);
            put_varint32(dst, file.0 as u32);   // level
            put_varint64(dst, meta.number);
            put_varint64(dst, meta.file_size);
            put_length_prefixed_slice(dst, &meta.smallest.encode());
            put_length_prefixed_slice(dst, &meta.largest.encode());
        }
    }

    pub(crate) fn decode_from(src: &Slice) -> Result<Self, Status> {
        let mut msg = String::new();
        let mut input = src.clone();

        let mut result = Self::new();
        while msg.is_empty() {
            match get_varint32(&mut input) {
                Some(tag) => {
                    match tag as u8 {
                        COMPARATOR => {
                            match get_length_prefixed_slice(&mut input) {
                                Some(s) => {
                                    if let Some(ss) = s.to_utf8_string() {
                                        result.comparator_ = ss;
                                        result.has_comparator_ = true;
                                    } else {
                                        msg = "comparator name".to_string();
                                    }
                                },
                                None => { msg = "comparator name".to_string(); },
                            }
                        },
                        LOG_NUMBER => {
                            match get_varint64(&mut input) {
                                Some(n) => {
                                    result.log_number_ = n;
                                    result.has_log_number_ = true;
                                },
                                None => { msg = "log number".to_string(); },
                            }
                        },
                        PREV_LOG_NUMBER => {
                            match get_varint64(&mut input) {
                                Some(n) => {
                                    result.prev_log_number_ = n;
                                    result.has_prev_log_number_ = true;
                                },
                                None => { msg = "previous log number".to_string(); },
                            }
                        },
                        NEXT_FILE_NUMBER => {
                            match get_varint64(&mut input) {
                                Some(n) => {
                                    result.next_file_number_ = n;
                                    result.has_next_file_number_ = true;
                                },
                                None => { msg = "next file number".to_string(); },
                            }
                        },
                        LAST_SEQUENCE => {
                            match get_varint64(&mut input) {
                                Some(n) => {
                                    result.last_sequence_ = n;
                                    result.has_last_sequence_ = true;
                                },
                                None => { msg = "last sequence number".to_string(); },
                            }
                        },
                        COMPACT_POINTER => {
                            match (get_level(&mut input), get_internal_key(&mut input)) {
                                (Some(l), Some(k)) => {
                                    result.compact_pointers_.push((l, k));
                                },
                                _ => { msg = "compaction pointer".to_string(); },
                            }
                        },
                        DELETED_FILE => {
                            match (get_level(&mut input), get_varint64(&mut input)) {
                                (Some(l), Some(n)) => {
                                    result.deleted_files_.insert((l, n));
                                },
                                _ => { msg = "deleted file".to_string(); },
                            }
                        },
                        NEW_FILE => {
                            match (get_level(&mut input), get_varint64(&mut input), get_varint64(&mut input),
                                    get_internal_key(&mut input), get_internal_key(&mut input)) {
                                (Some(level), Some(number), Some(file_size), 
                                    Some(smallest), Some(largest)) => {
                                    let mut meta = FileMetaData::new();
                                    meta.number = number;
                                    meta.file_size = file_size;
                                    meta.smallest = smallest;
                                    meta.largest = largest;
                                    result.new_files_.push((level, meta));
                                },
                                _ => { msg = "new-file entry".to_string(); },
                            }
                        },
                        _ => {
                            msg = "unknown tag".to_string();
                        },
                    }
                },
                None => { break; },
            }
        }

        if msg.is_empty() && !input.is_empty() {
            msg = "invalid tag".to_string();
        }

        if msg.is_empty() {
            Ok(result)
        } else {
            Err(Status::corruption("VersionEdit", &msg))
        }
    }

    /// Add the specified file at the specified number.
    /// REQUIRES: This version has not been saved (see VersionSet::SaveTo)
    /// REQUIRES: "smallest" and "largest" are smallest and largest keys in file
    pub(crate) fn add_file(&mut self, level: i32, file: u64, file_size: u64, 
        smallest: &InternalKey, largest: &InternalKey) {
        let mut meta = FileMetaData::new();
        meta.number = file;
        meta.file_size = file_size;
        meta.smallest = smallest.clone();
        meta.largest = largest.clone();
        self.new_files_.push((level, meta));
    }

    /// Delete the specified "file" from the specified "level".
    pub(crate) fn remove_file(&mut self, level: i32, file: u64) {
        self.deleted_files_.insert((level, file));
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

fn get_internal_key(input: &mut Slice) -> Option<InternalKey> {
    let slice = get_length_prefixed_slice(input)?;
    Some(InternalKey::decode_from(&slice))
}

fn get_level(input: &mut Slice) -> Option<i32> {
    let n = get_varint32(input)? as i32;
    if n < NUM_LEVELS {
        Some(n)
    } else {
        None
    }
}

pub(crate) struct FileMetaData {
    refs: i32,
    allowed_seeks: i32, // Seeks allowed until compaction
    number: u64,
    file_size: u64,     // File size in bytes
    smallest: InternalKey, // Smallest internal key served by table
    largest: InternalKey,  // Largest internal key served by table
}

impl FileMetaData {
    pub(crate) fn new() -> Self {
        Self { 
            refs: 0, 
            allowed_seeks: 1i32 << 30, 
            number: 0,  // 0 shouldn't be used, just for initialization
            file_size: 0, 
            smallest: InternalKey::new(), // empty key shouldn't be used either
            largest: InternalKey::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::db::dbformat::ValueType;

    use super::*;
    static BIG: u64 = 1u64 << 50;

    fn test_encode_decode(edit: &VersionEdit) {
        let mut encoded = Vec::new();
        edit.encode_to(&mut encoded);
        let parsed = VersionEdit::decode_from(&Slice::new(&encoded));
        assert!(parsed.is_ok());
        let mut encoded2 = Vec::new();
        parsed.unwrap().encode_to(&mut encoded2);
        assert_eq!(encoded, encoded2);
    }

    #[test]
    fn encode_decode_test() {
        let mut edit = VersionEdit::new();
        for i in 0..4 {
            test_encode_decode(&edit);
            edit.add_file(3, BIG + 300 + i, BIG + 400 + i, 
                &InternalKey::new_from(&Slice::new(b"foo"), BIG + 500 + i, ValueType::type_value()),
                &InternalKey::new_from(&Slice::new(b"zoo"), BIG + 600 + i, ValueType::type_deletion()));
            edit.remove_file(4, BIG + 700 + i);
            edit.set_compact_pointer(i as i32, InternalKey::new_from(&Slice::new(b"x"), BIG + 900 + i, ValueType::type_value()));
        }

        edit.set_comparator_name("foo");
        edit.set_log_number(BIG + 100);
        edit.set_next_file(BIG + 200);
        edit.set_last_sequence(BIG + 1000);
        test_encode_decode(&edit);
    }
}
