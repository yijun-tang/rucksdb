use std::rc::Rc;

use crate::{env::WritableFile, slice::Slice, status::Status};

use super::log_format::{RecordType, MAX_RECORD_TYPE};

pub(crate) struct Writer {
    dest_: Rc<dyn WritableFile>,
    block_offset_: i32, // Current offset in block
    
    // crc32c values for all supported record types.  These are
    // pre-computed to reduce the overhead of computing the crc of the
    // record type stored in the header.
    type_crc_: [u32; MAX_RECORD_TYPE as usize + 1],
}

impl Writer {
    /// Create a writer that will append data to "*dest".
    /// "*dest" must be initially empty.
    /// "*dest" must remain live while this Writer is in use.
    pub(crate) fn new(dest: Rc<dyn WritableFile>) -> Self {
        Self {
            dest_: dest,
            block_offset_: 0,
            type_crc_: Self::init_type_crc(),
        }
    }

    /// Create a writer that will append data to "*dest".
    /// "*dest" must have initial length "dest_length".
    /// "*dest" must remain live while this Writer is in use.
    pub(crate) fn new2(dest: Rc<dyn WritableFile>, dest_length: u64) -> Self {
        todo!()
    }

    pub(crate) fn add_record(&self, slice: &Slice) -> Status {
        todo!()
    }

    fn emit_physical_record(&self, t: RecordType) -> Status {
        todo!()
    }

    fn init_type_crc() -> [u32; MAX_RECORD_TYPE as usize + 1] {
        let res = [0u32; MAX_RECORD_TYPE as usize + 1];
        for i in 0..res.len() {
            
        }
        todo!()
    }
}
