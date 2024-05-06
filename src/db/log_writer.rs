use std::rc::Rc;

use crate::{db::log_format::{BLOCK_SIZE, HEADER_SIZE}, env::WritableFile, slice::Slice, status::Status, util::{coding::encode_fixed32, crc32c::{extend, mask, value}}};

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

    pub(crate) fn add_record(&mut self, slice: &Slice) -> Status {
        // Fragment the record if necessary and emit it.  Note that if slice
        // is empty, we still want to iterate once to emit a single
        // zero-length record
        let mut s = Status::new_ok();
        let mut begin = true;
        let mut slice_copy = slice.clone();
        loop {
            let left = slice_copy.size();
            let leftover = BLOCK_SIZE - (self.block_offset_ as usize);
            if leftover < HEADER_SIZE {
                // Switch to a new block
                if leftover > 0 {
                    // Fill the trailer (literal below relies on kHeaderSize being 7)
                    debug_assert!(HEADER_SIZE == 7);
                    self.dest_.append(&Slice::new(&vec![0u8; leftover]));
                }
                self.block_offset_ = 0;
            }

            // Invariant: we never leave < kHeaderSize bytes in a block.
            debug_assert!(BLOCK_SIZE - (self.block_offset_ as usize) >= HEADER_SIZE);

            let avail = BLOCK_SIZE - (self.block_offset_ as usize) - HEADER_SIZE;
            let fragment_length = if left < avail { left } else { avail };

            let mut type_ = RecordType::middle_type();
            let end = left == fragment_length;
            if begin && end {
                type_ = RecordType::full_type();
            } else if begin {
                type_ = RecordType::first_type();
            } else if end {
                type_ = RecordType::last_type();
            }

            s = self.emit_physical_record(type_, &mut slice_copy, fragment_length);
            begin = false;
            if !s.ok() || slice_copy.is_empty() {
                break;
            }
        }
        s
    }

    fn emit_physical_record(&mut self, t: RecordType, slice: &mut Slice, length: usize) -> Status {
        debug_assert!(length <= 0xffff);    // Must fit in two bytes
        debug_assert!((self.block_offset_ as usize) + HEADER_SIZE + length <= BLOCK_SIZE);

        // Format the header
        let mut buf = [0u8; HEADER_SIZE];
        buf[4] = length as u8;
        buf[5] = (length >> 8) as u8;
        buf[6] = t.value();

        // Compute the crc of the record type and the payload.
        let mut crc = extend(self.type_crc_[t.value() as usize], &slice.data()[0..length]);
        crc = mask(crc);    // Adjust for storage
        let crc_encoded = encode_fixed32(crc);
        buf[0] = crc_encoded[0];
        buf[1] = crc_encoded[1];
        buf[2] = crc_encoded[2];
        buf[3] = crc_encoded[3];

        // Write the header and the payload
        let mut s = self.dest_.append(&Slice::new(&buf));
        if s.ok() {
            let payload = slice.advance(length);
            s = self.dest_.append(&payload);
            if s.ok() {
                self.dest_.flush();
            }
        }
        self.block_offset_ += (HEADER_SIZE + length) as i32;
        s
    }

    fn init_type_crc() -> [u32; MAX_RECORD_TYPE as usize + 1] {
        let mut type_crc = [0u32; MAX_RECORD_TYPE as usize + 1];
        for i in 0..type_crc.len() {
            type_crc[i] = value(&[i as u8]);
        }
        type_crc
    }
}
