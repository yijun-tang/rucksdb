

/// Return the crc32c of concat(A, data[0,n-1]) where init_crc is the
/// crc32c of some string A.  Extend() is often used to maintain the
/// crc32c of a stream of data.
pub(crate) fn extend(init_crc: u32, data: u8, n: usize) -> u32 {
    todo!()
}

/// Return the crc32c of data[0,n-1]
pub(crate) fn value(data: u8, n: usize) -> u32 {
    extend(0, data, n)
}
