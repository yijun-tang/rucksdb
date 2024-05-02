//! Slice is a simple structure containing a pointer into some external
//! storage and a size.  The user of a Slice must ensure that the slice
//! is not used after the corresponding external storage has been
//! deallocated.
//! 
//! Multiple threads can invoke const methods on a Slice without
//! external synchronization, but if any of the threads may call a
//! non-const method, all threads accessing the same Slice must use
//! external synchronization.

pub struct Slice {
}

impl Slice {
    pub fn new() -> Self {
        Self {}
    }
}
