//! Slice is a simple structure containing a pointer into some external
//! storage and a size.  The user of a Slice must ensure that the slice
//! is not used after the corresponding external storage has been
//! deallocated.
//! 
//! Multiple threads can invoke const methods on a Slice without
//! external synchronization, but if any of the threads may call a
//! non-const method, all threads accessing the same Slice must use
//! external synchronization.

#[derive(Clone)]
pub struct Slice<'a> {
    data_: &'a [u8],
    start_: usize,
    end_: usize,
}

impl<'a> Slice<'a> {
    pub fn new(s: &'a [u8]) -> Self {
        Self::new_with_range(s, 0, s.len())
    }

    pub fn new_with_range(s: &'a [u8], start: usize, end: usize) -> Self {
        Self { data_: s, start_: start, end_: end }
    }

    /// Return the length (in bytes) of the referenced data
    pub fn size(&self) -> usize {
        if self.start_ >= self.end_ {
            0
        } else {
            self.end_ - self.start_
        }
    }

    /// Return a pointer to the beginning of the referenced data
    pub fn data(&self) -> &[u8] {
        &self.data_[self.start_..self.end_]
    }

    pub fn advance(&mut self, n: usize) -> Self {
        let mut clone = self.clone();
        self.start_ += n;
        clone.end_ = self.start_;
        clone
    }
}

impl<'a> PartialEq<&[u8]> for Slice<'a> {
    fn eq(&self, other: &&[u8]) -> bool {
        if self.size() != other.len() {
            return false;
        }
        for i in self.start_..self.end_ {
            if self.data_[i] != other[i - self.start_] {
                return false;
            }
        }
        true
    } 
}
