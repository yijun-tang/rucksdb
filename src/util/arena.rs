use std::{alloc::{Allocator, Global}, rc::Rc, sync::{atomic::{AtomicUsize, Ordering}, Arc}};

#[derive(Clone)]
pub(crate) struct Arena {
    global_: Arc<Global>,
    allocated_: Arc<AtomicUsize>,
}

impl Arena {
    pub(crate) fn new() -> Self {
        Self { global_: Arc::new(Global), allocated_: Arc::new(AtomicUsize::new(0)) }
    }

    /// Returns an estimate of the total memory usage of data allocated
    /// by the arena.
    pub(crate) fn memory_usage(&self) -> usize {
        self.allocated_.load(Ordering::Relaxed)
    }
}

unsafe impl Allocator for Arena {
    fn allocate(&self, layout: std::alloc::Layout) -> Result<std::ptr::NonNull<[u8]>, std::alloc::AllocError> {
        let ret = self.global_.allocate(layout)?;
        self.allocated_.fetch_add(layout.size(), Ordering::Relaxed);
        Ok(ret)
    }

    unsafe fn deallocate(&self, ptr: std::ptr::NonNull<u8>, layout: std::alloc::Layout) {
        self.global_.deallocate(ptr, layout);
        self.allocated_.fetch_sub(layout.size(), Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use std::mem::size_of_val;

    use super::*;

    #[test]
    fn empty_test() {
        let _ = Arena::new();
    }

    #[test]
    fn boxed_test() {
        let arena = Arena::new();
        {
            let boxed = Box::new_in(0u8, arena.clone());
            assert_eq!(0, *boxed);
            assert_eq!(1, arena.memory_usage());
        }
        assert_eq!(0, arena.memory_usage());
        
        {
            let _1 = Box::new_in(0u8, arena.clone());
            let _2 = Box::new_in(0u8, arena.clone());
            let _3 = Box::new_in(0u8, arena.clone());
            let _4 = Box::new_in(0u8, arena.clone());
            assert_eq!(4, arena.memory_usage());
        }
        assert_eq!(0, arena.memory_usage());
    }
}
