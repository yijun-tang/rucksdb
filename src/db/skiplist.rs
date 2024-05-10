// Thread safety
// -------------
//
// Writes require external synchronization, most likely a mutex.
// Reads require a guarantee that the SkipList will not be destroyed
// while the read is in progress.  Apart from that, reads progress
// without any internal locking or synchronization.
//
// Invariants:
//
// (1) Allocated nodes are never deleted until the SkipList is
// destroyed.  This is trivially guaranteed by the code since we
// never delete any skip list nodes.
//
// (2) The contents of a Node except for the next/prev pointers are
// immutable after the Node has been linked into the SkipList.
// Only Insert() modifies the list, and it is careful to initialize
// a node and use release-stores to publish the nodes in one or
// more lists.
//
// ... prev vs. next pointer ordering ...

use std::{cell::RefCell, rc::Rc, sync::{Arc, RwLock}};

use crate::util::{arena::Arena, random::Random};

static MAX_HEIGHT: i32 = 12;

pub(crate) struct SkipList<K> {
    arena_: Arena,
    head_: Arc<Node<K>, Arena>,
    
    // Modified only by Insert().  Read racily by readers, but stale
    // values are ok.
    max_height_: RwLock<i32>, // Height of the entire list

    // Read/written only by Insert().
    rnd_: RwLock<Random>,
}

impl<K: PartialOrd + Clone> SkipList<K> {
    pub(crate) fn new_in(key: K, arena: Arena) -> Self {
        let head = Self::new_node(key, MAX_HEIGHT, arena.clone());
        Self {
            arena_: arena,
            head_: head,
            max_height_: RwLock::new(1),
            rnd_: RwLock::new(Random::new(0xdeadbeef)),
        }
    }

    /// Insert key into the list.
    /// REQUIRES: nothing that compares equal to key is currently in the list.
    pub(crate) fn insert(&self, key: &K) {
        let mut prev: Vec<NullableNodePtr<K>, Arena> = Vec::with_capacity_in(MAX_HEIGHT as usize, self.arena_.clone());
        for _ in 0..(MAX_HEIGHT) { prev.push(None); }
        let x = self.find_greater_or_equal(key, &mut prev);

        // Our data structure does not allow duplicate insertion
        debug_assert!(x.is_none() || x.unwrap().key != *key);

        let height = self.random_height();
        if height > self.get_max_height() {
            for i in self.get_max_height()..height {
                prev[i as usize] = Some(self.head_.clone());
            }
            // It is ok to mutate max_height_ without any synchronization
            // with concurrent readers.  A concurrent reader that observes
            // the new value of max_height_ will see either the old value of
            // new level pointers from head_ (nullptr), or a new value set in
            // the loop below.  In the former case the reader will
            // immediately drop to the next level since nullptr sorts after all
            // keys.  In the latter case the reader will use the new node.
            *self.max_height_.write().unwrap() = height;
        }

        let x = Self::new_node(key.clone(), height, self.arena_.clone());
        for i in 0..(height as usize) {
            let p = prev[i].clone().unwrap();
            x.set_next(i, p.next(i));
            p.set_next(i, Some(x.clone()));
        }
    }

    /// Returns true iff an entry that compares equal to key is in the list.
    pub(crate) fn contains(&self, key: &K) -> bool {
        let mut prev: Vec<NullableNodePtr<K>, Arena> = Vec::with_capacity_in(MAX_HEIGHT as usize, self.arena_.clone());
        for _ in 0..(MAX_HEIGHT) { prev.push(None); }
        let x = self.find_greater_or_equal(key, &mut prev);
        if let Some(n) = x {
            n.key == *key
        } else {
            false
        }
    }

    pub(crate) fn iter(list: Arc<SkipList<K>, Arena>) -> Iter<K> {
        Iter::new(list)
    }

    fn new_node(key: K, height: i32, alloc: Arena) -> Arc<Node<K>, Arena> {
        let mut next_: Vec<NullableNodePtr<K>, Arena> = Vec::with_capacity_in(height as usize, alloc.clone());
        for _ in 0..(height as usize) {
            next_.push(None);
        }
        Arc::new_in(Node { key, next_: RwLock::new(next_) }, alloc)
    }

    /// Return the earliest node that comes at or after key.
    /// Return nullptr if there is no such node.
    /// 
    /// If prev is non-null, fills prev[level] with pointer to previous
    /// node at "level" for every level in [0..max_height_-1].
    fn find_greater_or_equal(&self, key: &K, prev: &mut Vec<NullableNodePtr<K>, Arena>) -> NullableNodePtr<K> {
        let mut x = self.head_.clone();
        let mut level = self.get_max_height() as usize - 1;
        loop {
            let next = x.next(level);
            if self.key_is_after_node(key, next.clone()) {
                // Keep searching in this list
                x = next.unwrap();
            } else {
                prev[level] = Some(x.clone());
                if level == 0 {
                    return next;
                } else {
                    // Switch to next list
                    level -= 1;
                }
            }
        }
    }

    fn get_max_height(&self) -> i32 {
        *self.max_height_.read().unwrap()
    }

    /// Return true if key is greater than the data stored in "n"
    fn key_is_after_node(&self, key: &K, n: NullableNodePtr<K>) -> bool {
        // None n is considered infinite
        if let Some(node) = n {
            return node.key < *key;
        }
        false
    }

    fn random_height(&self) -> i32 {
        // Increase height with probability 1 in kBranching
        static BRANCHING: i32 = 4;
        let mut height = 1;
        while height < MAX_HEIGHT && self.rnd_.write().unwrap().one_in(BRANCHING) {
            height += 1;
        }
        height
    }

    /// Return the last node in the list.
    /// Return head_ if list is empty.
    fn find_last(&self) -> NullableNodePtr<K> {
        let mut x = self.head_.clone();
        let mut level = self.get_max_height() as usize - 1;
        loop {
            let next = x.next(level);
            if let Some(n) = next {
                x = n;
            } else {
                if level == 0 {
                    return Some(x);
                } else {
                    level -= 1;
                }
            }
        }
    }

    /// Return the latest node with a key < key.
    /// Return head_ if there is no such node.
    fn find_less_than(&self, key: &K) -> NullableNodePtr<K> {
        let mut x = self.head_.clone();
        let mut level = self.get_max_height() as usize - 1;
        loop {
            let next = x.next(level);
            if let Some(n) = next {
                if n.key < *key {
                    x = n;
                    continue;
                }
            }
            if level == 0 {
                return Some(x);
            } else {
                level -= 1;
            }
        }
    }
}

/// Iteration over the contents of a skip list
pub(crate) struct Iter<K> {
    list_: Arc<SkipList<K>, Arena>,
    node_: NullableNodePtr<K>,
}

impl<K: PartialOrd + Clone> Iter<K> {
    /// Initialize an iterator over the specified list.
    /// The returned iterator is not valid.
    pub(crate) fn new(list: Arc<SkipList<K>, Arena>) -> Self {
        Self { list_: list, node_: None }
    }

    /// Returns true iff the iterator is positioned at a valid node.
    pub(crate) fn valid(&self) -> bool {
        self.node_.is_some()
    }

    /// Position at the first entry in list.
    /// Final state of iterator is Valid() iff list is not empty.
    pub(crate) fn seek_to_first(&mut self) {
        self.node_ = self.list_.head_.next(0);
    }

    /// Position at the last entry in list.
    /// Final state of iterator is Valid() iff list is not empty.
    pub(crate) fn seek_to_last(&mut self) {
        if let Some(l) = self.list_.find_last() {
            if Arc::ptr_eq(&l, &self.list_.head_) {
                self.node_ = None;
            } else {
                self.node_ = Some(l);
            }
        }
    }

    /// Advance to the first entry with a key >= target
    pub(crate) fn seek(&mut self, target: &K) {
        let mut prev: Vec<NullableNodePtr<K>, Arena> = Vec::with_capacity_in(MAX_HEIGHT as usize, self.list_.arena_.clone());
        for _ in 0..(MAX_HEIGHT) { prev.push(None); }
        self.node_ = self.list_.find_greater_or_equal(target, &mut prev);
    }

    /// Returns the key at the current position.
    /// REQUIRES: Valid()
    pub(crate) fn key(&self) -> K {
        self.node_.clone().expect("require non-null").key.clone()
    }

    /// Advances to the next position.
    /// REQUIRES: Valid()
    pub(crate) fn next(&mut self) {
        self.node_ = self.node_.clone().unwrap().next(0);
    }

    /// Advances to the previous position.
    /// REQUIRES: Valid()
    pub(crate) fn prev(&mut self) {
        // Instead of using explicit "prev" links, we just search for the
        // last node that falls before key.
        if let Some(p) = self.list_.find_less_than(&self.node_.clone().unwrap().key) {
            if Arc::ptr_eq(&p, &self.list_.head_) {
                self.node_ = None;
            } else {
                self.node_ = Some(p);
            }
        }
    }
}

type NullableNodePtr<K> = Option<Arc<Node<K>, Arena>>;
struct Node<K> {
    key: K,
    // Array of length equal to the node height.  next_[0] is lowest level link.
    next_: RwLock<Vec<NullableNodePtr<K>, Arena>>,
}

impl<K> Node<K> {
    /// Accessors/mutators for links.  Wrapped in methods so we can
    /// add the appropriate barriers as necessary.
    fn next(&self, n: usize) -> NullableNodePtr<K> {
        // Use an 'acquire load' so that we observe a fully initialized
        // version of the returned Node.
        self.next_.read().unwrap()[n].clone()
    }

    fn set_next(&self, n: usize, x: NullableNodePtr<K>) {
        self.next_.write().unwrap()[n] = x;
    }
}

#[cfg(test)]
mod tests {
    use std::{borrow::Borrow, collections::BTreeSet, io::Read, os::macos::raw::stat, sync::{atomic::{AtomicBool, AtomicI32, Ordering}, Arc, Condvar, Mutex}, thread};

    use crate::util::{hash::hash, testutil::random_seed};

    use super::*;

    #[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Clone, Copy)]
    struct Key(u64);

    #[test]
    fn empty_test() {
        let arena = Arena::new();
        let list: Arc<SkipList<Key>, Arena> = Arc::new_in(SkipList::new_in(Key(0), arena.clone()), arena);
        assert!(!list.contains(&Key(10)));

        let mut iter = Iter::new(list.clone());
        assert!(!iter.valid());
        iter.seek_to_first();
        assert!(!iter.valid());
        iter.seek(&Key(100));
        assert!(!iter.valid());
        iter.seek_to_last();
        assert!(!iter.valid());
    }

    #[test]
    fn insert_and_lookup_test() {
        let N: usize = 2000;
        let R = 5000;
        let mut rnd = Random::new(1000);
        let mut keys: BTreeSet<Key> = BTreeSet::new();
        let arena = Arena::new();
        let list: Arc<SkipList<Key>, Arena> = Arc::new_in(SkipList::new_in(Key(0), arena.clone()), arena);

        for _ in 0..N {
            let key = Key((rnd.next() % R) as u64);
            if keys.insert(key) {
                list.insert(&key);
            }
        }

        for i in 0..R {
            let key = Key(i as u64);
            if list.contains(&key) {
                assert!(keys.contains(&key));
            } else {
                assert!(!keys.contains(&key));
            }
        }

        // Simple iterator tests
        {
            let mut iter = Iter::new(list.clone());
            assert!(!iter.valid());

            iter.seek(&Key(0));
            assert!(iter.valid());
            assert_eq!(keys.first().unwrap(), &iter.key());

            iter.seek_to_first();
            assert!(iter.valid());
            assert_eq!(keys.first().unwrap(), &iter.key());

            iter.seek_to_last();
            assert!(iter.valid());
            assert_eq!(keys.last().unwrap(), &iter.key());
        }

        // Forward iteration test
        for i in 0..R {
            let mut iter = Iter::new(list.clone());
            let key = Key(i as u64);
            iter.seek(&key);

            let mut k_iter = keys.iter().skip_while(|e| *e < &key);
            for _ in 0..3usize {
                if let Some(k) = k_iter.next() {
                    assert!(iter.valid());
                    assert_eq!(k, &iter.key());
                    iter.next();
                } else {
                    assert!(!iter.valid());
                    break;
                }
            }
        }

        // Backward iteration test
        {
            let mut iter = Iter::new(list.clone());
            iter.seek_to_last();

            let mut k_iter = keys.iter().rev();
            while let Some(k) = k_iter.next() {
                assert!(iter.valid());
                assert_eq!(k, &iter.key());
                iter.prev();
            }
            assert!(!iter.valid());
        }
    }

    // We want to make sure that with a single writer and multiple
    // concurrent readers (with no synchronization other than when a
    // reader's iterator is created), the reader always observes all the
    // data that was present in the skip list when the iterator was
    // constructed.  Because insertions are happening concurrently, we may
    // also observe new values that were inserted since the iterator was
    // constructed, but we should never miss any values that were present
    // at iterator construction time.
    //
    // We generate multi-part keys:
    //     <key,gen,hash>
    // where:
    //     key is in range [0..K-1]
    //     gen is a generation number for key
    //     hash is hash(key,gen)
    //
    // The insertion code picks a random key, sets gen to be 1 + the last
    // generation number inserted for that key, and sets hash to Hash(key,gen).
    //
    // At the beginning of a read, we snapshot the last inserted
    // generation number for each key.  We then iterate, including random
    // calls to Next() and Seek().  For every key we encounter, we
    // check that it is either expected given the initial snapshot or has
    // been concurrently added since the iterator started.
    const K: u32 = 4;
    struct ConcurrentTest {
        // Current state of the test
        current_: State,
        // SkipList is not protected by mu_.  We just use a single writer
        // thread to modify it.
        list_: Arc<SkipList<Key>, Arena>,
    }
    impl ConcurrentTest {
        fn new() -> Self {
            let arena = Arena::new();
            Self {
                current_: State::new(),
                list_: Arc::new_in(SkipList::new_in(Key(0), arena.clone()), arena),
            }
        }
        // REQUIRES: External synchronization
        fn write_step(&self, rnd: &mut Random) {
            let k = rnd.next() % K;
            let g = self.current_.get(k as usize) + 1;
            let key = Self::make_key(k as u64, g as u64);
            self.list_.insert(&key);
            self.current_.set(k as usize, g);
        }
        fn read_step(&self, rnd: &mut Random) {
            // Remember the initial committed state of the skiplist.
            let initial_state = State::new();
            for k in 0..(K as usize) {
                initial_state.set(k, self.current_.get(k));
            }

            let mut pos = Self::randome_target(rnd);
            let mut iter = Iter::new(self.list_.clone());
            iter.seek(&pos);
            loop {
                let mut current = Key(0);
                if !iter.valid() {
                    current = Self::make_key(K as u64, 0);
                } else {
                    current = iter.key();
                    assert!(Self::is_valid_key(&current));
                }
                assert!(pos <= current);

                // Verify that everything in [pos,current) was not present in
                // initial_state.
                while pos < current {
                    assert!(Self::key(&pos) < K as u64);

                    // Note that generation 0 is never inserted, so it is ok if
                    // <*,0,*> is missing.
                    assert!(Self::gen(&pos) == 0 ||
                            (Self::gen(&pos) > initial_state.get(Self::key(&pos) as usize) as u64));
                    
                    // Advance to next key in the valid key space
                    if Self::key(&pos) < Self::key(&current) {
                        pos = Self::make_key(Self::key(&pos) + 1, 0);
                    } else {
                        pos = Self::make_key(Self::key(&pos), Self::gen(&pos) + 1);
                    }
                }

                if !iter.valid() {
                    break;
                }

                if (rnd.next() % 2) == 1 {
                    iter.next();
                    pos = Self::make_key(Self::key(&pos), Self::gen(&pos) + 1);
                } else {
                    let new_target = Self::randome_target(rnd);
                    if new_target > pos {
                        pos = new_target;
                        iter.seek(&new_target);
                    }
                }
            }
        }
        fn key(key: &Key) -> u64 { key.0 >> 40 }
        fn gen(key: &Key) -> u64 { (key.0 >> 8) & 0xffff_ffffu64 }
        fn hash(key: &Key) -> u64 { key.0 & 0xff }
        fn is_valid_key(k: &Key) -> bool {
            Self::hash(k) == (Self::hash_number(Self::key(k), Self::gen(k)) & 0xff)
        }
        fn randome_target(rnd: &mut Random) -> Key {
            match rnd.next() % 10 {
                0 => {
                    // Seek to beginning
                    Self::make_key(0, 0)
                },
                1 => {
                    // Seek to end
                    Self::make_key(K as u64, 0)
                },
                _ => {
                    // Seek to middle
                    Self::make_key((rnd.next() % K) as u64, 0)
                },
            }
        }
        fn make_key(k: u64, g: u64) -> Key {
            Key((k << 40) | (g << 8) | (Self::hash_number(k, g) & 0xff))
        }
        fn hash_number(k: u64, g: u64) -> u64 {
            hash([k.to_ne_bytes(), g.to_ne_bytes()].flatten(), 0) as u64
        }
    }
    // Per-key generation
    struct State {
        generation: [AtomicI32; K as usize],
    }
    impl State {
        fn new() -> Self {
            Self { generation: [AtomicI32::new(0), AtomicI32::new(0), AtomicI32::new(0), AtomicI32::new(0)] }
        }
        fn set(&self, k: usize, v: i32) {
            self.generation[k].store(v, Ordering::Release);
        }
        fn get(&self, k: usize) -> i32 {
            self.generation[k].load(Ordering::Acquire)
        }
    }

    #[test]
    fn concurrent_without_threads_test() {
        let t = ConcurrentTest::new();
        let mut rnd = Random::new(random_seed());
        for _ in 0..10_000usize {
            t.read_step(&mut rnd);
            t.write_step(&mut rnd);
        }
    }

    struct TestState {
        t_: ConcurrentTest,
        seed_: u32,
        quit_flag_: AtomicBool,
        mu_: Mutex<ReaderState>,
        state_cv_: Condvar,
    }
    impl TestState {
        fn new(s: u32) -> Self {
            Self {
                t_: ConcurrentTest::new(),
                seed_: s,
                quit_flag_: AtomicBool::new(false),
                mu_: Mutex::new(ReaderState::Starting),
                state_cv_: Condvar::new(),
            }
        }
        fn wait(&self, s: ReaderState) {
            let mut state = self.mu_.lock().unwrap();
            while *state != s {
                state = self.state_cv_.wait(state).unwrap();
            }
        }
        fn change(&self, s: ReaderState) {
            let mut state = self.mu_.lock().unwrap();
            *state = s;
            self.state_cv_.notify_one();
        }
    }
    #[derive(PartialEq)]
    enum ReaderState {
        Starting,
        Running,
        Done,
    }

    fn concurrent_reader(arg: Arc<TestState>) {
        let mut rnd = Random::new(arg.seed_);
        let mut reads = 0i64;
        arg.change(ReaderState::Running);
        while !arg.quit_flag_.load(Ordering::Acquire) {
            arg.t_.read_step(&mut rnd);
            reads += 1;
        }
        arg.change(ReaderState::Done);
    }

    fn run_concurrent(run: i32) {
        let seed = random_seed() + (run * 100) as u32;
        let mut rnd = Random::new(seed);
        let N = 1000;
        let size = 1000;
        for i in 0..N {
            if i % 100 == 0 {
                println!("Run {} of {}\n", i, N);
            }
            let state = Arc::new(TestState::new(seed + 1));
            let arg = state.clone();
            thread::spawn(move || { concurrent_reader(arg); });
            state.wait(ReaderState::Running);
            for _ in 0..size {
                state.t_.write_step(&mut rnd);
            }
            state.quit_flag_.store(true, Ordering::Release);
            state.wait(ReaderState::Done);
        }
    }

    #[test]
    fn concurrent1_test() {
        run_concurrent(1);
    }

    #[test]
    fn concurrent2_test() {
        run_concurrent(2);
    }

    #[test]
    fn concurrent3_test() {
        run_concurrent(3);
    }

    #[test]
    fn concurrent4_test() {
        run_concurrent(4);
    }

    #[test]
    fn concurrent5_test() {
        run_concurrent(5);
    }
}
