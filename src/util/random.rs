//! A very simple random number generator.  Not especially good at
//! generating truly random bits, but good enough for our needs in this
//! package.

static M: u32 = (1u32 << 31) - 1;   // 2^31-1
static A: u64 = 16807;              // bits 14, 8, 7, 5, 2, 1, 0

pub(crate) struct Random {
    seed_: u32,
}

impl Random {
    pub(crate) fn new(s: u32) -> Self {
        let mut seed_ = s & 0x7fff_ffff;    // why?
        // Avoid bad seeds.
        if seed_ == 0 || seed_ == ((1u32 << 31) - 1) {
            seed_ = 1;
        }
        Self { seed_ }
    }

    // TODO: needs to investigate the underlying mathematical formula.
    pub(crate) fn next(&mut self) -> u32 {
        // We are computing
        //       seed_ = (seed_ * A) % M,    where M = 2^31-1
        //
        // seed_ must not be zero or M, or else all subsequent computed values
        // will be zero or M respectively.  For all other values, seed_ will end
        // up cycling through every number in [1,M-1]
        let product = self.seed_ as u64 * A;

        // Compute (product % M) using the fact that ((x << 31) % M) == x.
        // 
        // ((x << 31) % M) = (x * 2^31) % M = (x * (2^31 - 1) + x) % M = (x * M + x) % M = x.
        self.seed_ = ((product >> 31) + (product & (M as u64))) as u32;
        // The first reduction may overflow by 1 bit, so we may need to
        // repeat.  mod == M is not possible; using > allows the faster
        // sign-bit-based test.
        if self.seed_ > M {
            self.seed_ -= M;
        }
        self.seed_
    }

    /// Returns a uniformly distributed value in the range [0..n-1]
    /// REQUIRES: n > 0
    pub(crate) fn uniform(&mut self, n: i32) -> u32 {
        debug_assert!(n > 0);
        self.next() % (n as u32)
    }

    /// Randomly returns true ~"1/n" of the time, and false otherwise.
    /// REQUIRES: n > 0
    pub(crate) fn one_in(&mut self, n: i32) -> bool {
        debug_assert!(n > 0);
        (self.next() % (n as u32)) == 0
    }

    /// Skewed: pick "base" uniformly from range [0,max_log] and then
    /// return "base" random bits.  The effect is to pick a number in the
    /// range [0,2^max_log-1] with exponential bias towards smaller numbers.
    pub(crate) fn skewed(&mut self, max_log: i32) -> u32 {
        let base = self.uniform(max_log + 1);
        self.uniform(1i32 << base)
    }
}
