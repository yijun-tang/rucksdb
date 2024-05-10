use std::time::{SystemTime, UNIX_EPOCH};

/// Returns the random seed used at the start of the current test run.
pub(crate) fn random_seed() -> u32 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as u32
}
