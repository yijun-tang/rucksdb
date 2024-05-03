pub mod db;
pub mod status;
pub mod slice;
pub mod options;
pub mod cache;
pub mod comparator;
pub mod env;
pub mod filter_policy;
mod memtable;
mod util;

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
