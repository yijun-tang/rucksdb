pub(crate) fn descriptor_file_name(dbname: &str, number: u64) -> String {
    assert!(number > 0);
    format!("{}/MANIFEST-{:06}", dbname, number)
}

pub(crate) fn current_file_name(dbname: &str) -> String {
    format!("{}/CURRENT", dbname)
}

pub(crate) fn lock_file_name(dbname: &str) -> String {
    format!("{}/LOCK", dbname)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptor_file_name_test() {
        assert_eq!(descriptor_file_name("test", 111), "test/MANIFEST-000111");
        assert_eq!(descriptor_file_name("test", 1111111), "test/MANIFEST-1111111");
    }
}
