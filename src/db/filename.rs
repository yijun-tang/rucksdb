use std::rc::Rc;

use crate::{env::Env, slice::Slice, status::Status, util::env::write_string_to_file_sync};

fn make_file_name(dbname: &str, number: u64, suffix: &str) -> String {
    format!("{}/{:06}.{}", dbname, number, suffix)
}

pub(crate) fn descriptor_file_name(dbname: &str, number: u64) -> String {
    debug_assert!(number > 0);
    format!("{}/MANIFEST-{:06}", dbname, number)
}

pub(crate) fn current_file_name(dbname: &str) -> String {
    format!("{}/CURRENT", dbname)
}

pub(crate) fn lock_file_name(dbname: &str) -> String {
    format!("{}/LOCK", dbname)
}

pub(crate) fn temp_file_name(dbname: &str, number: u64) -> String {
    debug_assert!(number > 0);
    make_file_name(dbname, number, "dbtmp")
}

pub(crate) fn set_current_file(env: Rc<dyn Env>, dbname: &str, descriptor_number: u64) -> Status {
    // Remove leading "dbname/" and add newline to manifest file name
    let manifest = descriptor_file_name(dbname, descriptor_number);
    let prefix = format!("{}/", dbname);
    debug_assert!(manifest.starts_with(&prefix));
    let contents = format!("{}\n", manifest.strip_prefix(&prefix).unwrap());
    let tmp = temp_file_name(dbname, descriptor_number);
    let mut s = write_string_to_file_sync(env.clone(), &Slice::new(contents.as_bytes()), &tmp);
    if s.ok() {
        s = env.rename_file(&tmp, &current_file_name(dbname));
    }
    if !s.ok() {
        env.remove_file(&tmp);
    }
    s
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
