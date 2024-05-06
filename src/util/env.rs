use std::rc::Rc;

use crate::{env::Env, slice::Slice, status::Status};

pub(crate) fn write_string_to_file_sync(env: Rc<dyn Env>, data: &Slice, fname: &str) -> Status {
    do_write_string_to_file(env, data, fname, true)
}

fn do_write_string_to_file(env: Rc<dyn Env>, data: &Slice, fname: &str, should_sync: bool) -> Status {
    let mut s = Status::new_ok();
    match env.new_writable_file(fname) {
        Ok(file) => {
            s = file.append(data);
            if s.ok() && should_sync {
                s = file.sync();
            }
            if s.ok() {
                s = file.close();
            }
        },
        Err(s) => { return s; },
    }
    if !s.ok() {
        env.remove_file(fname);
    }
    s
}
