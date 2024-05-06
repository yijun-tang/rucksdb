//! A Status encapsulates the result of an operation.  It may indicate success,
//! or it may indicate an error with an associated error message.
//! 
//! Multiple threads can invoke const methods on a Status without
//! external synchronization, but if any of the threads may call a
//! non-const method, all threads accessing the same Status must use
//! external synchronization.

#[derive(Debug)]
pub struct Status {
    // OK status has a None state_.  Otherwise, state_ is a byte vector
    // of the following form:
    //    state_[0..3] == length of message
    //    state_[4]    == code
    //    state_[5..]  == message
    state_: Option<Vec<u8>>,
}

impl Status {
    /// Return a success status.
    pub fn new_ok() -> Self {
        Self { state_: None }
    }

    // Return error status of an appropriate type.
    pub fn not_found(msg: &str, msg2: &str) -> Self {
        Self::new(Code::not_found(), msg, msg2)
    }
    pub fn corruption(msg: &str, msg2: &str) -> Self {
        Self::new(Code::corruption(), msg, msg2)
    }
    pub fn not_supported(msg: &str, msg2: &str) -> Self {
        Self::new(Code::not_supported(), msg, msg2)
    }
    pub fn invalid_argument(msg: &str, msg2: &str) -> Self {
        Self::new(Code::invalid_argument(), msg, msg2)
    }
    pub fn io_error(msg: &str, msg2: &str) -> Self {
        Self::new(Code::io_error(), msg, msg2)
    }

    /// Returns true iff the status indicates success.
    pub fn ok(&self) -> bool {
        self.state_.is_none()
    }

    /// Returns true iff the status indicates a NotFound error.
    pub fn is_not_found(&self) -> bool {
        self.code().is_not_found()
    }

    /// Returns true iff the status indicates a Corruption error.
    pub fn is_corruption(&self) -> bool {
        self.code().is_corruption()
    }

    /// Returns true iff the status indicates an IOError.
    pub fn is_io_error(&self) -> bool {
        self.code().is_io_error()
    }

    fn new(code: Code, msg: &str, msg2: &str) -> Self {
        todo!()
    }

    fn code(&self) -> Code {
        match self.state_.as_ref() {
            Some(s) => {
                assert!(s.len() >= 5);  // TODO
                Code::from(s[4])
            },
            None => Code::ok(),
        }
    }
}

impl ToString for Status {
    /// Return a string representation of this status suitable for printing.
    /// Returns the string "OK" for success.
    fn to_string(&self) -> String {
        todo!()
    }
}

struct Code(u8);
impl Code {
    fn ok() -> Self { Self(0) }
    fn not_found() -> Self { Self(1) }
    fn corruption() -> Self { Self(2) }
    fn not_supported() -> Self { Self(3) }
    fn invalid_argument() -> Self { Self(4) }
    fn io_error() -> Self { Self(5) }
    fn unsupported() -> Self { Self(u8::MAX) }

    fn is_not_found(&self) -> bool { self.0 == 1 }
    fn is_corruption(&self) -> bool { self.0 == 2 }
    fn is_io_error(&self) -> bool { self.0 == 5 }

    fn from(c: u8) -> Self {
        match c {
            0 => Self::ok(),
            1 => Self::not_found(),
            2 => Self::corruption(),
            3 => Self::not_supported(),
            4 => Self::invalid_argument(),
            5 => Self::io_error(),
            _ => Self::unsupported(),
        }
    }
}

pub type Result<T> = std::result::Result<T, String>;
