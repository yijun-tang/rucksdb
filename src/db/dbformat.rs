

/// Modules in this directory should keep internal keys wrapped inside
/// the following class instead of plain strings so that we do not
/// incorrectly use string comparisons instead of an InternalKeyComparator.
pub(crate) struct InternalKey {
    rep_: String,
}

impl InternalKey {
    pub(crate) fn new() -> Self {
        // Leave rep_ as empty to indicate it is invalid
        Self { rep_: String::new() }
    }
}
