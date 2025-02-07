use crate::runtime::transaction::Txn;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LockKind {
    Read,
    Write,
    Update,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Lock {
    pub lock_kind: LockKind,
    pub txn: Txn,
}
