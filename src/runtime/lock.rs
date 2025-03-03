use crate::runtime::transaction::{Txn, TxnId};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LockKind {
    Read,
    Write, 
    Upgrade,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Lock {
    pub lock_kind: LockKind,
    pub txn: Txn,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LockWorkerInfo {
    pub lock: Lock,
    pub worker_name: String,
} 

#[derive(Debug, Clone, PartialEq)]
pub enum LockType {
    Read(TxnId),
    Write(TxnId),
    Upgrade(TxnId),
}