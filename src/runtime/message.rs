use crate::{
    frontend::meerast::Expr,
    runtime::{lock::LockKind, transaction::Txn},
};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};

use tokio::sync::mpsc::Sender;

pub const BUFFER_SIZE: usize = 1024;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Val {
    Int(i32),
    Bool(bool),
    Action(Expr), // Expr have to be Action
    Lambda(Expr), // Expr have to be Lambda
}

#[derive(Debug, Clone)]
pub enum Message {
    UsrReadVarRequest {
        txn: Txn,
    },
    UsrReadVarResult {
        var_name: String,
        var_version: u64,
        result: Option<Val>,
        result_preds: HashSet<Txn>,
        txn: Txn,
    },
    UsrWriteVarRequest {
        txn: Txn,
        write_val: Val,
    },
    UsrUpdateVarRequest {
        txn: Txn,
        update_val: Val,
    },
    UsrReadDefRequest {
        txn: Txn,
        requires: HashSet<Txn>, // ?? Why we need this
    },
    UsrReadDefResult {
        txn: Txn,
        name: String,
        result: Option<Val>,
        result_pred: HashSet<Txn>,
    },

    DevReadVarRequest {
        txn: Txn,
    },
    DevReadDefRequest {
        txn: Txn,
    },
    DevReadDefResult {
        // grant access to Delta(name)
        name: String,
        txn: Txn,
    },

    DevWriteDefRequest {
        txn: Txn,
        write_expr: Expr,
    },

    VarLockRequest {
        lock_kind: LockKind,
        txn: Txn,
    },
    VarLockRelease {
        txn: Txn,
        requires: HashSet<Txn>,
    },
    VarLockGranted {
        txn: Txn,
        from_name: String,
    },
    VarLockAbort {
        txn: Txn,
    },

    DefLockRequest {
        lock_kind: LockKind,
        txn: Txn,
    },
    DefLockRelease {
        txn: Txn,
    },
    DefLockGranted {
        txn: Txn,
    },
    DefLockAbort {
        txn: Txn,
    },

    Propagate {
        propa_change: PropaChange, // a small change, make batch valid easier
    },
    Subscribe {
        subscribe_who: String,
        subscriber_name: String,
        sender_to_subscriber: Sender<Message>,
    },
    DeSubscribe {
        // cancel subscription
        desubscribe_who: String,
        desubscriber_name: String,
        // sender_to_subscriber: Sender<Message>,
    },
    SubscriptionGranted {
        name: String,
        value: Option<Val>,
        provides: HashSet<Txn>,
        trans_dep_set: HashSet<String>,
    },
    DeSubscriptionGranted {
        name: String,
    },
    DefUpdate {
        txn: Txn,
        update_expr: Expr,
        expr_dependencies: HashMap<String, Option<Val>>,
    },
    OnUpdate {
        txn: Txn,
        from_name: String,
        from_value: Option<Val>,
    },
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct PropaChange {
    pub from_name: String,
    pub new_val: Val,
    pub preds: HashSet<Txn>,
    // pub requires: HashSet<Txn>,
}

#[derive(PartialEq, Eq, Hash, Clone, Debug)]
pub struct TxnAndName {
    pub txn: Txn,
    pub name: String,
}

#[derive(PartialEq, Eq, Clone, Debug)]
pub struct _PropaChange {
    pub propa_id: i32,
    pub propa_change: PropaChange,
    pub deps: HashSet<TxnAndName>,
}

impl Hash for _PropaChange {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.propa_id.hash(state);
    }
}
