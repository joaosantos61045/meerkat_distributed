use crate::{
    frontend::{meerast::Expr, typecheck::Type},
    runtime::{
        eval_expr,
        lock::{Lock, LockKind},
        message::{self, Message, Val},
        transaction::{Txn, TxnId, WriteToName},
    },
};

use inline_colorization::*;
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc::{self, Receiver, Sender};

use super::{defworker::DefWorker, message::BUFFER_SIZE, varworker::VarWorker};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WorkerKind {
    Var,
    Def,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LockWorkerInfo {
    pub lock: Lock,
    pub worker_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ValTxnInfo {
    pub val: Val,
    pub txn_id: TxnId,
}

pub struct Manager {
    // cloned and given to new workers when creating them
    pub sender_to_manager: Sender<Message>,
    pub receiver_from_workers: Receiver<Message>,
    pub senders_to_workers: HashMap<String, Sender<Message>>,
    pub typing_env: HashMap<String, Type>,
    pub worker_kind_env: HashMap<String, WorkerKind>,
    // { txn_id |-> { locks } }. What locks have this txn gotten
    pub txn_locks_map: HashMap<TxnId, HashSet<LockWorkerInfo>>,
    // { name |-> { subscribers } }
    pub dependency_graph: HashMap<String, HashSet<String>>,
    pub most_recently_applied_txn: Option<Txn>,
}

impl Manager {
    pub fn new() -> Self {
        let (sndr, rcvr) = mpsc::channel(message::BUFFER_SIZE);
        Manager {
            sender_to_manager: sndr,     // sndr created by manager
            receiver_from_workers: rcvr, // rcvr created by manager, used for workers
            senders_to_workers: HashMap::new(),
            typing_env: HashMap::new(),
            worker_kind_env: HashMap::new(),
            txn_locks_map: HashMap::new(),
            dependency_graph: HashMap::new(),
            most_recently_applied_txn: None,
        }
    }

    pub async fn handle_transaction(&mut self, txn: &Txn) {
        let mut names_read_by_txn = HashSet::new();
        let mut names_written_by_txn = HashSet::new();
        for w2n in txn.writes.iter() {
            let ex = w2n.expr.clone();
            names_read_by_txn.extend(ex.names_contained().into_iter());
            names_written_by_txn.insert(w2n.name.clone());
        }
        loop {
            let mut temp_val_env: HashMap<String, Val> = HashMap::new();
            let mut read_abort = false;
            let mut write_abort = false;
            let mut this_txn_write_requires = HashSet::new();
            for nm in names_read_by_txn.iter() {
                let var_or_def = self.worker_kind_env.get(nm).unwrap();
                if *var_or_def == WorkerKind::Var {
                    let opt_val = self
                        .read_single_var(nm, txn, &mut this_txn_write_requires)
                        .await;
                    if opt_val == None {
                        read_abort = true;
                        break;
                    } else {
                        let val_of_nm = opt_val.unwrap();
                        temp_val_env.insert(nm.clone(), val_of_nm);
                    }
                } else {
                    todo!()
                }
            }
            if read_abort {
                for nm in names_read_by_txn.iter() {
                    let sender_to_this_nm = self.senders_to_workers.get(nm).unwrap().clone();
                    let _ = sender_to_this_nm
                        .send(Message::VarLockAbort { txn: txn.clone() })
                        .await
                        .unwrap();
                }
                continue;
            }
            for w2n in txn.writes.iter() {
                let var_or_def = self.worker_kind_env.get(&w2n.name).unwrap();
                assert_eq!(*var_or_def, WorkerKind::Var);
                let write_success = self.write_single_var(w2n, &temp_val_env, txn).await;
                if !write_success {
                    write_abort = true;
                    break;
                }
            }
            if write_abort {
                for nm in names_read_by_txn.iter() {
                    let sender_to_this_nm = self.senders_to_workers.get(nm).unwrap().clone();
                    let _ = sender_to_this_nm
                        .send(Message::VarLockAbort { txn: txn.clone() })
                        .await
                        .unwrap();
                }
                for nm in names_written_by_txn.iter() {
                    let sender_to_this_nm = self.senders_to_workers.get(nm).unwrap().clone();
                    let _ = sender_to_this_nm
                        .send(Message::VarLockAbort { txn: txn.clone() })
                        .await
                        .unwrap();
                }
                continue;
            }
            self.release_var_locks(txn, &this_txn_write_requires).await;
            break;
        }
    }

    // request lock, read and return. return None if LOCK ABORT.
    async fn read_single_var(
        &mut self,
        var_name: &str,
        txn: &Txn,
        this_txn_write_requires: &mut HashSet<Txn>,
    ) -> Option<Val> {
        let sender_to_this_var = self.senders_to_workers.get(var_name).unwrap().clone();
        let lock_req_msg = Message::VarLockRequest {
            lock_kind: LockKind::Read,
            txn: txn.clone(),
        };
        let _ = sender_to_this_var.send(lock_req_msg).await.unwrap();
        if let Some(grant_msg) = self.receiver_from_workers.recv().await {
            match grant_msg {
                Message::VarLockGranted {
                    txn: resp_txn,
                    from_name,
                } => {
                    assert_eq!(
                        resp_txn.id, txn.id,
                        "{color_red}should not receive grant \
msg for other txns, but is this implementation correct?{color_reset}"
                    );
                    // TODO: check if this suffices for txn not yet in the txn_lock_map
                    if !self.txn_locks_map.contains_key(&txn.id) {
                        self.txn_locks_map.insert(txn.id.clone(), HashSet::new());
                    }
                    let txn_lock_ref = self.txn_locks_map.get_mut(&txn.id).unwrap();
                    txn_lock_ref.insert(LockWorkerInfo {
                        lock: Lock {
                            lock_kind: LockKind::Read,
                            txn: txn.clone(),
                        },
                        worker_name: from_name,
                    });
                    let read_req_msg = Message::UsrReadVarRequest { txn: txn.clone() };
                    let _ = sender_to_this_var.send(read_req_msg).await.unwrap();
                    if let Some(read_resp_msg) = self.receiver_from_workers.recv().await {
                        match read_resp_msg {
                            Message::UsrReadVarResult {
                                var_name: rslt_var_name,
                                result,
                                result_preds,
                                txn: read_rslt_txn,
                                var_version: _,
                            } => {
                                assert_eq!(
                                    read_rslt_txn.id, txn.id,
                                    "{color_red}should not receive read rslt \
msg for other txns, but is this implementation correct?{color_reset}"
                                );
                                assert_eq!(rslt_var_name, var_name);
                                let read_last_txn =
                                    result_preds.into_iter().max_by(|x, y| x.id.cmp(&y.id));
                                if read_last_txn != None {
                                    this_txn_write_requires.insert(read_last_txn.unwrap());
                                }
                                return Some(result.unwrap());
                            }
                            _ => panic!(
                                "{color_red}should not receive non read rslt message \
when require read locks, but really?{color_reset}"
                            ),
                        }
                    }
                }
                Message::VarLockAbort { txn: resp_txn } => {
                    assert_eq!(
                        resp_txn.id, txn.id,
                        "{color_red}should not receive abort \
msg for other txns, but is this implementation correct?{color_reset}"
                    );
                    return None;
                }
                _ => panic!(
                    "{color_red}should not receive non-grant message when \
require read locks, but really?{color_reset}"
                ),
            }
        }
        panic!("should not come to here!")
    }

    // return true if write successful, return false if write lock abort occurs
    async fn write_single_var(
        &mut self,
        write_to_var: &WriteToName,
        val_env: &HashMap<String, Val>,
        txn: &Txn,
    ) -> bool {
        let mut opt_val_env = HashMap::new();
        for (nm, v) in val_env.iter() {
            opt_val_env.insert(nm.clone(), Some(v.clone()));
        }
        let write_val = eval_expr::evaluate_expr(&write_to_var.expr, &opt_val_env).unwrap();
        let lock_req_msg = Message::VarLockRequest {
            lock_kind: LockKind::Write,
            txn: txn.clone(),
        };
        let sender_to_this_var = self
            .senders_to_workers
            .get(&write_to_var.name)
            .unwrap()
            .clone();
        let _ = sender_to_this_var.send(lock_req_msg).await.unwrap();
        if let Some(grant_msg) = self.receiver_from_workers.recv().await {
            match grant_msg {
                Message::VarLockGranted {
                    txn: resp_txn,
                    from_name,
                } => {
                    assert_eq!(resp_txn.id, txn.id);
                    assert_eq!(from_name, write_to_var.name);
                    // TODO: check if this suffices for txn not yet in the txn_lock_map
                    if !self.txn_locks_map.contains_key(&txn.id) {
                        self.txn_locks_map.insert(txn.id.clone(), HashSet::new());
                    }
                    let txn_lock_ref = self.txn_locks_map.get_mut(&txn.id).unwrap();
                    txn_lock_ref.insert(LockWorkerInfo {
                        lock: Lock {
                            lock_kind: LockKind::Write,
                            txn: txn.clone(),
                        },
                        worker_name: from_name,
                    });
                    let write_msg = Message::UsrWriteVarRequest {
                        txn: txn.clone(),
                        write_val: write_val,
                    };
                    let _ = sender_to_this_var.send(write_msg).await.unwrap();
                }
                Message::VarLockAbort { txn: resp_txn } => {
                    assert_eq!(resp_txn.id, txn.id);
                    return false;
                }
                _ => panic!(),
            }
        }
        true
    }

    async fn release_var_locks(&mut self, txn: &Txn, this_txn_write_requires: &HashSet<Txn>) {
        let lwis_ref = self.txn_locks_map.get(&txn.id).unwrap();
        for lwi in lwis_ref.iter() {
            let sender_to_this_worker = self.senders_to_workers.get(&lwi.worker_name).unwrap();
            let var_lock_release_msg = Message::VarLockRelease {
                txn: txn.clone(),
                requires: this_txn_write_requires.clone(),
            };
            let _ = sender_to_this_worker
                .send(var_lock_release_msg)
                .await
                .unwrap();
        }
    }

    pub async fn create_varworker(&mut self, name: &str) {
        // the channel send from manager to worker
        let (sndr_from_manager, rcvr_from_manager) = mpsc::channel(BUFFER_SIZE);
        let var_worker = VarWorker::new(
            name,
            rcvr_from_manager,
            self.sender_to_manager.clone(),
            Some(Val::Int(0)),
        );
        self.senders_to_workers
            .insert(name.to_string(), sndr_from_manager);
        // TODO. Added for testing. Does this suffice for updating the worker kind environment?
        self.worker_kind_env
            .insert(name.to_string(), WorkerKind::Var);
        tokio::spawn(var_worker.run_varworker());
    }

    pub async fn create_defworker(
        &mut self,
        name: &str,
        init_expr: &Expr,
        transitive_deps: HashMap<String, HashSet<String>>,
    ) {
        // the channel send from manager to worker
        let (defs_sndr, defs_rcvr) = mpsc::channel(BUFFER_SIZE);
        let def_worker = DefWorker::new(
            name,
            self.sender_to_manager.clone(),
            defs_sndr.clone(),
            defs_rcvr,
            init_expr,
            transitive_deps,
        );
        self.senders_to_workers.insert(name.to_string(), defs_sndr);
        tokio::spawn(def_worker.run_defworker());
    }

    // Do we really need the instruction `close(txn_id)`?
    // Yes! Because need to remember {txn |-> lock_info set}
}
