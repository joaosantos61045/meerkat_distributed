// defworker
// read lock

// append following to existing meerkat project:
/*
    var a = 0;
    var b = 0;
    def f = a + b;
    def g = f;
*/
/*
Service Manager:
- create all worker a, b, f OR require RLocks for {a, b} and WLock for {f}
- for code update, processing line l+1 will be blocked by line l (dependency)
- for newly created
- gather all typing inforation permitted by RLock, info directly from typing env
  maintained by service manager
- defworker process Write:
    - establish all dependencies (graph's structure) by subscribption,
    - gain all dependencies' values (current inputs)
*/

// One problem:
// for a defwork, code update vs pending change message (v, P, R), which apply first
// - idea 1 : ignore all pending change messages (code update has precedence over user's, may have glitch)
// - idea 1.5: when propa changes involving transaction from last 'epoch' are received
//   we still process them based on latest version of def. If 
// - next step to think about:
    // - idea 2 : wait for all pending change messages to finish
    // - idea 3 : when update don't change type, can we lock less
/*
update {
    var c = 1;
    def f' = a * b * c; // if read(f') >= read(f), then batch validity remains
    def g = f;
}
*/

/*
update {
    var c = 1;
    def f' = a;         // wait for pending messages to finish, then apply code update
    def g = f;          // since now batch validity changes
}
*/

/*
update {
    var c = 1;
    def f' = a * b * c;
    def g = f;
}
 |concurrently|
update {
    var c = 1;
    def f' = a;
    def g = f;
}
*/
// one WLock rejected

/*
var c = 1;
def f1 = a;
def f2 = b;
def g = f1 + f2;

dev1 update {
    def f2 = b + c;
}
 |concurrently|
dev2 update {
    def f1 = a + c;
}
*/
// dev1:: WLock: {f1}; RLock: {b, c, g}
// dev2:: WLock: {f2}; RLock: {a, c, g}

// grant write lock to developer

// process Write Message from developer
// process write lock

use std::{
    collections::{HashMap, HashSet},
    hash::Hash, thread::panicking,
};

use crate::{
    frontend::meerast::Expr,
    runtime::{
        def_batch_utils::{apply_batch, search_batch},
        lock::{Lock, LockKind},
        message::{Message, PropaChange, TxnAndName, Val, _PropaChange},
        transaction::{Txn, TxnId, WriteToName},
    },
};

use tokio::sync::mpsc::{self, Receiver, Sender};
const BUFFER_SIZE: usize = 1024;


use inline_colorization::*;

use super::{eval_expr::{self, evaluate_expr}, message, varworker::PendingWrite};

#[derive(Debug, Clone)]
pub struct PendingCodeUpdate {
    // pub w_lock: Lock,
    pub expr: Expr,
    pub inputs_data: HashMap<String, Option<(Option<Val>, HashSet<Txn>, HashSet<String>)>>,
}

pub struct DefWorker {
    pub name: String,
    pub def_rcvr: Receiver<Message>, 
    pub def_sndr: Sender<Message>,
    // Anrui: rename it to inbox, as it's created and used by def worker as general 
    // inbox, not only receiving messages from service manager
    pub sender_to_manager: Sender<Message>,
    pub senders_to_subscribers: HashMap<String, Sender<Message>>,

    pub value: Option<Val>,
    pub pred_txns: Vec<Txn>,
    pub prev_batch_provides: HashSet<Txn>,
    // data structure maintaining all propa_changes to be apply
    pub propa_changes_to_apply: HashMap<TxnAndName, _PropaChange>,

    // for now expr is list of name or values calculating their sum
    pub expr: Expr,
    // direct dependency and their current value
    pub replica: HashMap<String, Option<Val>>,
    // transtitive dependencies: handled by srvmanager for local dependencies
    // and SubscribeRequest/Grant for global dependencies
    pub transtitive_deps: HashMap<String, HashSet<String>>,
    // input(def) -> vars that the input(def) depend on
    /*
      a   b   c
       \  |  /
          d
       a -> [] // or reflexively contain a ?
       b -> [] // or reflexively contain b ?
       c -> [] // or reflexively contain c ?
    */
    pub counter: i32,

    pub locks: HashSet<Lock>,
    pub lock_queue: HashSet<Lock>,
    pub pending_write: Option<PendingCodeUpdate>,
}

impl DefWorker {
    pub fn new(
        name: &str,
        def_sndr: mpsc::Sender<Message>,
        def_rcvr: mpsc::Receiver<Message>,
        sender_to_manager: mpsc::Sender<Message>,
        init_expr: &Expr,
        // replica: HashMap<String, Option<Val>>, // HashMap { dependent name -> None }
        transtitive_deps: HashMap<String, HashSet<String>>,
    ) -> DefWorker {
        DefWorker {
            name: name.to_string(),
            def_sndr, def_rcvr,
            
            sender_to_manager,
            senders_to_subscribers: HashMap::new(),

            value: None,
            pred_txns: Vec::new(),
            prev_batch_provides: HashSet::new(),
            propa_changes_to_apply: HashMap::new(),

            expr: init_expr.clone(),
            replica: init_expr.names_contained().into_iter().map(|name| (name, None)).collect(),
            transtitive_deps,
            counter: 0,

            locks: HashSet::new(),
            lock_queue: HashSet::new(),
            pending_write: None,
        }
    }

    pub fn next_count(counter_ref: &mut i32) -> i32 {
        *counter_ref += 1;
        *counter_ref
    }

    pub fn has_write_lock(&self) -> bool {
        for lk in self.locks.iter() {
            if lk.lock_kind == LockKind::Write {
                return true;
            }
        }
        false
    }

    pub async fn tick(&mut self) {
        if self.lock_queue.is_empty() {
            return;
        }
        let oldest_lock = self
            .lock_queue
            .iter()
            .min_by(|x, y| x.txn.id.cmp(&y.txn.id))
            .unwrap()
            .clone();
        self.lock_queue.remove(&oldest_lock);
        self.locks.insert(oldest_lock.clone());
        let lock_granted_msg = Message::VarLockGranted {
            txn: oldest_lock.txn,
            from_name: self.name.clone(),
        };
        self.sender_to_manager.send(lock_granted_msg).await.unwrap();
    }

    pub fn apply_code_update(&mut self) {
        // we have everything stored in PendingCodeUpdate, now we can apply update
        let pcu =  self.pending_write.as_ref().unwrap();
        self.expr = pcu.expr.clone();
      
        for (name, op_data) in pcu.inputs_data.iter() {
            let data = op_data.as_ref().unwrap();
            // data.0 parent's current value 
            self.replica.insert(name.to_string(), data.0.clone());
            // data.1 parent's applied transactions 
            // todo!("process provides accumulated by the input node?");
            self.pred_txns.extend(data.1.iter().cloned());
            // data.2 parent's transtitive dependency
            self.transtitive_deps.insert(name.to_string(), data.2.clone());
        }

        self.value = evaluate_expr(&self.expr, &self.replica);
        self.pending_write = None;
    }

    pub async fn handle_message(&mut self, msg: Message) {
        match msg {
            Message::DefWorkerUpdate { node, expr } => {
                // Unsubscribe from old dependencies
                let old_deps = self.expr.names_contained();
                for dep in old_deps {
                    self.sender_to_manager
                        .send(Message::DeSubscribe {
                            desubscribe_who: dep,
                            desubscriber_name: self.name.clone(),
                        })
                        .await
                        .unwrap();
                }

                // Update expression and dependencies
                self.expr = expr.clone();
                let new_deps = expr.names_contained();
                self.replica = new_deps.iter().map(|d| (d.clone(), None)).collect();

                // Subscribe to new dependencies 
                for dep in new_deps {
                    self.sender_to_manager
                        .send(Message::Subscribe {
                            subscribe_who: dep,
                            subscriber_name: self.name.clone(),
                            sender_to_subscriber: self.def_sndr.clone(),
                        })
                        .await
                        .unwrap();
                }
            }

            Message::DefLockRequest { 
                lock_kind, 
                txn 
            } => {
                let mut oldest_txn_id = txn.id.clone();
                for lock in self.locks.iter() {
                    if lock.txn.id <= oldest_txn_id {
                        oldest_txn_id = lock.txn.id.clone();
                    }
                }
                if txn.id == oldest_txn_id
                    || (!self.has_write_lock() && self.pending_write.is_none())
                {
                    self.lock_queue.insert(Lock {
                        lock_kind: lock_kind,
                        txn: txn,
                    });
                } else {
                    let _ = self
                        .sender_to_manager
                        .send(Message::DefLockAbort { txn: txn })
                        .await
                        .unwrap();
                }
            }
            Message::DefLockRelease { txn } => {
                let to_be_removed: HashSet<Lock> = self
                    .locks
                    .iter()
                    .cloned()
                    .filter(|t| t.txn.id == txn.id)
                    .collect();
                for tbr in to_be_removed.iter() {
                    self.locks.remove(tbr);
                }
            }
            Message::DevReadDefRequest { txn } => {
                let mut WLock: Option<Lock> = None;
                for lock in self.locks.iter() {
                    if lock.lock_kind == LockKind::Read && txn.id == lock.txn.id {
                        WLock = Some(lock.clone());
                        break;
                    }
                }
                match WLock {
                    // todo!("should we do");
                    //     // self.pred_txns.push(txn.clone());
                    Some(l) => {
                        let _ = self
                            .sender_to_manager
                            .send(Message::DevReadDefResult {
                                name: self.name.clone(),
                                txn: txn,
                            })
                            .await;
                        self.locks.remove(&l);
                    }
                    None => panic!(),
                }
            }
            Message::DevWriteDefRequest { 
                txn, 
                write_expr 
            } => {
                let mut w_lock_granted = false;
                let mut w_lock_for_txn: Option<Lock> = None;
                for lk in self.locks.iter() {
                    if lk.lock_kind == LockKind::Write && lk.txn.id == txn.id {
                        w_lock_granted = true;
                        w_lock_for_txn = Some(lk.clone());
                    }
                }
                if !w_lock_granted {
                    panic!("in def worker attempt to write when no write lock");
                } else {
                    self.locks.remove(&(w_lock_for_txn.unwrap()));

                    // now start processing the update
                    let inputs = write_expr.names_contained();
                    let old_inputs = self.replica.keys().cloned().collect();
                    let added_inputs: HashSet<String> = inputs.difference(&old_inputs).cloned().collect();
                    let deleted_inputs: HashSet<String> = old_inputs.difference(&inputs).cloned().collect();

                    // request subscription for all inputs, 
                    // service manager will handle the following
                    for input_name in added_inputs.iter() {
                        let _ = self.sender_to_manager.send(Message::Subscribe { 
                            subscribe_who: input_name.clone(), 
                            subscriber_name: self.name.clone(),
                            sender_to_subscriber: self.def_sndr.clone(), 
                        });
                    } 
                    for input_name in deleted_inputs.iter() {
                        let _ = self.sender_to_manager.send(Message::DeSubscribe { 
                            desubscribe_who: input_name.clone(),
                            desubscriber_name: self.name.clone(), 
                        });
                    } 

                    self.pending_write = Some(PendingCodeUpdate{
                        // w_lock: Lock {
                        //     lock_kind: LockKind::Write,
                        //     txn: txn.clone(),
                        // },
                        expr: write_expr.clone(),
                        inputs_data: added_inputs.into_iter().map(|name| (name.to_string(), None)).collect(),
                    });
                }
            }
            Message::UsrReadDefRequest { txn, requires } => {
                let result_pred = self.pred_txns.clone().into_iter().collect();
                let msg_back = Message::UsrReadDefResult {
                    // TODO(Optional optimization): set smaller than applied_txns should also work ...
                    txn: txn.clone(),
                    name: self.name.clone(),
                    result: self.value.clone(),
                    result_pred, // send back pred_txn (applied txns) to manager
                };
                let _ = self.sender_to_manager.send(msg_back).await;
            }
            Message::Propagate { propa_change } => {
                if self.replica.contains_key(&propa_change.from_name) {
                // Follows idea 1, ignore all irrelavant propagate messages
                    println!("{color_blue}PropaMessage{color_reset}");
                    let _propa_change = Self::processed_propachange(
                        &mut self.counter,
                        &propa_change,
                        &mut self.transtitive_deps,
                    );

                    for txn in &propa_change.preds {
                        println!("{color_blue}insert propa_changes_to_apply{color_reset}");
                        self.propa_changes_to_apply.insert(
                            TxnAndName {
                                txn: txn.clone(),
                                name: propa_change.from_name.clone(),
                            },
                            _propa_change.clone(),
                        );
                    }
                    println!(
                        "after receiving propamsg, the graph is {:#?}",
                        &self.propa_changes_to_apply
                    );
                } 
            }
            Message::Subscribe { 
                subscribe_who,
                subscriber_name, 
                sender_to_subscriber 
            } => {
               self.senders_to_subscribers.insert(subscriber_name, sender_to_subscriber.clone());
               let respond_msg = Message::SubscriptionGranted { 
                    name: self.name.clone(), 
                    value: self.value.clone(), 
                    provides: HashSet::from_iter(self.pred_txns.clone()), // not sure!!
                    trans_dep_set: self.transtitive_deps.values().fold(HashSet::new(), 
                        |mut acc, set| {acc.extend(set.iter().cloned()); acc}), 
                };
                let _ = sender_to_subscriber.send(respond_msg).await.unwrap();
            }
            Message::SubscriptionGranted { 
                name, 
                value, 
                provides, 
                trans_dep_set
            } => {
                let pcu = self.pending_write.as_mut().unwrap();
                pcu.inputs_data.insert(name, Some((value, provides, trans_dep_set))); 
                if pcu.inputs_data.values().all(|v| v.is_some()) {
                    self.apply_code_update();
                }
            }
            Message::DeSubscriptionGranted { name,} => {
                /* todo!("
                    we haven't implement desubscribe yet, in current implementation,
                    we simply insert new subscriptions in current def node, without
                    deleting no-longer-used subscriptions");
                 */ 
            }

            // for test only
            // Message::ManagerRetrieve => {
            //     let msg = Message::ManagerRetrieveResult {
            //         name: worker.name.clone(),
            //         result: curr_val.clone(),
            //     };
            //     let _ = worker.sender_to_manager.send(msg).await;
            // }
            _ => panic!(),
        }
    }

    pub async fn run_defworker(mut self) {
        while let Some(msg) = self.def_rcvr.recv().await {
            println!("{color_red}defworker receive msg {:?}{color_reset}", msg);
            // handle messages
            let _ = DefWorker::handle_message(&mut self, msg).await;

            // evict next lock
            let _ = self.tick().await;

            // search for valid batch
            let valid_batch =
                search_batch(&self.propa_changes_to_apply, &self.pred_txns);

            // apply valid batch
            if !valid_batch.is_empty() {
                println!("{color_yellow}apply batch called{color_reset}");
                let (all_provides, new_value) = apply_batch(
                    valid_batch,
                    // &def_worker.worker,
                    &mut self.value,
                    &mut self.pred_txns,
                    &mut self.prev_batch_provides,
                    &mut self.propa_changes_to_apply,
                    &mut self.replica,
                    &self.expr,
                );
            }




            // for test, ack srvmanager
            // if new_value != None {
            //     let msg = Message::ManagerRetrieveResult {
            //         name: def_worker.worker.name.clone(),
            //         result: new_value.clone(),
            //     };
            //     let _ = def_worker.worker.sender_to_manager.send(msg).await;

            //     // broadcast the update to subscribers
            //     let msg_propa = Message::PropaMessage {
            //         propa_change: PropaChange {
            //             name: def_worker.worker.name.clone(),
            //             new_val: new_value.unwrap(),
            //             provides: all_provides.clone(),
            //             requires: all_requires.clone(),
            //         },
            //     };
            //     for succ in def_worker.worker.senders_to_succs.iter() {
            //         let _ = succ.send(msg_propa.clone()).await;
            //     }
            // }

            // println!(
            //     "{color_red}run def worker, def_worker.value after apply_batch: {:?}{color_reset}",
            //     def_worker.value
            // );
        }
    }

    pub fn processed_propachange(
        counter_ref: &mut i32,
        propa_change: &PropaChange,
        transtitive_deps: &HashMap<String, HashSet<String>>,
        // expect inputs(d) maps to transtitively depending vars
    ) -> _PropaChange {
        println!("def should have inputs: {:?}", transtitive_deps);
        let mut deps: HashSet<TxnAndName> = HashSet::new();

        for txn in propa_change.preds.iter() {
            for write in txn.writes.iter() {
                let var_name = write.name.clone();
                println!("def name: {:?}", var_name);

                let mut inputs: Vec<String> = Vec::new();
                for (i, dep_vars) in transtitive_deps.iter() {
                    match dep_vars.get(&var_name) {
                        Some(_) => {
                            println!("def add input: {:?}", i);
                            inputs.push(i.clone());
                        }
                        None => {}
                    }
                }
                println!("def has inputs: {:?}", inputs);

                for i_name in inputs.iter() {
                    let txn_name = TxnAndName {
                        txn: txn.clone(),
                        name: i_name.clone(),
                    };
                    deps.insert(txn_name);
                }
            }
        }

        // Not fully sure about below:

        // for txn in propa_change.requires.iter() {
        //     for write in txn.writes.iter() {
        //         let var_name = write.name.clone();

        //         let mut inputs: Vec<String> = Vec::new();
        //         for (i, dep_vars) in transtitive_deps.iter() {
        //             match dep_vars.get(&var_name) {
        //                 Some(_) => {
        //                     inputs.push(i.clone());
        //                 }
        //                 None => {}
        //             }
        //         }

        //         for i_name in inputs.iter() {
        //             let txn_name = TxnAndName {
        //                 txn: txn.clone(),
        //                 name: i_name.clone(),
        //             };
        //             deps.insert(txn_name);
        //         }
        //     }
        // }

        _PropaChange {
            propa_id: Self::next_count(counter_ref),
            propa_change: propa_change.clone(),
            deps,
        }
    }
}
