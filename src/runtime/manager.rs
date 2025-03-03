use crate::{
    frontend::{meerast::Expr, typecheck::Type},
    runtime::{
        eval_expr,
        lock::{Lock, LockKind, LockType, LockWorkerInfo},
        message::{self, Message, Val, WorkerKind, CodeUpdate},
        transaction::{Txn, TxnId, WriteToName},
    },
};

use std::fmt;
use inline_colorization::*;
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc::{self, Receiver, Sender};

use super::{defworker::DefWorker, message::BUFFER_SIZE, varworker::VarWorker};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ValTxnInfo {
    pub val: Val,
    pub txn_id: TxnId,
}

#[derive(Debug, Clone)]
pub enum ManagerError {
    WorkerUnavailable(String),
    CyclicDependency(String),
    LockConflict(String),
    DistributedError(String),
}

impl fmt::Display for ManagerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ManagerError::WorkerUnavailable(name) => write!(f, "Worker unavailable: {}", name),
            ManagerError::CyclicDependency(cycle) => write!(f, "Cyclic dependency: {}", cycle),
            ManagerError::LockConflict(msg) => write!(f, "Lock conflict: {}", msg),
            ManagerError::DistributedError(msg) => write!(f, "Distributed error: {}", msg),
        }
    }
}

#[derive(Debug)]
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
    pub system_configuration: HashMap<String, Expr>,
    pub reverse_dependencies: HashMap<String, HashSet<String>>,
    pub node_locks: HashMap<String, Vec<LockType>>,
    pub active_transactions: HashMap<TxnId, HashSet<String>>,
    pub transaction_values: HashMap<String, Val>,
    pub subscript_versions: HashMap<String, u64>, 
    pub manager_id: Option<String>,
    pub managed_nodes: HashSet<String>, //which nodes this manager handles 
    pub peer_managers: HashMap<String, Sender<Message>>,
    pub node_manager_map: HashMap<String, String>, // Mapping from node names to the manager ID that owns them.
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
            system_configuration: HashMap::new(),
            reverse_dependencies: HashMap::new(),
            node_locks: HashMap::new(),
            active_transactions: HashMap::new(),
            transaction_values: HashMap::new(),
            subscript_versions: HashMap::new(),
            manager_id: None,
            managed_nodes: HashSet::new(),
            peer_managers: HashMap::new(),
            node_manager_map: HashMap::new(),
        }
    }

    /// Looks up the manager for a given node.
    /// Returns an error if the node is local or no remote manager is known.
    fn get_manager_for_node(&self, node: &str) -> Result<String, ManagerError> {
        if self.managed_nodes.contains(node) {
            Err(ManagerError::DistributedError(format!("Node {} is local", node)))
        } else {
            self.node_manager_map.get(node)
                .cloned()
                .ok_or_else(|| ManagerError::DistributedError(format!("Manager for node {} not found", node)))
        }
    }

    pub async fn acquire_upgrade_lock(
        &mut self,
        node: &str,
        txn_id: TxnId,
    ) -> Result<(), ManagerError> {
        // Initialize worker if it doesn't exist
        if !self.senders_to_workers.contains_key(node) {
            self.create_varworker(node).await;
        }
    
        // Initialize locks vector if it doesn't exist
        self.node_locks.entry(node.to_string()).or_default();
    
        // First check if we already have the upgrade lock
        {
            let locks = self.node_locks.get(node).unwrap();
            if locks
                .iter()
                .any(|lock| matches!(lock, LockType::Upgrade(id) if *id == txn_id))
            {
                return Ok(());
            }
        }
    
        // Collect transactions to abort
        let txns_to_abort: Vec<TxnId> = {
            let locks = self.node_locks.get(node).unwrap();
            locks
                .iter()
                .filter_map(|lock| match lock {
                    LockType::Read(id) | LockType::Write(id) | LockType::Upgrade(id) => {
                        Some(id.clone())
                    }
                })
                .collect()
        };
    
        // Abort all existing transactions
        for txn in txns_to_abort {
            self.abort_transaction(&txn)?;
        }
    
        // Clear existing locks before requesting new one
        if let Some(locks) = self.node_locks.get_mut(node) {
            locks.clear();
        }
    
        // Send upgrade lock request to worker
        let sender = self.senders_to_workers.get(node).unwrap().clone();
        let txn = Txn {
            id: txn_id.clone(),
            writes: vec![],
        };
    
        let lock_req_msg = Message::VarLockRequest {
            lock_kind: LockKind::Upgrade,
            txn: txn.clone(),
        };
        
        sender
            .send(lock_req_msg)
            .await
            .map_err(|_| ManagerError::WorkerUnavailable(node.to_string()))?;
    
        // Wait for response
        match self.receiver_from_workers.recv().await {
            Some(Message::VarLockGranted {
                txn: resp_txn,
                from_name,
            }) => {
                if resp_txn.id != txn_id {
                    return Err(ManagerError::LockConflict(format!(
                        "Received response for wrong transaction ID"
                    )));
                }
                assert_eq!(from_name, node);
    
                // Update lock tracking
                self.txn_locks_map
                    .entry(txn_id.clone())
                    .or_default()
                    .insert(LockWorkerInfo {
                        lock: Lock {
                            lock_kind: LockKind::Upgrade,
                            txn: txn.clone(),
                        },
                        worker_name: from_name,
                    });
    
                // Add upgrade lock to node
                self.node_locks
                    .get_mut(node)
                    .unwrap()
                    .push(LockType::Upgrade(txn_id));
    
                Ok(())
            }
            Some(Message::VarLockAbort { txn: resp_txn }) => {
                assert_eq!(resp_txn.id, txn_id);
                Err(ManagerError::LockConflict(format!(
                    "Failed to acquire upgrade lock on node {}",
                    node
                )))
            }
            _ => Err(ManagerError::WorkerUnavailable(node.to_string())),
        }
    } 

    fn abort_transaction(&mut self, txn_id: &TxnId) -> Result<(), ManagerError> {
        if let Some(nodes) = self.active_transactions.remove(txn_id) {
            for node in nodes {
                if let Some(locks) = self.node_locks.get_mut(&node) {
                    locks.retain(|lock| match lock {
                        LockType::Read(id) | LockType::Write(id) => id != txn_id,
                        _ => true,
                    });
                }
            }
        }
        Ok(())
    }

    fn translate_to_versioned_form(
        &mut self,
        update: &[(String, Expr)],
    ) -> Result<Vec<(String, Expr)>, ManagerError> {
        let mut versioned_updates = Vec::new();
        for (name, expr) in update {
            // Increment the version for this node
            let version = self.subscript_versions.entry(name.clone()).or_insert(0);
            let versioned_name = format!("{}{}", name, version);
            *version += 1;

            // Update the expression to use versioned dependencies
            let versioned_expr = self.version_expr(expr);

            versioned_updates.push((versioned_name, versioned_expr));
        }
        Ok(versioned_updates)
    }

    fn version_expr(&self, expr: &Expr) -> Expr {
        match expr {
            Expr::IdExpr { ident } => {
                let version = self.subscript_versions.get(ident).unwrap_or(&0).saturating_sub(1);
                Expr::IdExpr {
                    ident: format!("{}{}", ident, version),
                }
            }
            Expr::BopExpr { opd1, opd2, bop } => Expr::BopExpr {
                opd1: Box::new(self.version_expr(opd1)),
                opd2: Box::new(self.version_expr(opd2)),
                bop: bop.clone(),
            },
            _ => expr.clone(),
        }
    }

    // detect cycles in the dependency graph
    fn detect_cycles(&self, updates: &[(String, Expr)]) -> Result<(), ManagerError> {
        // Create a temporary graph that includes both existing and new dependencies
        let mut temp_graph: HashMap<String, HashSet<String>> = self.dependency_graph.clone();
        
        // Track which manager owns each node
        let mut node_ownership: HashMap<String, Option<String>> = HashMap::new();
        
        // Initialize ownership for existing nodes
        for (node, _) in &temp_graph {
            if self.managed_nodes.contains(node) {
                node_ownership.insert(node.clone(), Some(self.manager_id.clone().unwrap_or_default()));
            } else if let Some(manager_id) = self.node_manager_map.get(node) {
                node_ownership.insert(node.clone(), Some(manager_id.clone()));
            } else {
                node_ownership.insert(node.clone(), None);
            }
        }
    
        // Add new dependencies from the updates
        for (name, expr) in updates {
            let deps = expr.names_contained();
            temp_graph.insert(name.clone(), deps.clone());
            
            // Track ownership of new nodes
            if self.managed_nodes.contains(name) {
                node_ownership.insert(name.clone(), Some(self.manager_id.clone().unwrap_or_default()));
            }
            
            for dep in &deps {
                if !node_ownership.contains_key(dep) {
                    if self.managed_nodes.contains(dep) {
                        node_ownership.insert(dep.clone(), Some(self.manager_id.clone().unwrap_or_default()));
                    } else if let Some(manager_id) = self.node_manager_map.get(dep) {
                        node_ownership.insert(dep.clone(), Some(manager_id.clone()));
                    } else {
                        node_ownership.insert(dep.clone(), None);
                    }
                }
            }
        }
    
        // Helper function for DFS cycle detection
        fn has_cycle(
            graph: &HashMap<String, HashSet<String>>,
            node: &str,
            visited: &mut HashSet<String>,
            path: &mut HashSet<String>,
            node_ownership: &HashMap<String, Option<String>>,
            cycle_start: &mut Option<String>,
        ) -> Option<Vec<(String, Option<String>)>> {
            if path.contains(node) {
                *cycle_start = Some(node.to_string());
                return Some(vec![(node.to_string(), node_ownership.get(node).cloned().flatten())]);
            }
    
            if visited.contains(node) {
                return None;
            }
    
            path.insert(node.to_string());
    
            if let Some(deps) = graph.get(node) {
                for dep in deps {
                    if let Some(mut cycle) = has_cycle(graph, dep, visited, path, node_ownership, cycle_start) {
                        // Only add nodes until we complete the cycle
                        if let Some(ref start) = *cycle_start {
                            if node == start {
                                *cycle_start = None;
                            } else {
                                cycle.push((node.to_string(), node_ownership.get(node).cloned().flatten()));
                            }
                        }
                        return Some(cycle);
                    }
                }
            }
            // Remove node from current path (but leave it in visited)
            path.remove(node);
            visited.insert(node.to_string());
            None
        }
    
        // Check each node for cycles
        let mut visited = HashSet::new();
        let mut path = HashSet::new();
        let current_manager = self.manager_id.clone().unwrap_or_default();
    
        for node in temp_graph.keys() {
            let mut cycle_start = None;
            if let Some(cycle) = has_cycle(
                &temp_graph, 
                node, 
                &mut visited, 
                &mut path, 
                &node_ownership,
                &mut cycle_start,
            ) {
                // Collect managers involved in the cycle
                let managers: HashSet<String> = cycle.iter()
                    .filter_map(|(_, manager)| manager.clone())
                    .collect();
    
                // Only include manager information if there's more than one manager
                let cycle_str = cycle.into_iter().map(|(node, manager)| {
                    if managers.len() > 1 {
                        if let Some(mgr) = manager {
                            if mgr == current_manager {
                                format!("{} (local)", node)
                            } else {
                                format!("{} (manager: {})", node, mgr)
                            }
                        } else {
                            format!("{} (unknown manager)", node)
                        }
                    } else {
                        node
                    }
                }).collect::<Vec<_>>().join(" → ");
    
                let error_msg = if managers.len() > 1 {
                    format!(
                        "Cross-manager cyclic dependency detected across managers [{}]: {}",
                        managers.into_iter().collect::<Vec<_>>().join(", "),
                        cycle_str
                    )
                } else {
                    format!("Cyclic dependency detected: {}", cycle_str)
                };
    
                return Err(ManagerError::CyclicDependency(error_msg));
            }
        }
    
        Ok(())
    }

    pub async fn apply_code_update(&mut self, update: CodeUpdate, txn_id: &TxnId) -> Result<(), ManagerError> {  
        for node in update.nodes_to_modify.iter() {
            self.acquire_upgrade_lock(node, txn_id.clone()).await;
       }   
        // Translate to versioned form, incrementing versions
        let qualified_updates = self.translate_to_versioned_form(&update.new_code)?;

        // Remove old versions from all relevant data structures
        for base_name in &update.nodes_to_modify {
            let keys_to_remove: Vec<String> = self.system_configuration.keys()
                .filter(|k| k.starts_with(base_name))
                .cloned()
                .collect();
            
            for key in keys_to_remove {
                self.system_configuration.remove(&key);
                self.dependency_graph.remove(&key);
                for rev_deps in self.reverse_dependencies.values_mut() {
                    rev_deps.remove(&key);
                }
            }
        }

        // Add new versions with updated dependencies
        for (versioned_name, expr) in qualified_updates {
            let deps = expr.names_contained();
            self.system_configuration
                .insert(versioned_name.clone(), expr.clone());
            self.dependency_graph
                .insert(versioned_name.clone(), deps.clone());

            for dep in deps.clone() { 
                self.reverse_dependencies
                .entry(dep)
                .or_default()
                .insert(versioned_name.clone());
            }

            // Create workers as needed
            if deps.is_empty() {
                self.create_varworker(&versioned_name.clone()).await;
            } else {
                let transitive_deps = self.dependency_graph.clone();
                self.create_defworker(&versioned_name, &expr, transitive_deps)
                    .await;
            }
        } 
        Ok(())
    }

    /// This partitions an update into local and remote parts,
    /// sends remote updates via peer_managers, and waits for acknowledgements.
    pub async fn handle_code_update(&mut self, update: CodeUpdate) -> Result<(), ManagerError> {
        let txn_id = TxnId::new();
    
        if let Err(e) = self.detect_cycles(&update.new_code) { 
            return Err(e);
        }
    
        let mut local_update = CodeUpdate {
            nodes_to_modify: HashSet::new(),
            new_code: Vec::new(),
        };
        let mut remote_updates: HashMap<String, CodeUpdate> = HashMap::new();
    
        for node in update.nodes_to_modify.iter() {
            if self.managed_nodes.contains(node) {
                local_update.nodes_to_modify.insert(node.clone());
            } else {
                match self.get_manager_for_node(node) {
                    Ok(manager_id) => { 
                        remote_updates
                            .entry(manager_id)
                            .or_insert(CodeUpdate { nodes_to_modify: HashSet::new(), new_code: Vec::new() })
                            .nodes_to_modify
                            .insert(node.clone());
                    }
                    Err(e) => { 
                        return Err(e);
                    }
                }
            }
        }
    
        for (node, expr) in update.new_code.into_iter() {
            if self.managed_nodes.contains(&node) {
                local_update.new_code.push((node, expr));
            } else {
                match self.get_manager_for_node(&node) {
                    Ok(manager_id) => {
                        remote_updates
                            .entry(manager_id)
                            .or_insert(CodeUpdate { nodes_to_modify: HashSet::new(), new_code: Vec::new() })
                            .new_code
                            .push((node, expr));
                    }
                    Err(e) => { 
                        return Err(e);
                    }
                }
            }
        }

        for (manager_id, remote_update) in &remote_updates {
            if let Some(peer_sender) = self.peer_managers.get(manager_id) { 
                let send_result = peer_sender
                    .send(Message::DistributedCodeUpdate {
                        code_update: remote_update.clone(),
                        txn_id: txn_id.clone(),
                        sender_manager_id: self.manager_id.clone().unwrap_or_default(), // Added
                    })
                    .await;
                if let Err(e) = send_result { 
                    return Err(ManagerError::DistributedError(
                        format!("Failed to send update to manager {}: {:?}", manager_id, e)
                    ));
                }
            } else {
                return Err(ManagerError::DistributedError(
                    format!("Peer manager {} not found", manager_id)
                ));
            }
            }
        
        self.apply_code_update(local_update, &txn_id).await?; 
    
        let mut acks_received = 0;
        let expected_acks = remote_updates.len();
    
        while acks_received < expected_acks {
            if let Some(msg) = self.receiver_from_workers.recv().await {
                if let Message::DistributedCodeUpdateAck { txn_id: ack_txn, success, manager_id } = msg {
                    if ack_txn == txn_id {
                        if !success {
                            return Err(ManagerError::DistributedError(
                                format!("Update failed at manager {}", manager_id)
                            ));
                        }
                        acks_received += 1; 
                    }
                }
            }
        } 

        // Release upgrade locks immediately after applying updates
        for node in &update.nodes_to_modify {
            if let Some(locks) = self.node_locks.get_mut(node) {
                locks.retain(|lock| !matches!(lock, LockType::Upgrade(id) if id == &txn_id));
            }
        }
        Ok(())

    }       

    pub async fn handle_message(&mut self, msg: Message) -> Result<(), ManagerError> {
        match msg {
            Message::DistributedCodeUpdate { code_update, txn_id, sender_manager_id } => {
                let update_result = self.apply_code_update(code_update, &txn_id).await;
    
                // Send acknowledgment via the peer channel back to the sender
                if let Some(peer_sender) = self.peer_managers.get(&sender_manager_id) {
                    let ack_msg = Message::DistributedCodeUpdateAck {
                        txn_id,
                        success: update_result.is_ok(),
                        manager_id: self.manager_id.clone().unwrap_or_default(),
                    };
                    peer_sender.send(ack_msg).await.map_err(|e| {
                        ManagerError::DistributedError(format!("Failed to send ack: {}", e))
                    })?;
                } else {
                    return Err(ManagerError::DistributedError(
                        format!("Peer manager {} not found", sender_manager_id)
                    ));
                } 
            }
            _ => { }
        }
        Ok(())
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
        let var_worker = VarWorker::new(name, rcvr_from_manager, self.sender_to_manager.clone());
        self.senders_to_workers
            .insert(name.to_string(), sndr_from_manager);
        // TODO. Added for testing. Does this suffice for updating the worker kind environment?
        self.worker_kind_env
            .insert(name.to_string(), WorkerKind::Var);
        self.managed_nodes.insert(name.to_string());
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
            defs_sndr.clone(),
            defs_rcvr,
            self.sender_to_manager.clone(),
            init_expr,
            transitive_deps,
        );
        self.senders_to_workers.insert(name.to_string(), defs_sndr.clone());
        self.worker_kind_env.insert(name.to_string(), WorkerKind::Def);
        self.managed_nodes.insert(name.to_string());

        // Update ownership mapping: record when this manager owns the node.
        if let Some(my_id) = &self.manager_id {
            self.node_manager_map.insert(name.to_string(), my_id.clone());
        }

        // Immediately subscribe to dependencies 
        let deps = init_expr.names_contained();
        for dep in deps {
            if self.managed_nodes.contains(&dep) {
                let _ = defs_sndr
                    .send(Message::Subscribe {
                        subscribe_who: dep,
                        subscriber_name: name.to_string(),
                        sender_to_subscriber: defs_sndr.clone(),
                    })
                    .await;
            } else if let Some(manager_id) = self.node_manager_map.get(&dep) {
                if let Some(peer_sender) = self.peer_managers.get(manager_id) {
                    let _ = peer_sender
                        .send(Message::DistributedSubscribe {
                            remote_node: dep,
                            subscriber_node: name.to_string(),
                            subscriber_sender: defs_sndr.clone(),
                        })
                        .await;
                }
            } else {
                // Fallback if no remote mapping is found.
                let _ = defs_sndr
                    .send(Message::Subscribe {
                        subscribe_who: dep,
                        subscriber_name: name.to_string(),
                        sender_to_subscriber: defs_sndr.clone(),
                    })
                    .await;
            }
        }


        tokio::spawn(def_worker.run_defworker());
    }

    // Do we really need the instruction `close(txn_id)`?
    // Yes! Because need to remember {txn |-> lock_info set}

    pub fn retrieve_val(&self, name: &str) -> Option<Val> {
        let base_name = name.trim_end_matches(|c: char| c.is_numeric());
        let version = self.subscript_versions.get(base_name).unwrap_or(&0).saturating_sub(1); 
        let versioned_name = format!("{}{}", base_name, version);

        if let Some(expr) = self.system_configuration.get(&versioned_name) {
            let mut val_env = HashMap::new();

            // Recursively get values for dependencies
            for dep in expr.names_contained() {
                if let Some(dep_val) = self.retrieve_val(&dep) {
                    val_env.insert(dep.clone(), Some(dep_val));
                } else {
                    return None;
                }
            }

            eval_expr::evaluate_expr(expr, &val_env)
        } else {
            None
        }
    }
}
