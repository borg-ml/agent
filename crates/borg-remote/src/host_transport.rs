use crate::auth::TunnelFrame;
use anyhow::{Result, ensure};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

#[derive(Clone)]
pub(super) struct Transport {
    host: Uuid,
    pub outbound: mpsc::Sender<TunnelFrame>,
    state: Arc<Mutex<RegistrationState>>,
    tasks: Arc<Mutex<Vec<tokio::task::AbortHandle>>>,
    ledger: Arc<OnceLock<crate::command_ledger::CommandLedger>>,
    routes: Arc<Mutex<Vec<crate::auth::HostRoute>>>,
}
#[derive(Default)]
struct RegistrationState {
    generation: u64,
    clock: u64,
    catalog: HashMap<Uuid, CatalogEntry>,
    pending: HashMap<String, Registration>,
}
struct CatalogEntry {
    request: crate::LaunchSession,
    digest: [u8; 32],
    acknowledged: Option<u64>,
    used: u64,
}
struct Registration {
    session: Uuid,
    digest: [u8; 32],
    generation: u64,
    waiters: Vec<oneshot::Sender<Result<()>>>,
}
tokio::task_local! { pub(super) static CURRENT: Transport; }
pub(super) struct StopOnDrop(pub Transport);
impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.stop_tasks();
    }
}
impl Transport {
    pub fn new(host: Uuid, outbound: mpsc::Sender<TunnelFrame>) -> Self {
        Self {
            host,
            outbound,
            state: Arc::default(),
            tasks: Arc::default(),
            ledger: Arc::default(),
            routes: Arc::default(),
        }
    }
    pub fn set_ledger(&self, ledger: crate::command_ledger::CommandLedger) -> Result<()> {
        ensure!(
            self.ledger.set(ledger).is_ok(),
            "host command ledger already configured"
        );
        Ok(())
    }
    pub fn ledger(&self) -> Result<crate::command_ledger::CommandLedger> {
        self.ledger
            .get()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("host command ledger missing"))
    }
    pub fn set_routes(&self, routes: Vec<crate::auth::HostRoute>) {
        *self.routes.lock().unwrap() = routes;
    }
    pub async fn announce_routes(&self) -> Result<()> {
        let routes = self.routes.lock().unwrap().clone();
        self.outbound.send(TunnelFrame::Routes { routes }).await?;
        Ok(())
    }
    pub async fn register(
        &self,
        session_id: Uuid,
        mut request: crate::LaunchSession,
    ) -> Result<()> {
        use sha2::{Digest, Sha256};
        request.initial_prompt = None;
        let encoded = serde_json::to_vec(&request)?;
        ensure!(
            encoded.len() <= 1_048_000,
            "registration exceeds frame budget"
        );
        let digest: [u8; 32] = Sha256::digest(encoded).into();
        let (tx, rx) = oneshot::channel();
        let (registration_id, send) = {
            let mut state = self.state.lock().unwrap();
            state.clock = state.clock.saturating_add(1);
            let used = state.clock;
            let generation = state.generation;
            if let Some(entry) = state.catalog.get_mut(&session_id) {
                entry.used = used;
                if entry.digest == digest && entry.acknowledged == Some(generation) {
                    return Ok(());
                }
            } else if state.catalog.len() >= 128 {
                let oldest = state
                    .catalog
                    .iter()
                    .min_by_key(|(_, entry)| entry.used)
                    .map(|(id, _)| *id)
                    .unwrap();
                state.catalog.remove(&oldest);
            }
            state.catalog.insert(
                session_id,
                CatalogEntry {
                    request: request.clone(),
                    digest,
                    acknowledged: None,
                    used,
                },
            );
            let pending = state
                .pending
                .iter()
                .find(|(_, pending)| {
                    pending.session == session_id
                        && pending.digest == digest
                        && pending.generation == generation
                })
                .map(|(id, _)| id.clone());
            if let Some(id) = pending {
                let waiters = &mut state.pending.get_mut(&id).unwrap().waiters;
                ensure!(waiters.len() < 128, "registration waiter capacity exceeded");
                waiters.push(tx);
                (id, false)
            } else {
                ensure!(
                    state.pending.len() < 128,
                    "registration ingress capacity exceeded"
                );
                let id = Uuid::new_v4().to_string();
                state.pending.insert(
                    id.clone(),
                    Registration {
                        session: session_id,
                        digest,
                        generation,
                        waiters: vec![tx],
                    },
                );
                (id, true)
            }
        };
        let result = tokio::time::timeout(Duration::from_secs(15), async {
            if send {
                self.outbound
                    .send(TunnelFrame::RegisterSession {
                        registration_id: registration_id.clone(),
                        session_id: session_id.to_string(),
                        request,
                    })
                    .await?;
            }
            rx.await?
        })
        .await
        .map_err(anyhow::Error::from)
        .and_then(|result| result);
        if result.is_err() {
            if let Some(pending) = self.state.lock().unwrap().pending.remove(&registration_id) {
                for waiter in pending.waiters {
                    let _ = waiter.send(Err(anyhow::anyhow!(
                        "registration interrupted; retry metadata on the next connection"
                    )));
                }
            }
        }
        result
    }
    pub fn connection_lost(&self) {
        let mut state = self.state.lock().unwrap();
        state.generation = state.generation.wrapping_add(1);
        for entry in state.catalog.values_mut() {
            entry.acknowledged = None;
        }
        for (_, pending) in state.pending.drain() {
            for waiter in pending.waiters {
                let _ = waiter.send(Err(anyhow::anyhow!(
                    "relay disconnected during registration"
                )));
            }
        }
    }
    pub fn registrations(&self) -> Vec<(Uuid, crate::LaunchSession)> {
        self.state
            .lock()
            .unwrap()
            .catalog
            .iter()
            .map(|(id, entry)| (*id, entry.request.clone()))
            .collect()
    }
    pub fn stop_tasks(&self) {
        for task in self.tasks.lock().unwrap().drain(..) {
            task.abort();
        }
    }
    pub fn registered(&self, id: &str, error: Option<String>) {
        let mut state = self.state.lock().unwrap();
        if let Some(pending) = state.pending.remove(id) {
            if error.is_none() && state.generation == pending.generation {
                if let Some(entry) = state.catalog.get_mut(&pending.session) {
                    if entry.digest == pending.digest {
                        entry.acknowledged = Some(pending.generation);
                    }
                }
            }
            for waiter in pending.waiters {
                let _ = waiter.send(
                    error
                        .as_ref()
                        .map_or(Ok(()), |error| Err(anyhow::anyhow!(error.clone()))),
                );
            }
        }
    }
}
pub(super) fn get(host: Uuid) -> Result<Transport> {
    let transport = CURRENT
        .try_with(Clone::clone)
        .map_err(|_| anyhow::anyhow!("authenticated host tunnel is not running"))?;
    ensure!(transport.host == host, "host transport authority mismatch");
    Ok(transport)
}
pub(super) fn spawn<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: std::future::Future + Send + 'static,
    F::Output: Send + 'static,
{
    let transport = CURRENT.try_with(Clone::clone).ok();
    let inherited = transport.clone();
    let task = tokio::spawn(async move {
        match transport {
            Some(transport) => CURRENT.scope(transport, future).await,
            None => future.await,
        }
    });
    if let Some(transport) = inherited {
        let mut tasks = transport.tasks.lock().unwrap();
        tasks.retain(|task| !task.is_finished());
        tasks.push(task.abort_handle());
    }
    task
}

#[cfg(test)]
mod tests {
    use super::*;
    // Network acknowledgements must be reused within a connection and invalidated
    // on reconnect; a long archived roster must not permanently exhaust admission.
    #[tokio::test]
    async fn registration_ack_is_memoized_reconnectable_and_lru_bounded() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let (tx, mut rx) = mpsc::channel(128);
        let transport = Transport::new(Uuid::new_v4(), tx);
        let observed = Arc::new(AtomicUsize::new(0));
        let responder = {
            let transport = transport.clone();
            let observed = Arc::clone(&observed);
            tokio::spawn(async move {
                while let Some(frame) = rx.recv().await {
                    if let TunnelFrame::RegisterSession {
                        registration_id, ..
                    } = frame
                    {
                        observed.fetch_add(1, Ordering::SeqCst);
                        transport.registered(&registration_id, None);
                    }
                }
            })
        };
        let request: crate::LaunchSession = serde_json::from_value(serde_json::json!({
            "request_id":Uuid::new_v4(), "cwd":"/tmp", "provider":crate::CodingProvider::Codex,
            "permission_mode":crate::PermissionMode::Manual
        }))
        .unwrap();
        let session = Uuid::new_v4();
        transport.register(session, request.clone()).await.unwrap();
        transport.register(session, request.clone()).await.unwrap();
        assert_eq!(observed.load(Ordering::SeqCst), 1);
        transport.connection_lost();
        transport.register(session, request.clone()).await.unwrap();
        assert_eq!(observed.load(Ordering::SeqCst), 2);
        for _ in 0..129 {
            transport
                .register(Uuid::new_v4(), request.clone())
                .await
                .unwrap();
        }
        transport.register(session, request).await.unwrap();
        assert_eq!(observed.load(Ordering::SeqCst), 132);
        responder.abort();
        assert!(responder.await.unwrap_err().is_cancelled());
    }

    // A compiler check cannot prove spawned actors inherit socket authority or
    // that revocation cancels their work rather than leaving detached effects.
    #[tokio::test]
    async fn inherited_authority_and_terminal_cancellation() {
        let host = Uuid::new_v4();
        let (tx, _rx) = mpsc::channel(128);
        let transport = Transport::new(host, tx);
        CURRENT
            .scope(transport.clone(), async {
                spawn(async move {
                    assert!(get(host).is_ok());
                    assert!(get(Uuid::new_v4()).is_err());
                })
                .await
                .unwrap();
                let (started, ready) = oneshot::channel();
                let actor = spawn(async move {
                    started.send(()).unwrap();
                    std::future::pending::<()>().await;
                });
                ready.await.unwrap();
                transport.stop_tasks();
                assert!(actor.await.unwrap_err().is_cancelled());
            })
            .await;
        assert!(get(host).is_err());
    }
}
