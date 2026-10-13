use crate::auth::TunnelFrame;
use anyhow::{Result, ensure};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

#[derive(Clone)]
pub(super) struct Transport {
    host: Uuid,
    pub outbound: mpsc::Sender<TunnelFrame>,
    catalog: Arc<Mutex<HashMap<Uuid, crate::LaunchSession>>>,
    tasks: Arc<Mutex<Vec<tokio::task::AbortHandle>>>,
    registrations: Arc<Mutex<HashMap<String, oneshot::Sender<Result<()>>>>>,
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
            registrations: Arc::default(),
            catalog: Arc::default(),
            tasks: Arc::default(),
        }
    }
    pub async fn register(&self, session_id: Uuid, request: crate::LaunchSession) -> Result<()> {
        {
            let mut catalog = self.catalog.lock().unwrap();
            ensure!(
                catalog.contains_key(&session_id) || catalog.len() < 128,
                "host registration capacity exceeded"
            );
            catalog.insert(session_id, request.clone());
        }
        let registration_id = Uuid::new_v4().to_string();
        let (tx, rx) = oneshot::channel();
        self.registrations
            .lock()
            .unwrap()
            .insert(registration_id.clone(), tx);
        let result = async {
            self.outbound
                .send(TunnelFrame::RegisterSession {
                    registration_id: registration_id.clone(),
                    session_id: session_id.to_string(),
                    request,
                })
                .await?;
            tokio::time::timeout(Duration::from_secs(15), rx).await???;
            Ok(())
        }
        .await;
        self.registrations.lock().unwrap().remove(&registration_id);
        result
    }
    pub fn registrations(&self) -> Vec<TunnelFrame> {
        self.catalog
            .lock()
            .unwrap()
            .iter()
            .map(|(id, request)| TunnelFrame::RegisterSession {
                registration_id: Uuid::new_v4().to_string(),
                session_id: id.to_string(),
                request: request.clone(),
            })
            .collect()
    }
    pub fn stop_tasks(&self) {
        for task in self.tasks.lock().unwrap().drain(..) {
            task.abort();
        }
    }
    pub fn registered(&self, id: &str, error: Option<String>) {
        if let Some(tx) = self.registrations.lock().unwrap().remove(id) {
            let _ = tx.send(error.map_or(Ok(()), |error| Err(anyhow::anyhow!(error))));
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
