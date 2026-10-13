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
    pub outbound: mpsc::Sender<TunnelFrame>,
    catalog: Arc<Mutex<HashMap<Uuid, crate::LaunchSession>>>,
    tasks: Arc<Mutex<Vec<tokio::task::AbortHandle>>>,
    registrations: Arc<Mutex<HashMap<String, oneshot::Sender<Result<()>>>>>,
}
tokio::task_local! { pub(super) static CURRENT: Transport; }
impl Transport {
    pub fn new(outbound: mpsc::Sender<TunnelFrame>) -> Self {
        Self {
            outbound,
            registrations: Arc::default(),
            catalog: Arc::default(),
            tasks: Arc::default(),
        }
    }
    pub async fn register(&self, session_id: Uuid, request: crate::LaunchSession) -> Result<()> {
        self.catalog
            .lock()
            .unwrap()
            .insert(session_id, request.clone());
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
pub(super) fn get(_host: Uuid) -> Result<Transport> {
    CURRENT
        .try_with(Clone::clone)
        .map_err(|_| anyhow::anyhow!("authenticated host tunnel is not running"))
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
