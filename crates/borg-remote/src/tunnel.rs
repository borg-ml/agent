//! One authenticated outbound socket; TLS uses system trust roots. No credential
//! URLs, HTTP event upload, or command polling belongs to this transport.
use crate::{
    HostConfig,
    auth::{Authentication, TunnelFrame},
};
use anyhow::{Result, bail, ensure};
use futures::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

pub const TERMINAL_AUTH_EXIT: i32 = 78;
#[derive(Debug)]
pub struct TerminalAuthentication(pub String);
impl std::fmt::Display for TerminalAuthentication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for TerminalAuthentication {}

pub async fn connect(
    config: &HostConfig,
    inbound: mpsc::Sender<TunnelFrame>,
    outbound: &mut mpsc::Receiver<TunnelFrame>,
) -> Result<()> {
    let mut url = url::Url::parse(&config.server)?;
    ensure!(
        url.scheme() == "https",
        "host tunnel requires verified HTTPS/WSS"
    );
    ensure!(
        url.username().is_empty() && url.password().is_none(),
        "credentials forbidden in host route"
    );
    url.set_scheme("wss")
        .map_err(|_| anyhow::anyhow!("invalid WSS route"))?;
    url.set_path("/api/remote/ws/host");
    url.set_query(None);
    url.set_fragment(None);
    let (socket, _) = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        tokio_tungstenite::connect_async(url.as_str()),
    )
    .await??;
    let (mut sink, mut stream) = socket.split();
    let auth = serde_json::to_string(&Authentication::Host {
        host_id: config.host_id.to_string(),
        token: crate::host::host_token(config),
    })?;
    sink.send(Message::Text(auth.into())).await?;
    let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(5));
    let mut last_received = tokio::time::Instant::now();
    loop {
        tokio::select! {
            incoming=stream.next()=> {
                let Some(incoming)=incoming else {bail!("host socket closed")};
                match incoming? {
                    Message::Text(text)=> {
                        ensure!(text.len()<=1_048_576,"host inbound frame exceeds budget");
                        let frame: TunnelFrame=serde_json::from_str(&text)?;
                        last_received=tokio::time::Instant::now();
                        match &frame {
                            TunnelFrame::Revoked { .. } => return Err(TerminalAuthentication("host enrollment revoked".into()).into()),
                            TunnelFrame::AuthenticationRejected { reason } => return Err(TerminalAuthentication(reason.clone()).into()),
                            _ => {}
                        }
                        // Never let actor work block socket pings or receiver work.
                        inbound.try_send(frame).map_err(|_|anyhow::anyhow!("host ingress overflow: reconnect from durable receipts"))?;
                    }
                    Message::Ping(payload)=> {last_received=tokio::time::Instant::now();sink.send(Message::Pong(payload)).await?;}
                    Message::Pong(_)=> {last_received=tokio::time::Instant::now();}
                    Message::Close(_)=> bail!("host socket closed"),
                    Message::Binary(_)=> bail!("binary host frame unsupported"),
                    _=>{}
                }
            }
            frame=outbound.recv()=> {
                let Some(frame)=frame else {return Ok(())};
                let encoded=serde_json::to_string(&frame)?;
                ensure!(encoded.len()<=1_048_576,"host outbound frame exceeds budget");
                tokio::time::timeout(std::time::Duration::from_secs(10),sink.send(Message::Text(encoded.into()))).await??;
            }
            _=heartbeat.tick()=> {
                ensure!(last_received.elapsed()<std::time::Duration::from_secs(15),"host socket heartbeat deadline");
                sink.send(Message::Ping(Vec::new().into())).await?;
            }
        }
    }
}
