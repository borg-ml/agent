//! End-to-end encrypted collaboration wire primitives.
//!
//! The relay sees only the room id and opaque envelopes. Authority is checked
//! by the session-owning host after authenticated decryption.

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use anyhow::{Context, Result, bail};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use getrandom::fill;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::IsTerminal;
use std::path::Path;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, broadcast};
use tokio_tungstenite::tungstenite::Message;
use url::Url;
use uuid::Uuid;

use borg_remote::{
    ApprovalDecision, EventActor, HostCommand, MessageStatus, PromptDelivery, SessionEvent,
    SessionEventKind, SessionStore, SqliteSessionStore, default_host_config_path,
    send_local_session_command, session_control_socket_path,
};
use futures_util::{SinkExt, StreamExt};

use crate::cli::CollabCommand;

const PROTOCOL_VERSION: u8 = 1;
const MAX_PLAINTEXT_BYTES: usize = 1024 * 1024;
const MAX_RELAY_FRAME_BYTES: usize = MAX_PLAINTEXT_BYTES + 8 * 1024;
const REPLAY_WINDOW: usize = 16_384;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum CollabRole {
    View,
    Control,
}

#[derive(Debug, Clone)]
pub(crate) struct CollabSecrets {
    room_id: [u8; 16],
    key: [u8; 32],
    view_token: [u8; 32],
    control_token: [u8; 32],
}

#[derive(Debug, Clone)]
pub(crate) struct CollabLink {
    pub(crate) relay: Url,
    room_id: [u8; 16],
    key: [u8; 32],
    token: [u8; 32],
    pub(crate) role: CollabRole,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct EncryptedFrame {
    pub(crate) version: u8,
    pub(crate) room: String,
    pub(crate) epoch: u64,
    pub(crate) sequence: u64,
    pub(crate) frame_id: Uuid,
    pub(crate) nonce: String,
    pub(crate) ciphertext: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct AuthorizedFrame {
    pub(crate) operation_id: Uuid,
    pub(crate) token: String,
    pub(crate) body: serde_json::Value,
}

#[derive(Default)]
struct ReplayGuard {
    order: VecDeque<Uuid>,
    ids: HashSet<Uuid>,
}

impl ReplayGuard {
    fn admit(&mut self, id: Uuid) -> bool {
        if !self.ids.insert(id) {
            return false;
        }
        self.order.push_back(id);
        if self.order.len() > REPLAY_WINDOW
            && let Some(expired) = self.order.pop_front()
        {
            self.ids.remove(&expired);
        }
        true
    }
}

impl CollabSecrets {
    pub(crate) fn generate() -> Result<Self> {
        let mut secrets = Self {
            room_id: [0; 16],
            key: [0; 32],
            view_token: [0; 32],
            control_token: [0; 32],
        };
        fill(&mut secrets.room_id).context("failed to generate collaboration room id")?;
        fill(&mut secrets.key).context("failed to generate collaboration encryption key")?;
        fill(&mut secrets.view_token).context("failed to generate view capability")?;
        fill(&mut secrets.control_token).context("failed to generate control capability")?;
        Ok(secrets)
    }

    pub(crate) fn link(&self, relay: Url, role: CollabRole) -> CollabLink {
        CollabLink {
            relay,
            room_id: self.room_id,
            key: self.key,
            token: match role {
                CollabRole::View => self.view_token,
                CollabRole::Control => self.control_token,
            },
            role,
        }
    }

    pub(crate) fn authorizes(&self, role: CollabRole, token: &[u8]) -> bool {
        let expected = match role {
            CollabRole::View => &self.view_token,
            CollabRole::Control => &self.control_token,
        };
        constant_time_eq(&Sha256::digest(expected), &Sha256::digest(token))
    }
}

impl CollabLink {
    pub(crate) fn parse(value: &str) -> Result<Self> {
        let mut url = Url::parse(value).context("invalid collaboration link")?;
        anyhow::ensure!(
            matches!(url.scheme(), "http" | "https" | "ws" | "wss"),
            "collaboration links must use http, https, ws, or wss"
        );
        let fragment = url
            .fragment()
            .context("collaboration link is missing its encrypted capability")?
            .to_owned();
        url.set_fragment(None);
        let websocket_scheme = match url.scheme() {
            "http" => "ws",
            "https" => "wss",
            "ws" => "ws",
            "wss" => "wss",
            _ => unreachable!("scheme validated above"),
        };
        url.set_scheme(websocket_scheme)
            .map_err(|_| anyhow::anyhow!("invalid collaboration relay scheme"))?;
        let values: serde_json::Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(fragment)?)?;
        let room_id = decode_array::<16>(values.get("room"), "room")?;
        let key = decode_array::<32>(values.get("key"), "key")?;
        let token = decode_array::<32>(values.get("token"), "token")?;
        let role = match values.get("role").and_then(serde_json::Value::as_str) {
            Some("view") => CollabRole::View,
            Some("control") => CollabRole::Control,
            _ => bail!("collaboration link has an unknown role"),
        };
        Ok(Self {
            relay: url,
            room_id,
            key,
            token,
            role,
        })
    }

    pub(crate) fn expose(&self) -> Result<String> {
        let mut url = self.relay.clone();
        let browser_scheme = match url.scheme() {
            "ws" => "http",
            "wss" => "https",
            "http" => "http",
            "https" => "https",
            _ => unreachable!("collaboration link scheme validated"),
        };
        url.set_scheme(browser_scheme)
            .map_err(|_| anyhow::anyhow!("invalid collaboration browser scheme"))?;
        let fragment = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&serde_json::json!({
            "room": URL_SAFE_NO_PAD.encode(self.room_id),
            "key": URL_SAFE_NO_PAD.encode(self.key),
            "token": URL_SAFE_NO_PAD.encode(self.token),
            "role": match self.role {
                CollabRole::View => "view",
                CollabRole::Control => "control",
            },
        }))?);
        url.set_fragment(Some(&fragment));
        Ok(url.into())
    }

    pub(crate) fn seal(
        &self,
        epoch: u64,
        sequence: u64,
        operation_id: Uuid,
        body: serde_json::Value,
    ) -> Result<EncryptedFrame> {
        let plaintext = serde_json::to_vec(&AuthorizedFrame {
            operation_id,
            token: URL_SAFE_NO_PAD.encode(self.token),
            body,
        })?;
        anyhow::ensure!(
            plaintext.len() <= MAX_PLAINTEXT_BYTES,
            "collaboration frame exceeds the 1 MiB limit"
        );
        let mut nonce = [0_u8; 12];
        fill(&mut nonce).context("failed to generate collaboration frame nonce")?;
        let aad = aad(self.room_id, epoch, sequence);
        let cipher = Aes256Gcm::new_from_slice(&self.key).expect("AES-256 key length");
        let nonce = Nonce::try_from(nonce.as_slice()).expect("AES-GCM nonce length");
        let ciphertext = cipher
            .encrypt(
                &nonce,
                Payload {
                    msg: &plaintext,
                    aad: &aad,
                },
            )
            .map_err(|_| anyhow::anyhow!("failed to encrypt collaboration frame"))?;
        Ok(EncryptedFrame {
            version: PROTOCOL_VERSION,
            room: URL_SAFE_NO_PAD.encode(self.room_id),
            epoch,
            sequence,
            frame_id: Uuid::new_v4(),
            nonce: URL_SAFE_NO_PAD.encode(nonce),
            ciphertext: URL_SAFE_NO_PAD.encode(ciphertext),
        })
    }

    pub(crate) fn open(&self, frame: &EncryptedFrame) -> Result<AuthorizedFrame> {
        anyhow::ensure!(
            frame.version == PROTOCOL_VERSION,
            "unsupported collaboration protocol version"
        );
        anyhow::ensure!(
            frame.room == URL_SAFE_NO_PAD.encode(self.room_id),
            "frame targets another collaboration room"
        );
        let nonce = decode_array_text::<12>(&frame.nonce, "nonce")?;
        let ciphertext = URL_SAFE_NO_PAD.decode(&frame.ciphertext)?;
        anyhow::ensure!(
            ciphertext.len() <= MAX_PLAINTEXT_BYTES + 16,
            "collaboration frame exceeds the 1 MiB limit"
        );
        let cipher = Aes256Gcm::new_from_slice(&self.key).expect("AES-256 key length");
        let nonce = Nonce::try_from(nonce.as_slice()).expect("AES-GCM nonce length");
        let plaintext = cipher
            .decrypt(
                &nonce,
                Payload {
                    msg: &ciphertext,
                    aad: &aad(self.room_id, frame.epoch, frame.sequence),
                },
            )
            .map_err(|_| anyhow::anyhow!("collaboration frame authentication failed"))?;
        serde_json::from_slice(&plaintext).context("invalid collaboration frame payload")
    }
}

fn aad(room: [u8; 16], epoch: u64, sequence: u64) -> Vec<u8> {
    let mut aad = Vec::with_capacity(33);
    aad.push(PROTOCOL_VERSION);
    aad.extend_from_slice(&room);
    aad.extend_from_slice(&epoch.to_be_bytes());
    aad.extend_from_slice(&sequence.to_be_bytes());
    aad
}

fn decode_array<const N: usize>(value: Option<&serde_json::Value>, name: &str) -> Result<[u8; N]> {
    let text = value
        .and_then(serde_json::Value::as_str)
        .with_context(|| format!("collaboration link is missing {name}"))?;
    decode_array_text(text, name)
}

fn decode_array_text<const N: usize>(text: &str, name: &str) -> Result<[u8; N]> {
    URL_SAFE_NO_PAD
        .decode(text)?
        .try_into()
        .map_err(|_| anyhow::anyhow!("collaboration {name} has an invalid length"))
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .fold(0_u8, |difference, (left, right)| {
                difference | (left ^ right)
            })
            == 0
}

pub(crate) async fn run(command: CollabCommand) -> Result<()> {
    match command {
        CollabCommand::Relay { listen } => run_relay(&listen).await,
        CollabCommand::Host { session, relay } => host(session, &relay).await,
        CollabCommand::Join { link } => join(&link).await,
    }
}

async fn run_relay(listen: &str) -> Result<()> {
    let listener = TcpListener::bind(listen)
        .await
        .with_context(|| format!("failed to bind collaboration relay to {listen}"))?;
    let rooms = Arc::new(Mutex::new(
        HashMap::<String, broadcast::Sender<Message>>::new(),
    ));
    tracing::info!(%listen, "collaboration relay listening");
    loop {
        let (stream, peer) = listener.accept().await?;
        let rooms = Arc::clone(&rooms);
        tokio::spawn(async move {
            if let Err(error) = relay_connection(stream, rooms).await {
                tracing::warn!(%peer, %error, "collaboration relay client disconnected");
            }
        });
    }
}

async fn relay_connection(
    mut stream: TcpStream,
    rooms: Arc<Mutex<HashMap<String, broadcast::Sender<Message>>>>,
) -> Result<()> {
    let mut request = [0_u8; 8192];
    let request_len = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            let len = stream.peek(&mut request).await?;
            if request[..len]
                .windows(4)
                .any(|window| window == b"\r\n\r\n")
            {
                return Result::<usize>::Ok(len);
            }
            anyhow::ensure!(len < request.len(), "HTTP request headers exceed 8 KiB");
            tokio::task::yield_now().await;
        }
    })
    .await
    .context("collaboration handshake timed out")??;
    let request_text = String::from_utf8_lossy(&request[..request_len]).to_ascii_lowercase();
    if !request_text.contains("upgrade: websocket") {
        serve_browser(&mut stream).await?;
        return Ok(());
    }
    let websocket = tokio_tungstenite::accept_async(stream).await?;
    let (mut sink, mut source) = websocket.split();
    let room = source
        .next()
        .await
        .context("client disconnected before room handshake")??
        .into_text()
        .context("room handshake must be text")?
        .to_string();
    validate_room(&room)?;
    let sender = {
        let mut rooms = rooms.lock().await;
        rooms
            .entry(room)
            .or_insert_with(|| broadcast::channel(256).0)
            .clone()
    };
    let mut receiver = sender.subscribe();
    loop {
        tokio::select! {
            incoming = source.next() => {
                let Some(incoming) = incoming else { return Ok(()) };
                let message = incoming?;
                if message.len() > MAX_RELAY_FRAME_BYTES {
                    bail!("relay frame exceeds configured limit");
                }
                if matches!(message, Message::Text(_) | Message::Binary(_)) {
                    let _ = sender.send(message);
                }
            }
            outgoing = receiver.recv() => match outgoing {
                Ok(message) => sink.send(message).await?,
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    bail!("relay consumer lagged; reconnect from its durable cursor")
                }
                Err(broadcast::error::RecvError::Closed) => return Ok(()),
            }
        }
    }
}

async fn serve_browser(stream: &mut TcpStream) -> Result<()> {
    let mut request = vec![0_u8; 8192];
    let read = stream.read(&mut request).await?;
    anyhow::ensure!(read > 0, "empty HTTP request");
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\n\
         Content-Security-Policy: default-src 'self'; script-src 'unsafe-inline'; \
         connect-src ws: wss:; style-src 'unsafe-inline'\r\n\
         Referrer-Policy: no-referrer\r\nCache-Control: no-store\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{}",
        COLLAB_BROWSER.len(),
        COLLAB_BROWSER
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await?;
    Ok(())
}

async fn host(session_id: Uuid, relay: &str) -> Result<()> {
    let relay = Url::parse(relay).context("invalid collaboration relay URL")?;
    anyhow::ensure!(
        matches!(relay.scheme(), "ws" | "wss"),
        "relay must use ws or wss"
    );
    let sessions_dir = default_host_config_path()
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("sessions");
    let store = Arc::new(SqliteSessionStore::open(sessions_dir.join("sessions.sqlite3")).await?);
    anyhow::ensure!(
        store.contains_session(session_id).await?,
        "local session {session_id} does not exist"
    );
    let socket = session_control_socket_path(&sessions_dir, session_id);
    anyhow::ensure!(
        socket.exists(),
        "session {session_id} is not active; resume it before sharing"
    );
    let secrets = CollabSecrets::generate()?;
    let view = secrets.link(relay.clone(), CollabRole::View);
    let control = secrets.link(relay.clone(), CollabRole::Control);
    let view_url = view.expose()?;
    let control_url = control.expose()?;
    println!("View: {view_url}");
    println!("Control: {control_url}");
    if std::io::stdout().is_terminal() {
        println!("\nControl QR:\n{}", terminal_qr(&control_url)?);
    }

    let (websocket, _) = tokio_tungstenite::connect_async(relay.as_str()).await?;
    let (mut sink, mut source) = websocket.split();
    sink.send(Message::Text(
        URL_SAFE_NO_PAD.encode(secrets.room_id).into(),
    ))
    .await?;
    let hello = view.seal(
        0,
        0,
        Uuid::new_v4(),
        serde_json::json!({"type": "hello", "session_id": session_id}),
    )?;
    sink.send(Message::Text(serde_json::to_string(&hello)?.into()))
        .await?;
    let mut last_sequence = 0;
    let mut refresh = tokio::time::interval(std::time::Duration::from_millis(100));
    let mut frames = ReplayGuard::default();
    let mut operations = ReplayGuard::default();
    loop {
        tokio::select! {
            _ = refresh.tick() => {
                for event in store.events_after(session_id, last_sequence, 1_000).await? {
                    last_sequence = event.sequence;
                    let frame = view.seal(
                        0,
                        event.sequence,
                        event.id,
                        serde_json::json!({"type": "event", "event": event}),
                    )?;
                    sink.send(Message::Text(serde_json::to_string(&frame)?.into())).await?;
                }
            }
            incoming = source.next() => {
                let Some(incoming) = incoming else { bail!("collaboration relay disconnected") };
                let Message::Text(text) = incoming? else { continue };
                let Ok(frame) = serde_json::from_str::<EncryptedFrame>(&text) else { continue };
                if frame.epoch != 0 || !frames.admit(frame.frame_id) {
                    continue;
                }
                let Ok(authorized) = control.open(&frame) else { continue };
                if !operations.admit(authorized.operation_id) {
                    continue;
                }
                let Ok(token) = URL_SAFE_NO_PAD.decode(&authorized.token) else { continue };
                if !secrets.authorizes(CollabRole::Control, &token)
                    || authorized.body.get("type").and_then(serde_json::Value::as_str)
                        != Some("command")
                {
                    continue;
                }
                let command: HostCommand = serde_json::from_value(
                    authorized.body.get("command").cloned().context("missing command")?
                )?;
                anyhow::ensure!(
                    command.session_id() == Some(session_id),
                    "collaboration command targets another session"
                );
                anyhow::ensure!(
                    matches!(
                        command,
                        HostCommand::Prompt { .. }
                            | HostCommand::Interrupt { .. }
                            | HostCommand::Approve { .. }
                    ),
                    "collaboration capability does not authorize this command"
                );
                tracing::info!(
                    operation_id = %authorized.operation_id,
                    %session_id,
                    capability = "control",
                    command = match &command {
                        HostCommand::Prompt { .. } => "prompt",
                        HostCommand::Interrupt { .. } => "interrupt",
                        HostCommand::Approve { .. } => "approve",
                        _ => unreachable!("command allowlist checked"),
                    },
                    "collaboration command admitted"
                );
                send_local_session_command(&socket, session_id, command).await?;
            }
        }
    }
}

async fn join(value: &str) -> Result<()> {
    let link = CollabLink::parse(value)?;
    let (websocket, _) = tokio_tungstenite::connect_async(link.relay.as_str()).await?;
    let (mut sink, mut source) = websocket.split();
    sink.send(Message::Text(URL_SAFE_NO_PAD.encode(link.room_id).into()))
        .await?;
    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    let mut session_id = None;
    let mut pending_approval = None;
    let mut sequence = 0_u64;
    let mut seen = ReplayGuard::default();
    loop {
        tokio::select! {
            incoming = source.next() => {
                let Some(incoming) = incoming else { bail!("collaboration relay disconnected") };
                let Message::Text(text) = incoming? else { continue };
                let frame: EncryptedFrame = serde_json::from_str(&text)?;
                if frame.epoch != 0 || !seen.admit(frame.frame_id) {
                    continue;
                }
                let authorized = link.open(&frame)?;
                match authorized.body.get("type").and_then(serde_json::Value::as_str) {
                    Some("hello") => {
                        session_id = authorized.body.get("session_id")
                            .and_then(serde_json::Value::as_str)
                            .and_then(|value| Uuid::parse_str(value).ok());
                    }
                    Some("event") => {
                        let event: SessionEvent = serde_json::from_value(
                            authorized.body.get("event").cloned().context("missing event")?
                        )?;
                        match &event.kind {
                            SessionEventKind::ApprovalRequested { approval_id, .. } => {
                                pending_approval = Some(approval_id.clone());
                            }
                            SessionEventKind::ApprovalResolved { approval_id, .. }
                                if pending_approval.as_deref() == Some(approval_id) =>
                            {
                                pending_approval = None;
                            }
                            _ => {}
                        }
                        render_event(&event);
                    }
                    _ => {}
                }
            }
            line = lines.next_line(), if link.role == CollabRole::Control => {
                let Some(line) = line? else { return Ok(()) };
                let id = session_id.context("waiting for collaboration host metadata")?;
                let command = match line.trim() {
                    "/interrupt" | "/stop" => HostCommand::Interrupt { session_id: id },
                    "/approve" => HostCommand::Approve {
                        session_id: id,
                        approval_id: pending_approval.clone().context("no approval is pending")?,
                        decision: ApprovalDecision::AllowOnce,
                    },
                    "/approve-session" => HostCommand::Approve {
                        session_id: id,
                        approval_id: pending_approval.clone().context("no approval is pending")?,
                        decision: ApprovalDecision::AllowSession,
                    },
                    "/deny" => HostCommand::Approve {
                        session_id: id,
                        approval_id: pending_approval.clone().context("no approval is pending")?,
                        decision: ApprovalDecision::Deny,
                    },
                    text if !text.is_empty() => HostCommand::Prompt {
                        session_id: id,
                        message_id: Uuid::new_v4(),
                        text: text.to_owned(),
                        attachments: Vec::new(),
                        output_schema: None,
                        delivery: PromptDelivery::Queue,
                    },
                    _ => continue,
                };
                sequence = sequence.saturating_add(1);
                let frame = link.seal(
                    0,
                    sequence,
                    Uuid::new_v4(),
                    serde_json::json!({"type": "command", "command": command}),
                )?;
                sink.send(Message::Text(serde_json::to_string(&frame)?.into())).await?;
            }
        }
    }
}

fn render_event(event: &SessionEvent) {
    match &event.kind {
        SessionEventKind::Message {
            actor,
            text,
            status: MessageStatus::Complete,
            ..
        } => println!(
            "{}: {text}",
            match actor {
                EventActor::User => "you",
                EventActor::Assistant => "borg",
                EventActor::Tool => "tool",
                EventActor::System => "system",
            }
        ),
        SessionEventKind::ApprovalRequested { title, detail, .. } => {
            println!("approval required: {title} — {detail}");
        }
        SessionEventKind::Error { message } => eprintln!("error: {message}"),
        _ => {}
    }
}

fn validate_room(room: &str) -> Result<()> {
    anyhow::ensure!(URL_SAFE_NO_PAD.decode(room)?.len() == 16, "invalid room id");
    Ok(())
}

fn terminal_qr(value: &str) -> Result<String> {
    use qrcode::QrCode;
    use qrcode::render::unicode;

    Ok(QrCode::new(value.as_bytes())?
        .render::<unicode::Dense1x2>()
        .quiet_zone(true)
        .build())
}

const COLLAB_BROWSER: &str = r#"<!doctype html>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Borg collaboration</title>
<style>
body{margin:0;background:#160f13;color:#eee;font:15px ui-monospace,monospace}
main{max-width:900px;margin:auto;padding:24px}h1{color:#ff9b68;font-size:20px}
#log{white-space:pre-wrap;line-height:1.45;border:1px solid #503642;padding:16px;min-height:50vh}
form{display:flex;margin-top:12px}input{flex:1;background:#21171c;color:#fff;border:1px solid #70505e;padding:12px}
button{background:#ff9b68;border:0;padding:0 18px}small{color:#bbaab2}
</style>
<main><h1>Borg live collaboration</h1><small id="status">Connecting…</small>
<div id="log"></div><form id="form"><input id="prompt" autocomplete="off" placeholder="Send a follow-up"><button>Send</button></form></main>
<script>
const dec=s=>Uint8Array.from(atob(s.replace(/-/g,'+').replace(/_/g,'/').padEnd(Math.ceil(s.length/4)*4,'=')),c=>c.charCodeAt(0));
const enc=b=>btoa(String.fromCharCode(...new Uint8Array(b))).replace(/\+/g,'-').replace(/\//g,'_').replace(/=+$/,'');
const cfg=JSON.parse(new TextDecoder().decode(dec(location.hash.slice(1))));
const room=dec(cfg.room), token=cfg.token, control=cfg.role==='control';
const status=document.querySelector('#status'), log=document.querySelector('#log'), form=document.querySelector('#form'), input=document.querySelector('#prompt');
if(!control){form.hidden=true;status.textContent='Read-only · connecting…'}
let session=null, pendingApproval=null, sequence=0;
const keyPromise=crypto.subtle.importKey('raw',dec(cfg.key),'AES-GCM',false,['encrypt','decrypt']);
function aad(epoch,seq){const b=new ArrayBuffer(33),v=new DataView(b);new Uint8Array(b,1,16).set(room);v.setUint8(0,1);v.setBigUint64(17,BigInt(epoch));v.setBigUint64(25,BigInt(seq));return b}
function line(text){log.textContent+=text+'\\n';scrollTo(0,document.body.scrollHeight)}
const scheme=location.protocol==='https:'?'wss:':'ws:', ws=new WebSocket(scheme+'//'+location.host+location.pathname);
ws.onopen=()=>{ws.send(cfg.room);status.textContent=(control?'Control':'Read-only')+' · connected'};
ws.onclose=()=>status.textContent='Disconnected';
ws.onmessage=async e=>{try{const f=JSON.parse(e.data),key=await keyPromise;
 const plain=await crypto.subtle.decrypt({name:'AES-GCM',iv:dec(f.nonce),additionalData:aad(f.epoch,f.sequence)},key,dec(f.ciphertext));
 const msg=JSON.parse(new TextDecoder().decode(plain)),b=msg.body;
 if(b.type==='hello')session=b.session_id;
 if(b.type==='event'){const k=b.event.kind;if(k.type==='message'&&k.status==='complete')line((k.actor==='assistant'?'borg':k.actor)+': '+k.text);
  else if(k.type==='approval_requested'){pendingApproval=k.approval_id;line('approval required: '+k.title+' — '+k.detail+'\\nType /approve, /approve-session, or /deny')}
  else if(k.type==='approval_resolved'&&k.approval_id===pendingApproval)pendingApproval=null;
  else if(k.type==='error')line('error: '+k.message)}
 }catch(_){status.textContent='Rejected an invalid encrypted frame'}};
form.onsubmit=async e=>{e.preventDefault();if(!session||!input.value.trim())return;sequence++;
 const approval={'/approve':'allow_once','/approve-session':'allow_session','/deny':'deny'}[input.value.trim()];
 if(approval&&!pendingApproval){line('no approval is pending');return}
 const command=approval?{type:'approve',session_id:session,approval_id:pendingApproval,decision:approval}:{type:'prompt',session_id:session,message_id:crypto.randomUUID(),text:input.value,attachments:[],output_schema:null,delivery:'queue'};
 const operation=crypto.randomUUID(), body={type:'command',command};
 const plain=new TextEncoder().encode(JSON.stringify({operation_id:operation,token,body})),nonce=crypto.getRandomValues(new Uint8Array(12)),key=await keyPromise;
 const cipher=await crypto.subtle.encrypt({name:'AES-GCM',iv:nonce,additionalData:aad(0,sequence)},key,plain);
 ws.send(JSON.stringify({version:1,room:cfg.room,epoch:0,sequence,frame_id:crypto.randomUUID(),nonce:enc(nonce),ciphertext:enc(cipher)}));input.value=''};
</script>"#;

#[cfg(test)]
mod tests {
    use super::*;

    fn relay() -> Url {
        Url::parse("wss://relay.example.test/collab").unwrap()
    }

    #[test]
    fn links_round_trip_without_sending_secrets_to_the_relay() {
        let secrets = CollabSecrets::generate().unwrap();
        let link = secrets.link(relay(), CollabRole::View);
        let exposed = link.expose().unwrap();
        assert!(exposed.starts_with("https://"));
        let public = exposed.split('#').next().unwrap();
        assert!(!public.contains(&URL_SAFE_NO_PAD.encode(link.key)));
        let parsed = CollabLink::parse(&exposed).unwrap();
        assert_eq!(parsed.role, CollabRole::View);
        assert_eq!(parsed.room_id, link.room_id);
        assert_eq!(parsed.key, link.key);
        assert_eq!(parsed.token, link.token);
    }

    #[test]
    fn frames_are_authenticated_against_room_epoch_and_sequence() {
        let secrets = CollabSecrets::generate().unwrap();
        let link = secrets.link(relay(), CollabRole::Control);
        let frame = link
            .seal(7, 19, Uuid::new_v4(), serde_json::json!({"prompt": "hi"}))
            .unwrap();
        assert_eq!(link.open(&frame).unwrap().body["prompt"], "hi");

        let mut tampered = frame.clone();
        tampered.sequence += 1;
        assert!(link.open(&tampered).is_err());
        tampered = frame;
        tampered.epoch += 1;
        assert!(link.open(&tampered).is_err());
    }

    #[test]
    fn view_and_control_capabilities_do_not_escalate() {
        let secrets = CollabSecrets::generate().unwrap();
        let view = secrets.link(relay(), CollabRole::View);
        let control = secrets.link(relay(), CollabRole::Control);
        assert!(secrets.authorizes(CollabRole::View, &view.token));
        assert!(secrets.authorizes(CollabRole::Control, &control.token));
        assert!(!secrets.authorizes(CollabRole::Control, &view.token));
        assert!(!secrets.authorizes(CollabRole::View, &control.token));
    }

    #[test]
    fn wrong_keys_and_oversize_payloads_fail_closed() {
        let first = CollabSecrets::generate().unwrap();
        let second = CollabSecrets::generate().unwrap();
        let frame = first
            .link(relay(), CollabRole::View)
            .seal(0, 1, Uuid::new_v4(), serde_json::json!({"ok": true}))
            .unwrap();
        assert!(second.link(relay(), CollabRole::View).open(&frame).is_err());
        let huge = "x".repeat(MAX_PLAINTEXT_BYTES);
        assert!(
            first
                .link(relay(), CollabRole::Control)
                .seal(0, 2, Uuid::new_v4(), serde_json::json!({"text": huge}))
                .is_err()
        );
    }

    #[tokio::test]
    async fn relay_routes_opaque_frames_only_within_the_selected_room() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let rooms = Arc::new(Mutex::new(
            HashMap::<String, broadcast::Sender<Message>>::new(),
        ));
        let server_rooms = Arc::clone(&rooms);
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (stream, _) = listener.accept().await.unwrap();
                let rooms = Arc::clone(&server_rooms);
                tokio::spawn(async move {
                    relay_connection(stream, rooms).await.unwrap();
                });
            }
        });
        let url = format!("ws://{address}");
        let (mut first, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        let (mut second, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        let room = URL_SAFE_NO_PAD.encode([7_u8; 16]);
        first
            .send(Message::Text(room.clone().into()))
            .await
            .unwrap();
        second.send(Message::Text(room.into())).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        first
            .send(Message::Text("opaque-ciphertext".into()))
            .await
            .unwrap();
        let received = tokio::time::timeout(std::time::Duration::from_secs(2), second.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(received.into_text().unwrap(), "opaque-ciphertext");
        server.await.unwrap();
    }

    #[tokio::test]
    async fn relay_serves_a_no_store_browser_client_without_link_secrets() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let rooms = Arc::new(Mutex::new(
            HashMap::<String, broadcast::Sender<Message>>::new(),
        ));
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            relay_connection(stream, rooms).await.unwrap();
        });
        let mut client = TcpStream::connect(address).await.unwrap();
        client
            .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        client.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("Cache-Control: no-store"));
        assert!(response.contains("Borg live collaboration"));
        assert!(!response.contains("control_token"));
        server.await.unwrap();
    }
}
