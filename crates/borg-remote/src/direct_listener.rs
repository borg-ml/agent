use crate::{auth::Authentication, sync::SyncAuthority};
use anyhow::{Context, Result, ensure};
use axum::{
    Json, Router,
    extract::{
        RawQuery, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use ed25519_dalek::VerifyingKey;
use futures::{SinkExt, StreamExt};
use rustls::{
    RootCertStore,
    client::danger::ServerCertVerifier,
    pki_types::{ServerName, UnixTime},
};
use serde::{Deserialize, Serialize};
use std::{io::BufReader, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::{Semaphore, mpsc, watch};
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DirectListenerConfig {
    pub bind_address: SocketAddr,
    pub server_name: String,
    pub certificate_chain: PathBuf,
    pub private_key: PathBuf,
}

#[derive(Clone)]
struct ListenerState {
    host: Uuid,
    issuer: VerifyingKey,
    authority: Arc<SyncAuthority>,
    trust: Arc<crate::trust::DeviceTrust>,
    loopback: bool,
    route: String,
    connections: Arc<Semaphore>,
}

fn tls_config(config: &DirectListenerConfig) -> Result<Arc<rustls::ServerConfig>> {
    let mut certificates = BufReader::new(std::fs::File::open(&config.certificate_chain)?);
    let certificates =
        rustls_pemfile::certs(&mut certificates).collect::<std::io::Result<Vec<_>>>()?;
    ensure!(
        !certificates.is_empty(),
        "direct listener certificate chain is empty"
    );
    let name = ServerName::try_from(config.server_name.clone())
        .context("invalid direct listener DNS name")?;
    ensure!(
        matches!(name, ServerName::DnsName(_)),
        "direct listener requires a DNS server name"
    );
    let mut roots = RootCertStore::empty();
    let native = rustls_native_certs::load_native_certs();
    for certificate in native.certs {
        roots.add(certificate)?;
    }
    ensure!(
        !roots.is_empty(),
        "no native trust roots available for direct listener"
    );
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let verifier = rustls::client::WebPkiServerVerifier::builder_with_provider(
        Arc::new(roots),
        provider.clone(),
    )
    .build()?;
    verifier
        .verify_server_cert(
            &certificates[0],
            &certificates[1..],
            &name,
            &[],
            UnixTime::now(),
        )
        .context(
            "direct listener certificate is not trusted for configured DNS name and current time",
        )?;
    let mut key = BufReader::new(std::fs::File::open(&config.private_key)?);
    let key =
        rustls_pemfile::private_key(&mut key)?.context("direct listener private key is missing")?;
    let tls = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(certificates, key)?;
    Ok(Arc::new(tls))
}

pub(super) fn start(
    config: DirectListenerConfig,
    host: Uuid,
    issuer: VerifyingKey,
    authority: Arc<SyncAuthority>,
    trust: Arc<crate::trust::DeviceTrust>,
) -> Result<tokio::task::JoinHandle<Result<()>>> {
    ensure!(
        config.bind_address.port() != 0,
        "direct listener requires an explicit nonzero port"
    );
    let loopback = config.bind_address.ip().is_loopback();
    ensure!(
        loopback || !trust.devices().is_empty(),
        "direct listener on a non-loopback address requires pinned trusted devices (borg remote devices trust)"
    );
    let tls = tls_config(&config)?;
    let listener = std::net::TcpListener::bind(config.bind_address)
        .context("failed to bind verified direct listener")?;
    listener.set_nonblocking(true)?;
    let route = format!(
        "wss://{}:{}/ws",
        config.server_name,
        config.bind_address.port()
    );
    super::host_transport::get(host)?.set_routes(vec![crate::auth::HostRoute {
        url: route.clone(),
        server_name: config.server_name.clone(),
        protocol: borg_view::wire::PROTOCOL_VERSION,
    }]);
    let router = Router::new()
        .route("/.well-known/borg-remote", get(descriptor))
        .route("/preflight", get(preflight))
        .route("/ws", get(upgrade))
        .with_state(ListenerState {
            host,
            issuer,
            authority,
            trust,
            loopback,
            route,
            connections: Arc::new(Semaphore::new(128)),
        });
    Ok(super::spawn_transport_task(async move {
        axum_server::from_tcp_rustls(
            listener,
            axum_server::tls_rustls::RustlsConfig::from_config(tls),
        )
        .serve(router.into_make_service())
        .await
        .context("direct listener stopped")
    }))
}

async fn descriptor(State(state): State<ListenerState>, RawQuery(query): RawQuery) -> Response {
    if query.is_some() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    public_response(
        Json(
            serde_json::json!({"protocol":2,"host_id":state.host.to_string(),"route":state.route}),
        )
        .into_response(),
    )
}

fn public_response(mut response: Response) -> Response {
    response.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    response
}

async fn preflight(RawQuery(query): RawQuery) -> Response {
    if query.is_some() {
        StatusCode::BAD_REQUEST.into_response()
    } else {
        public_response(StatusCode::NO_CONTENT.into_response())
    }
}

async fn upgrade(
    State(state): State<ListenerState>,
    RawQuery(query): RawQuery,
    ws: WebSocketUpgrade,
) -> Response {
    if query.is_some() {
        return StatusCode::BAD_REQUEST.into_response();
    }
    let Ok(permit) = state.connections.clone().try_acquire_owned() else {
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    };
    ws.max_message_size(1_048_576)
        .max_frame_size(1_048_576)
        .on_upgrade(move |socket| async move {
            let _permit = permit;
            connection(socket, state).await;
        })
        .into_response()
}

async fn connection(mut socket: WebSocket, state: ListenerState) {
    let mut audited = None;
    let admission = async {
        let frame = tokio::time::timeout(Duration::from_secs(5), socket.recv())
            .await?
            .context("direct authentication frame missing")??;
        let Message::Text(text) = frame else {
            anyhow::bail!("direct authentication must be first text frame")
        };
        let Authentication::Client {
            ticket,
            nonce,
            proof,
        } = serde_json::from_str(&text)?
        else {
            anyhow::bail!("direct listener accepts client tickets only")
        };
        let claims = crate::admission::verify_ticket(
            &ticket,
            &state.issuer,
            &state.host.to_string(),
            state.authority.current_epoch(),
            chrono::Utc::now().timestamp(),
        )?;
        audited = Some(claims.clone());
        crate::admission::verify_device_proof(&claims, &nonce, &proof)?;
        let devices = state.trust.devices();
        if devices.is_empty() {
            // Trust can be removed while a public listener runs.
            ensure!(
                state.loopback,
                "non-loopback direct listener requires pinned trusted devices"
            );
        } else {
            crate::trust::ensure_trusted(&devices, &claims.device_public_key)?;
        }
        state.authority.consume_client_ticket(&claims).await?;
        Ok::<_, anyhow::Error>(claims)
    }
    .await;
    crate::trust::audit_admission(
        &state.trust.audit_path(),
        "direct",
        audited.as_ref(),
        admission.as_ref().err(),
    );
    let claims = match admission {
        Ok(claims) => claims,
        Err(error) => {
            tracing::debug!(%error, "direct client authentication rejected");
            let _ = socket.send(Message::Close(None)).await;
            return;
        }
    };
    let id = Uuid::new_v4().to_string();
    let (outbound, mut frames) = mpsc::channel(128);
    if state
        .authority
        .open_direct_connection(id.clone(), claims, outbound)
        .await
        .is_err()
    {
        let _ = socket.send(Message::Close(None)).await;
        return;
    }
    let (mut sink, mut stream) = socket.split();
    let (received, last_received) = watch::channel(tokio::time::Instant::now());
    let reader = async {
        while let Some(frame) = stream.next().await {
            received.send_replace(tokio::time::Instant::now());
            match frame? {
                Message::Text(text) => {
                    ensure!(text.len() <= 1_048_576, "direct frame exceeds budget");
                    state
                        .authority
                        .handle(&id, serde_json::from_str(&text)?)
                        .await?;
                }
                Message::Ping(_) | Message::Pong(_) => {}
                Message::Close(_) => break,
                Message::Binary(_) => anyhow::bail!("binary direct frames unsupported"),
            }
        }
        Ok::<_, anyhow::Error>(())
    };
    let writer = async {
        let mut heartbeat = tokio::time::interval(Duration::from_secs(5));
        loop {
            tokio::select! {
                frame = frames.recv() => {
                    let Some(frame) = frame else { break };
                    let text = serde_json::to_string(&frame)?;
                    ensure!(text.len() <= 1_048_576, "direct output exceeds frame budget");
                    tokio::time::timeout(Duration::from_secs(10), sink.send(Message::Text(text.into()))).await??;
                }
                _ = heartbeat.tick() => {
                    ensure!(last_received.borrow().elapsed() < Duration::from_secs(15), "direct heartbeat expired");
                    tokio::time::timeout(Duration::from_secs(10), sink.send(Message::Ping(Vec::new().into()))).await??;
                }
            }
        }
        Ok::<_, anyhow::Error>(())
    };
    let result = tokio::select! { result = reader => result, result = writer => result };
    state.authority.close_connection(&id).await;
    if let Err(error) = result {
        tracing::debug!(%error, connection_id=%id, "direct client disconnected");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // Serving a self-signed leaf must fail before accepting clients. A compiler
    // check cannot enforce the native-root trust boundary during TLS setup.
    #[test]
    fn native_roots_reject_self_signed_listener() {
        let root = tempfile::tempdir().unwrap();
        let chain = root.path().join("certificate.pem");
        std::fs::write(&chain, SELF_SIGNED_CERTIFICATE).unwrap();
        let config = DirectListenerConfig {
            bind_address: "127.0.0.1:9443".parse().unwrap(),
            server_name: "borg-direct-test.invalid".into(),
            certificate_chain: chain,
            private_key: root.path().join("unread-private-key.pem"),
        };
        let error = tls_config(&config).unwrap_err();
        assert!(error.chain().any(|cause| matches!(
            cause.downcast_ref::<rustls::Error>(),
            Some(rustls::Error::InvalidCertificate(_))
        )));
    }
    const SELF_SIGNED_CERTIFICATE: &str = r#"-----BEGIN CERTIFICATE-----
MIIBwjCCAWigAwIBAgIUb79GXMJdg+AVQnWDanudAlZ1cBswCgYIKoZIzj0EAwIw
IzEhMB8GA1UEAwwYYm9yZy1kaXJlY3QtdGVzdC5pbnZhbGlkMCAXDTI2MTAxMzA3
MzA1MloYDzIxMjYwOTE5MDczMDUyWjAjMSEwHwYDVQQDDBhib3JnLWRpcmVjdC10
ZXN0LmludmFsaWQwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAAQyMpx82OLvvaB5
TjW+KlUKk+7jTOUgN9ShiAkOf/HhwlAkuc9PvMAQCKakK9B8xjCjpmIwivNPgzPq
vkSSPYUSo3gwdjAdBgNVHQ4EFgQUTLecF9sGz837NRDn09uK13p5QQcwHwYDVR0j
BBgwFoAUTLecF9sGz837NRDn09uK13p5QQcwDwYDVR0TAQH/BAUwAwEB/zAjBgNV
HREEHDAaghhib3JnLWRpcmVjdC10ZXN0LmludmFsaWQwCgYIKoZIzj0EAwIDSAAw
RQIgMNoCSSI2/kkApa9pQAVV1L3ZfTz3C2gOQG6kkV7EqXUCIQCRhgwhJTRCzmlQ
gQFw1DzzmTYQWGnVhUUjVmNg2qBuIQ==
-----END CERTIFICATE-----
"#;
}
