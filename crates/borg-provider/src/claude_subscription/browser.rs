//! Native browser login collection; opening the URL belongs to the frontend.

use super::{AuthAuthority, PendingLogin};
use anyhow::{Context, Result, bail, ensure};
use std::{net::Ipv4Addr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{Instant, timeout, timeout_at},
};

const LOGIN_TIMEOUT: Duration = Duration::from_secs(600);
const HEADER_TIMEOUT: Duration = Duration::from_secs(5);
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(1);
const MAX_HEADER_BYTES: usize = 8 * 1024;

/// Owns the listener and pending PKCE login. Dropping it (or its completion
/// future) closes callback collection; no callback task is detached.
pub struct BrowserLogin {
    listener: TcpListener,
    pending: PendingLogin,
    authority: AuthAuthority,
    authorize_url: String,
    deadline: Instant,
}

pub async fn begin_browser_login(authority: AuthAuthority) -> Result<BrowserLogin> {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .context("bind Claude login callback")?;
    let pending = PendingLogin::loopback(listener.local_addr()?.port())?;
    let deadline = Instant::now() + LOGIN_TIMEOUT;
    let authorize_url = pending.authorize_url()?;
    Ok(BrowserLogin {
        listener,
        pending,
        authority,
        authorize_url,
        deadline,
    })
}

impl BrowserLogin {
    /// Frontend may display/open this URL; it contains no token or PKCE verifier.
    pub fn authorize_url(&self) -> &str {
        &self.authorize_url
    }

    /// Explicit new login may select a different account. Session/account guards
    /// remain the controller's responsibility; exchange/save belong to auth.
    pub async fn complete(self) -> Result<()> {
        let Self {
            listener,
            pending,
            authority,
            deadline,
            ..
        } = self;
        let (code, state) = wait_for_callback(&listener, &pending, deadline).await?;
        drop(listener);
        pending.complete(code, state, authority, None).await
    }
}

async fn wait_for_callback(
    listener: &TcpListener,
    pending: &PendingLogin,
    deadline: Instant,
) -> Result<(String, String)> {
    timeout_at(deadline, async {
        loop {
            let (mut stream, peer) = listener.accept().await.context("accept Claude callback")?;
            if !peer.ip().is_loopback() {
                continue;
            }
            let callback = match timeout(HEADER_TIMEOUT, read_header(&mut stream)).await {
                Ok(Ok(header)) => parse_callback(&header, pending),
                _ => Callback::Reject,
            };
            match callback {
                Callback::Reject => {
                    respond(&mut stream, false).await;
                }
                Callback::OAuthError => {
                    respond(&mut stream, false).await;
                    bail!("Claude browser login was not authorized; restart login");
                }
                Callback::Code(code, state) => {
                    respond(&mut stream, true).await;
                    return Ok((code, state));
                }
            }
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("Claude browser login expired; restart login"))?
}

async fn read_header(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut header = Vec::new();
    let mut chunk = [0u8; 512];
    loop {
        let remaining = MAX_HEADER_BYTES - header.len();
        ensure!(remaining > 0, "Claude callback header too large");
        let limit = remaining.min(chunk.len());
        let count = stream.read(&mut chunk[..limit]).await?;
        ensure!(count != 0, "incomplete Claude callback header");
        header.extend_from_slice(&chunk[..count]);
        if let Some(end) = header.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
            header.truncate(end + 4);
            return Ok(header);
        }
    }
}

// No Debug implementation: these values include the authorization code/state.
enum Callback {
    Reject,
    OAuthError,
    Code(String, String),
}

fn parse_callback(header: &[u8], pending: &PendingLogin) -> Callback {
    let Ok(header) = std::str::from_utf8(header) else {
        return Callback::Reject;
    };
    let Some(line) = header.split("\r\n").next() else {
        return Callback::Reject;
    };
    let mut parts = line.split(' ');
    let (Some("GET"), Some(target), Some("HTTP/1.1" | "HTTP/1.0"), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Callback::Reject;
    };
    // Check the raw origin-form path before URL normalization. No absolute URLs,
    // alternate paths, fragments, redirects, or Origin/Host-based routing.
    if target.split('?').next() != Some("/callback") || target.contains('#') {
        return Callback::Reject;
    }
    let Ok(url) = reqwest::Url::parse(&format!("http://localhost{target}")) else {
        return Callback::Reject;
    };
    let (mut code, mut state, mut error) = (None, None, None);
    for (key, value) in url.query_pairs() {
        let slot = match key.as_ref() {
            "code" => &mut code,
            "state" => &mut state,
            "error" => &mut error,
            _ => continue,
        };
        if slot.replace(value.into_owned()).is_some() {
            return Callback::Reject;
        }
    }
    let Some(state) = state.filter(|state| pending.matches_state(state)) else {
        return Callback::Reject;
    };
    // OAuth denial is only terminal after state validation. Never reflect its
    // provider-supplied error text or consume PKCE for an unrelated request.
    if error.is_some() {
        return Callback::OAuthError;
    }
    match code.filter(|code| !code.is_empty()) {
        Some(code) => Callback::Code(code, state),
        None => Callback::Reject,
    }
}

async fn respond(stream: &mut TcpStream, accepted: bool) {
    let (status, body) = if accepted {
        (
            "200 OK",
            "Callback received. Return to Borg to finish login.\n",
        )
    } else {
        ("400 Bad Request", "Login callback was not accepted.\n")
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = timeout(RESPONSE_TIMEOUT, stream.write_all(response.as_bytes())).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(pending: &PendingLogin) -> String {
        reqwest::Url::parse(&pending.authorize_url().unwrap())
            .unwrap()
            .query_pairs()
            .find(|(key, _)| key == "state")
            .unwrap()
            .1
            .into_owned()
    }

    async fn request(listener: &TcpListener, target: &str) -> String {
        let mut client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        client
            .write_all(format!("GET {target} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes())
            .await
            .unwrap();
        let mut response = String::new();
        timeout(Duration::from_secs(2), client.read_to_string(&mut response))
            .await
            .unwrap()
            .unwrap();
        response
    }

    // CSRF cannot consume PKCE or reach exchange. Exercise the actual listener,
    // stopping at parsed code/state so no auth fetch/storage hook is needed.
    #[tokio::test]
    async fn wrong_state_and_error_do_not_consume_then_valid_callback_decodes() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let pending = PendingLogin::loopback(listener.local_addr().unwrap().port()).unwrap();
        let state = state(&pending);
        let collect =
            wait_for_callback(&listener, &pending, Instant::now() + Duration::from_secs(3));
        let send = async {
            for target in [
                "/callback?code=owned-fake&state=wrong",
                "/callback?error=owned-fake-error&state=wrong",
                "/other?code=owned-fake&state=wrong",
            ] {
                let reply = request(&listener, target).await;
                assert!(reply.starts_with("HTTP/1.1 400"));
                assert!(reply.contains("Cache-Control: no-store\r\n"));
                assert!(reply.contains("Connection: close\r\n"));
                assert!(!reply.contains("owned-fake"));
            }
            let reply = request(
                &listener,
                &format!("/callback?code=owned%2Bfake+code&state={state}"),
            )
            .await;
            assert!(reply.starts_with("HTTP/1.1 200"));
            assert!(!reply.contains(&state));
            assert!(!reply.contains("owned"));
        };
        let (result, ()) = tokio::join!(collect, send);
        let (code, returned_state) = result.unwrap();
        assert_eq!(code, "owned+fake code");
        assert_eq!(returned_state, state);
        assert!(pending.matches_state(&returned_state));
    }

    // Only origin-form GET /callback is accepted, including before normalization.
    #[tokio::test]
    async fn method_path_duplicates_and_valid_state_denial() {
        let pending = PendingLogin::loopback(12345).unwrap();
        let state = state(&pending);
        for line in [
            format!("POST /callback?code=x&state={state} HTTP/1.1"),
            format!("GET /other?code=x&state={state} HTTP/1.1"),
            format!("GET /other/../callback?code=x&state={state} HTTP/1.1"),
            format!("GET http://localhost/callback?code=x&state={state} HTTP/1.1"),
            format!("GET /callback?code=x&state={state}&state={state} HTTP/1.1"),
        ] {
            assert!(matches!(
                parse_callback(format!("{line}\r\n\r\n").as_bytes(), &pending),
                Callback::Reject
            ));
        }
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let collect =
            wait_for_callback(&listener, &pending, Instant::now() + Duration::from_secs(3));
        let target = format!("/callback?error=owned-fake-error&state={state}");
        let (result, reply) = tokio::join!(collect, request(&listener, &target));
        assert!(result.unwrap_err().to_string().contains("not authorized"));
        assert!(!reply.contains("owned-fake-error"));
        assert!(!reply.contains(&state));
    }

    // A local slowloris/oversized header cannot allocate without bound or keep
    // callback collection alive beyond its overall deadline.
    #[tokio::test]
    async fn oversized_and_stalled_headers_are_bounded() {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let mut client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (mut server, _) = listener.accept().await.unwrap();
        let send = client.write_all(&[b'x'; MAX_HEADER_BYTES]);
        let read = timeout(Duration::from_secs(1), read_header(&mut server));
        let (_, result) = tokio::join!(send, read);
        assert!(result.unwrap().is_err());
        drop(server);
        drop(client);
        let pending = PendingLogin::loopback(listener.local_addr().unwrap().port()).unwrap();
        let stalled = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let error = wait_for_callback(
            &listener,
            &pending,
            Instant::now() + Duration::from_millis(30),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("expired"));
        drop(stalled);
    }
}
