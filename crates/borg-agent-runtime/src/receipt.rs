use anyhow::{Result, bail, ensure};
use async_trait::async_trait;
use serde::Serialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

pub(crate) const RECEIPT_VERSION: u8 = 1;

#[derive(Debug)]
pub enum ReceiptState<T> {
    Missing,
    Started,
    Terminal(T),
    Conflict,
    Corrupt,
}

pub(crate) const RECEIPT_STATE_STARTED: &str = "started";
pub(crate) const RECEIPT_STATE_TERMINAL: &str = "terminal";
/// Bounds metadata and response payloads stored in one receipt row.
pub(crate) const MAX_RECEIPT_JSON_BYTES: usize = 1024 * 1024;
/// A receipt may have one intent transition and one terminal transition.
pub(crate) const MAX_RECEIPT_TRANSITIONS: usize = 2;

#[derive(Debug, Clone)]
pub(crate) struct ReceiptRecord {
    pub(crate) version: i64,
    pub(crate) state: String,
    pub(crate) request: serde_json::Value,
    pub(crate) response: Option<serde_json::Value>,
}

#[derive(Debug, Clone)]
pub(crate) struct ReceiptTransition {
    pub(crate) sequence: i64,
    pub(crate) version: i64,
    pub(crate) state: String,
    pub(crate) request: serde_json::Value,
    pub(crate) response: Option<serde_json::Value>,
}

/// The durable receipt tier, independent of engine.
///
/// WHY THE TRAIT IS VALUE-TYPED: a receipt's whole job is to make a mutation
/// replay-safe, so callers hand it their own request and response types. Those
/// generics are not object-safe, and every backend converts them to JSON on the
/// first line anyway. So the trait carries the erased `serde_json::Value` form,
/// and the typed API callers actually use lives in `impl dyn ReceiptBackend`
/// below -- same ergonomics, but now one call site can hold either engine.
#[async_trait]
pub trait ReceiptBackend: Send + Sync {
    /// The recorded state of a request, with its response body untyped.
    async fn load_value(
        &self,
        request_id: Uuid,
        request: &serde_json::Value,
    ) -> Result<ReceiptState<serde_json::Value>>;

    /// Durably record intent before a mutation may begin.
    async fn begin_value(&self, request_id: Uuid, request: &serde_json::Value) -> Result<()>;

    /// Publish a terminal response, retaining the intent transition as evidence
    /// that the mutation was authorised.
    async fn finish_value(
        &self,
        request_id: Uuid,
        request: &serde_json::Value,
        response: &serde_json::Value,
    ) -> Result<()>;

    /// Queue a command for a relay host to collect.
    async fn enqueue_host_operation(
        &self,
        host_id: Uuid,
        request_id: Uuid,
        command: &serde_json::Value,
    ) -> Result<()>;

    /// Take the next queued command for a host.
    async fn next_host_operation(&self, host_id: Uuid)
    -> Result<Option<(Uuid, serde_json::Value)>>;

    /// Park a command that could not be executed, without losing it.
    async fn quarantine_host_operation(&self, host_id: Uuid, request_id: Uuid) -> Result<()>;

    /// Look up a still-queued command by request id.
    async fn queued_host_operation(
        &self,
        host_id: Uuid,
        request_id: Uuid,
    ) -> Result<Option<serde_json::Value>>;

    /// Remove a command once it has been carried out.
    async fn finish_host_operation(&self, host_id: Uuid, request_id: Uuid) -> Result<()>;
}

/// The typed receipt API.
///
/// Inherent methods on `dyn ReceiptBackend` rather than trait methods, because
/// they are generic and so cannot be dispatched dynamically. Callers get the
/// same signatures they had before the tier became engine-neutral.
impl dyn ReceiptBackend + '_ {
    /// The recorded state of `request_id`, decoded into `Response`.
    ///
    /// A stored body that will not deserialise into `Response` is reported as
    /// `Corrupt`, not as an error: a receipt that cannot be read back is
    /// exactly the corruption this type exists to surface, and the caller's
    /// recovery for it is the same either way.
    pub async fn load<Request, Response>(
        &self,
        request_id: Uuid,
        request: &Request,
    ) -> Result<ReceiptState<Response>>
    where
        Request: Serialize,
        Response: DeserializeOwned,
    {
        let request = bounded_json_value(request, "receipt request")?;
        Ok(match self.load_value(request_id, &request).await? {
            ReceiptState::Terminal(response) => match serde_json::from_value(response) {
                Ok(response) => ReceiptState::Terminal(response),
                Err(_) => ReceiptState::Corrupt,
            },
            ReceiptState::Missing => ReceiptState::Missing,
            ReceiptState::Started => ReceiptState::Started,
            ReceiptState::Conflict => ReceiptState::Conflict,
            ReceiptState::Corrupt => ReceiptState::Corrupt,
        })
    }

    /// Durably record intent before a mutation may begin.
    pub async fn begin<Request: Serialize>(
        &self,
        request_id: Uuid,
        request: &Request,
    ) -> Result<()> {
        let request = bounded_json_value(request, "receipt request")?;
        self.begin_value(request_id, &request).await
    }

    /// Publish a terminal response for an already-begun request.
    pub async fn finish<Request, Response>(
        &self,
        request_id: Uuid,
        request: &Request,
        response: &Response,
    ) -> Result<()>
    where
        Request: Serialize,
        Response: Serialize,
    {
        let request = bounded_json_value(request, "receipt request")?;
        let response = bounded_json_value(response, "receipt response")?;
        self.finish_value(request_id, &request, &response).await
    }

    /// Queue a typed command for a relay host.
    pub async fn enqueue_host_command<Command: Serialize>(
        &self,
        host_id: Uuid,
        request_id: Uuid,
        command: &Command,
    ) -> Result<()> {
        let command = bounded_json_value(command, "host command")?;
        self.enqueue_host_operation(host_id, request_id, &command)
            .await
    }
}

pub(crate) fn bounded_json_value<T: Serialize>(
    value: &T,
    label: &str,
) -> Result<serde_json::Value> {
    let value = serde_json::to_value(value).with_context(|| format!("failed to encode {label}"))?;
    let bytes = serde_json::to_vec(&value)?.len();
    ensure!(
        bytes <= MAX_RECEIPT_JSON_BYTES,
        "{label} exceeds {MAX_RECEIPT_JSON_BYTES} bytes"
    );
    Ok(value)
}

pub(crate) fn validate_existing_request(
    existing: &ReceiptRecord,
    request: &serde_json::Value,
) -> Result<()> {
    ensure!(
        existing.version == i64::from(RECEIPT_VERSION),
        "receipt version is incompatible"
    );
    ensure!(
        existing.request == *request,
        "receipt request identity conflict"
    );
    Ok(())
}

pub(crate) fn validate_receipt_projection(
    record: &ReceiptRecord,
    transitions: &[ReceiptTransition],
) -> Result<()> {
    ensure!(!transitions.is_empty(), "receipt has no audit transitions");
    ensure!(
        transitions.len() <= MAX_RECEIPT_TRANSITIONS,
        "receipt has too many audit transitions"
    );
    for (index, transition) in transitions.iter().enumerate() {
        ensure!(
            transition.sequence == i64::try_from(index + 1)?,
            "receipt audit sequence is not contiguous"
        );
        ensure!(
            transition.version == record.version && transition.request == record.request,
            "receipt audit identity does not match current projection"
        );
    }
    match record.state.as_str() {
        RECEIPT_STATE_STARTED => {
            ensure!(
                transitions.len() == 1,
                "started receipt has invalid audit history"
            );
            ensure!(
                transitions[0].state == RECEIPT_STATE_STARTED
                    && transitions[0].response.is_none()
                    && record.response.is_none(),
                "started receipt audit is inconsistent"
            );
        }
        RECEIPT_STATE_TERMINAL => {
            let terminal = transitions.last().expect("non-empty transitions");
            ensure!(
                terminal.state == RECEIPT_STATE_TERMINAL,
                "terminal audit is missing"
            );
            ensure!(
                terminal.response.is_some() && terminal.response == record.response,
                "terminal receipt response audit is inconsistent"
            );
            if transitions.len() == 2 {
                ensure!(
                    transitions[0].state == RECEIPT_STATE_STARTED
                        && transitions[0].response.is_none(),
                    "receipt intent audit is inconsistent"
                );
            }
        }
        _ => bail!("receipt state is invalid"),
    }
    Ok(())
}
