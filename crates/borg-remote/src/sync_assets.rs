use super::*;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use borg_view::wire::{AssetRef, TextField};

impl SyncAuthority {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn asset_page(
        &self,
        id: &str,
        claims: &ConnectionClaims,
        scope: Scope,
        request_id: String,
        asset_id: String,
        offset: String,
        budget: usize,
    ) -> Result<()> {
        let session = self
            .authorize(claims, &scope, false)
            .await?
            .ok_or_else(|| anyhow::anyhow!("asset read requires session scope"))?;
        ensure!((1..=65_536).contains(&budget), "asset page exceeds budget");
        let start = offset.parse::<usize>()?;
        let key = scope_key(self.host, Some(session));
        let (data, total, media_type) = if asset_id.starts_with("view-text:") {
            let address = borg_view::assets::TextAssetAddress::parse(&asset_id)?;
            let field = match address.field {
                TextField::Message => "message",
                TextField::Reasoning | TextField::ToolInput => "tool_input",
                TextField::ToolOutput => "tool_output",
            };
            let pinned = address.bytes.parse::<usize>()?;
            ensure!(start <= pinned, "asset offset exceeds captured prefix");
            let (body, current) = crate::row_index::field_page_at(
                &self.pg.pool,
                &key,
                &address.row_id,
                field,
                Some(&address.revision),
                start,
                budget.min(pinned - start).max(1),
            )
            .await?;
            ensure!(current == pinned, "captured text prefix identity mismatch");
            (
                body.into_iter().take(pinned - start).collect::<Vec<_>>(),
                pinned,
                "text/plain".to_owned(),
            )
        } else {
            let payload = Uuid::parse_str(&asset_id)?;
            let mut connection = self.pg.pool.acquire().await?;
            let slice = crate::session_store::postgres::payloads::read_payload_range(
                &mut connection,
                payload,
                Some(session),
                start,
                budget,
            )
            .await?
            .ok_or_else(|| anyhow::anyhow!("asset outside source scope"))?;
            ensure!(
                matches!(
                    slice.kind.as_str(),
                    "tool_input" | "tool_output" | "tool_result_input" | "final_text"
                ),
                "private provider/auth asset is not viewable"
            );
            let total = usize::try_from(slice.byte_len)?;
            ensure!(start <= total, "asset offset outside body");
            (slice.bytes, total, "text/plain".to_owned())
        };
        let next = start
            .checked_add(data.len())
            .ok_or_else(|| anyhow::anyhow!("asset range exhausted"))?;
        self.emit(
            id,
            ServerFrame::AssetPage {
                scope,
                request_id,
                asset_id,
                offset,
                data: URL_SAFE_NO_PAD.encode(data),
                total_bytes: total.to_string(),
                next_offset: (next < total).then(|| next.to_string()),
                media_type,
            },
        )
    }
}

pub(super) fn final_asset(
    session: Uuid,
    event: Uuid,
    text: &str,
) -> (AssetRef, (Uuid, Uuid, Uuid, Vec<u8>)) {
    let id = Uuid::new_v4();
    (
        AssetRef {
            id: id.to_string(),
            media_type: "text/plain".into(),
            bytes: text.len().to_string(),
        },
        (id, session, event, text.as_bytes().to_vec()),
    )
}
