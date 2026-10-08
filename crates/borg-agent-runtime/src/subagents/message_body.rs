use super::*;

impl SubagentCoordinator {
    pub(super) async fn message_body_page(
        &self,
        actor: Uuid,
        message_id: Uuid,
        text_offset_chars: usize,
        text_limit_chars: usize,
    ) -> Result<Value> {
        let binding = self
            .store
            .workspace_binding(actor)
            .await?
            .context("message body is unavailable to this participant")?;
        let store = self.workspace_store().await?;
        let visible: HashSet<_> = store
            .list_workspaces_for_participant(binding.participant_id)
            .await?
            .into_iter()
            .map(|workspace| workspace.id)
            .collect();
        let mut scanned = HashSet::new();
        for delivery in store.message_deliveries(message_id).await? {
            if !visible.contains(&delivery.workspace_id) || !scanned.insert(delivery.workspace_id) {
                continue;
            }
            // Replay already enforces membership and sender/recipient visibility.
            let events = store
                .replay(
                    delivery.workspace_id,
                    binding.participant_id,
                    delivery.sequence.saturating_sub(1),
                    1,
                )
                .await?;
            let Some(event) = events.into_iter().next() else {
                continue;
            };
            let WorkspaceEventKind::Message { message, mode } = event.kind else {
                continue;
            };
            if event.sequence != delivery.sequence || message.id != message_id {
                continue;
            }
            // Scrub before slicing so a secret cannot be split across page edges.
            let safe_text = crate::secret_scrub::scrub_secrets(&message.body.text);
            let total = safe_text.chars().count();
            ensure!(
                text_offset_chars <= total,
                "text_offset_chars exceeds message text"
            );
            let text: String = safe_text
                .chars()
                .skip(text_offset_chars)
                .take(text_limit_chars.clamp(1, 4_000))
                .collect();
            let next = text_offset_chars + text.chars().count();
            let mut attachments = serde_json::to_value(&message.body.attachments)?;
            team_harness::scrub_value(&mut attachments);
            return Ok(json!({
                "message_id": message.id, "event_id": event.id,
                "workspace_id": event.workspace_id, "sequence": event.sequence,
                "author_id": message.author_id, "thread_id": message.thread_id,
                "reply_to_message_id": message.reply_to_message_id,
                "created_at": event.created_at, "delivery_mode": mode,
                "attachments": attachments,
                "text": text, "text_offset_chars": text_offset_chars,
                "next_text_offset_chars": if next < total { Some(next) } else { None },
                "total_text_chars": total, "has_more": next < total,
                "text_redacted": safe_text.as_ref() != message.body.text,
                "source": "canonical workspace message; read-only sender/recipient replay",
                "caveat": "Text is paged after secret redaction; offsets refer to the redacted Unicode text. This read neither acknowledges nor re-delivers the message.",
            }));
        }
        bail!("message body is unavailable to this participant")
    }
}
