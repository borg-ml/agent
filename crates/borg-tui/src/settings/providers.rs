use super::*;
use borg_ui::providers::{ConfiguredModel, ConfiguredModelVariant, ConfiguredProvider};
use std::collections::BTreeMap;

pub(super) struct ProviderEditor {
    original: Option<(String, ConfiguredProvider)>,
    id: String,
    provider: ConfiguredProvider,
    model: Option<ModelEditor>,
    confirm_delete: bool,
    key_changed: bool,
}

struct ModelEditor {
    original_id: Option<String>,
    id: String,
    model: ConfiguredModel,
}

fn row(
    id: SettingId,
    label: &'static str,
    description: &str,
    control: SettingControl,
) -> SettingRow {
    SettingRow {
        id,
        section: SettingsSection::Providers,
        label,
        description: description.into(),
        control,
        enabled: true,
    }
}
fn text(id: SettingId, label: &'static str, description: &str, value: impl ToString) -> SettingRow {
    row(
        id,
        label,
        description,
        SettingControl::Text(value.to_string()),
    )
}
fn action(
    id: SettingId,
    label: &'static str,
    description: &str,
    value: impl Into<String>,
) -> SettingRow {
    row(id, label, description, SettingControl::Action(value.into()))
}

pub(super) fn list_rows(providers: &BTreeMap<String, ConfiguredProvider>) -> Vec<SettingRow> {
    use SettingId as Id;
    let mut rows = vec![action(
        Id::ProviderAdd,
        "Add custom provider",
        "Connect any OpenAI-compatible endpoint: local servers, cloud APIs or your own gateway. No API key is required for an unauthenticated local server.",
        "Add provider…",
    )];
    for (index, (id, provider)) in providers.iter().enumerate() {
        let auth = if let Some(env) = &provider.api_key_env {
            format!(
                "key from ${env}{}",
                if std::env::var_os(env).is_some_and(|value| !value.is_empty()) {
                    ""
                } else {
                    " (not set)"
                }
            )
        } else if provider.api_key.is_some()
            || borg_provider::credentials::named_provider_api_key(id).is_some()
        {
            "saved key".into()
        } else {
            "no API key".into()
        };
        rows.push(action(
            Id::ProviderEntry(index),
            "Custom provider",
            &format!(
                "{id} · {} · {auth} · {} models. Enter to edit; changes apply after Save provider.",
                provider.base_url,
                provider.models.len()
            ),
            format!("{} · {id}", provider.name.as_deref().unwrap_or(id)),
        ));
    }
    rows
}

pub(super) fn editor_rows(editor: &ProviderEditor) -> Vec<SettingRow> {
    use SettingId as Id;
    if let Some(model) = &editor.model {
        let optional = |value: Option<u64>| value.map(|v| v.to_string()).unwrap_or_default();
        let variants: BTreeMap<_, _> = model
            .model
            .variants
            .iter()
            .map(|(name, variant)| (name, &variant.body))
            .collect();
        return vec![
            action(
                Id::ProviderBack,
                "Back to provider",
                "Discard this model's unsaved changes and return to the provider form.",
                "Cancel model edit",
            ),
            text(
                Id::ProviderModelId,
                "Model ID",
                "Exact upstream model ID, including any slashes. Borg exposes it as provider/model.",
                &model.id,
            ),
            text(
                Id::ProviderModelName,
                "Display name",
                "Optional friendly name for the model picker.",
                model.model.name.as_deref().unwrap_or_default(),
            ),
            text(
                Id::ProviderModelContext,
                "Context window",
                "Optional positive token count. Set the real limit so automatic compaction works correctly.",
                optional(model.model.context_window_tokens),
            ),
            text(
                Id::ProviderModelOutput,
                "Maximum output",
                "Optional positive output-token limit supported by this endpoint.",
                optional(model.model.max_output_tokens),
            ),
            text(
                Id::ProviderModelCache,
                "Cache lifetime (seconds)",
                "Optional documented cache retention, greater than 10 seconds. Leave blank if unknown.",
                optional(model.model.prompt_cache_ttl_seconds),
            ),
            text(
                Id::ProviderModelBody,
                "Request options (JSON)",
                "Extra request fields, e.g. {\"temperature\":0.2}. Conversation and tool fields remain protected.",
                serde_json::to_string(&model.model.body).unwrap_or_default(),
            ),
            text(
                Id::ProviderModelVariants,
                "Variants (JSON)",
                "Named reasoning presets, e.g. {\"high\":{\"reasoning_effort\":\"high\"}}. Available in reasoning controls.",
                serde_json::to_string(&variants).unwrap_or_default(),
            ),
            action(
                Id::ProviderModelSave,
                "Keep model changes",
                "Keep this model in the provider draft. Save provider to write all changes to disk.",
                "Keep model",
            ),
            action(
                Id::ProviderModelDelete,
                "Remove model",
                "Remove this model from the provider draft. Save provider to apply.",
                "Remove from draft",
            ),
        ];
    }
    let provider = &editor.provider;
    let mut rows = vec![
        action(
            Id::ProviderBack,
            "Back to providers",
            "Discard the draft and return to configured providers. Nothing is written until Save provider.",
            "Cancel provider edit",
        ),
        text(
            Id::ProviderId,
            "Provider ID",
            "Stable alias: letters, digits, hyphens or underscores. For example work or ollama. Built-in provider names are reserved.",
            &editor.id,
        ),
        text(
            Id::ProviderName,
            "Display name",
            "Optional friendly connection name shown in model pickers.",
            provider.name.as_deref().unwrap_or_default(),
        ),
        text(
            Id::ProviderEndpoint,
            "Base URL",
            "OpenAI-compatible base URL, e.g. http://localhost:11434/v1, or a complete /chat/completions URL. No connection is made when saving.",
            &provider.base_url,
        ),
        text(
            Id::ProviderKeyEnv,
            "API key environment",
            "Recommended: environment-variable name, e.g. WORK_API_KEY. Choosing this removes any inline key. Leave both key fields blank for no authentication.",
            provider.api_key_env.as_deref().unwrap_or_default(),
        ),
        row(
            Id::ProviderKey,
            "API key",
            "Paste a replacement key into your private agent config. Blank keeps the saved key; Clear authentication removes it. Keys stay masked. A new key clears the environment reference.",
            SettingControl::Secret(provider.api_key.clone().unwrap_or_else(|| {
                if !editor.key_changed
                    && borg_provider::credentials::named_provider_api_key(&editor.id).is_some()
                {
                    "saved in credential store".into()
                } else {
                    String::new()
                }
            })),
        ),
        action(
            Id::ProviderClearAuth,
            "Clear authentication",
            "Remove the saved key and environment reference from this draft. Save provider to apply.",
            "Use no API key",
        ),
        row(
            Id::ProviderHeaders,
            "Headers (private JSON)",
            "Header values may be credentials and stay masked. Paste a replacement JSON object; blank keeps existing headers and {} clears them.",
            SettingControl::Secret(if provider.headers.is_empty() {
                String::new()
            } else {
                serde_json::to_string(&provider.headers).unwrap_or_default()
            }),
        ),
        action(
            Id::ProviderModelAdd,
            "Add model",
            "Add an upstream model ID and optional limits, request options and variants.",
            "Add model…",
        ),
    ];
    for (index, (id, model)) in provider.models.iter().enumerate() {
        rows.push(action(
            Id::ProviderModelEntry(index),
            "Model",
            &format!("{id} · edit limits, request options and reasoning variants."),
            model.name.as_deref().unwrap_or(id),
        ));
    }
    rows.push(action(Id::ProviderSave, "Save provider", "Validate and save this connection and its models. Updates model and commit-model pickers without switching the active chat model.", "Save provider"));
    if editor.original.is_some() {
        rows.push(action(Id::ProviderDelete, "Remove provider", "Removes this provider configuration and its saved key. Existing session history stays intact. Press again to confirm.", if editor.confirm_delete { "Confirm removal" } else { "Remove provider…" }));
    }
    rows
}

impl SettingsScreen {
    pub(super) fn provider_action(&mut self, id: SettingId) -> Option<SettingsEffect> {
        use SettingId as Id;
        match id {
            Id::ProviderAdd => {
                self.provider_editor = Some(ProviderEditor {
                    original: None,
                    id: String::new(),
                    provider: Default::default(),
                    model: None,
                    confirm_delete: false,
                    key_changed: false,
                })
            }
            Id::ProviderEntry(index) => {
                let (id, provider) = self.custom_providers.iter().nth(index)?;
                self.provider_editor = Some(ProviderEditor {
                    original: Some((id.clone(), provider.clone())),
                    id: id.clone(),
                    provider: provider.clone(),
                    model: None,
                    confirm_delete: false,
                    key_changed: false,
                });
            }
            Id::ProviderBack => {
                if self.provider_editor.as_ref()?.model.is_some() {
                    self.provider_editor.as_mut()?.model = None;
                } else {
                    self.provider_editor = None;
                }
            }
            Id::ProviderClearAuth => {
                let editor = self.provider_editor.as_mut()?;
                editor.provider.api_key = None;
                editor.provider.api_key_env = None;
                editor.key_changed = true;
                return Some(SettingsEffect::None);
            }
            Id::ProviderModelAdd => {
                self.provider_editor.as_mut()?.model = Some(ModelEditor {
                    original_id: None,
                    id: String::new(),
                    model: Default::default(),
                })
            }
            Id::ProviderModelEntry(index) => {
                let editor = self.provider_editor.as_mut()?;
                let (id, model) = editor.provider.models.iter().nth(index)?;
                editor.model = Some(ModelEditor {
                    original_id: Some(id.clone()),
                    id: id.clone(),
                    model: model.clone(),
                });
            }
            Id::ProviderModelSave => {
                let editor = self.provider_editor.as_mut()?;
                let model = editor.model.as_ref()?;
                if model.id.trim().is_empty() {
                    self.error = Some("Enter an upstream model ID.".into());
                    return Some(SettingsEffect::None);
                }
                if model.original_id.as_deref() != Some(&model.id)
                    && editor.provider.models.contains_key(&model.id)
                {
                    self.error = Some("That model ID already exists.".into());
                    return Some(SettingsEffect::None);
                }
                let model = editor.model.take()?;
                if let Some(id) = model.original_id {
                    editor.provider.models.remove(&id);
                }
                editor.provider.models.insert(model.id, model.model);
            }
            Id::ProviderModelDelete => {
                let editor = self.provider_editor.as_mut()?;
                let model = editor.model.take()?;
                if let Some(id) = model.original_id {
                    editor.provider.models.remove(&id);
                }
            }
            Id::ProviderSave => {
                let editor = self.provider_editor.as_ref()?;
                return Some(SettingsEffect::Apply(Box::new(
                    UiAction::SaveCustomProvider {
                        original: editor.original.clone(),
                        id: editor.id.clone(),
                        provider: editor.provider.clone(),
                        key_changed: editor.key_changed,
                    },
                )));
            }
            Id::ProviderDelete => {
                let editor = self.provider_editor.as_mut()?;
                if editor.confirm_delete {
                    let (id, expected) = editor.original.clone()?;
                    return Some(SettingsEffect::Apply(Box::new(
                        UiAction::DeleteCustomProvider { id, expected },
                    )));
                }
                editor.confirm_delete = true;
                return Some(SettingsEffect::None);
            }
            _ => return None,
        }
        self.section = SettingsSection::ALL
            .iter()
            .position(|section| *section == SettingsSection::Providers)
            .unwrap();
        self.query.clear();
        self.selected = 0;
        self.scroll = 0;
        self.error = None;
        self.editing = None;
        Some(SettingsEffect::None)
    }

    pub(super) fn provider_edit(
        &mut self,
        id: SettingId,
        value: &str,
    ) -> Option<Result<(), String>> {
        use SettingId as Id;
        if !matches!(
            id,
            Id::ProviderId
                | Id::ProviderName
                | Id::ProviderEndpoint
                | Id::ProviderKeyEnv
                | Id::ProviderKey
                | Id::ProviderHeaders
                | Id::ProviderModelId
                | Id::ProviderModelName
                | Id::ProviderModelContext
                | Id::ProviderModelOutput
                | Id::ProviderModelCache
                | Id::ProviderModelBody
                | Id::ProviderModelVariants
        ) {
            return None;
        }
        let editor = self.provider_editor.as_mut()?;
        let optional = || (!value.is_empty()).then(|| value.to_string());
        let number = || {
            if value.is_empty() {
                Ok(None)
            } else {
                value
                    .parse::<u64>()
                    .ok()
                    .filter(|v| *v > 0)
                    .map(Some)
                    .ok_or_else(|| "Enter a positive token count, or leave blank.".to_string())
            }
        };
        let json_error = |_: serde_json::Error| {
            "Enter a valid JSON object with the expected value types.".to_string()
        };
        let result = (|| -> Result<(), String> {
            match id {
                Id::ProviderId => editor.id = value.to_string(),
                Id::ProviderName => editor.provider.name = optional(),
                Id::ProviderEndpoint => editor.provider.base_url = value.to_string(),
                Id::ProviderKeyEnv => {
                    editor.provider.api_key_env = optional();
                    if !value.is_empty() {
                        editor.provider.api_key = None;
                        editor.key_changed = true;
                    }
                }
                Id::ProviderKey => {
                    if !value.is_empty() {
                        editor.provider.api_key = optional();
                        editor.provider.api_key_env = None;
                        editor.key_changed = true;
                    }
                }
                Id::ProviderHeaders => {
                    if !value.is_empty() {
                        editor.provider.headers =
                            serde_json::from_str(value).map_err(json_error)?;
                    }
                }
                id => {
                    let model = editor
                        .model
                        .as_mut()
                        .ok_or_else(|| "No model is being edited.".to_string())?;
                    match id {
                        Id::ProviderModelId => model.id = value.to_string(),
                        Id::ProviderModelName => model.model.name = optional(),
                        Id::ProviderModelContext => model.model.context_window_tokens = number()?,
                        Id::ProviderModelOutput => model.model.max_output_tokens = number()?,
                        Id::ProviderModelCache => {
                            let ttl = number()?;
                            if ttl.is_some_and(|ttl| ttl <= 10) {
                                return Err(
                                    "Cache lifetime must be greater than 10 seconds.".into()
                                );
                            }
                            model.model.prompt_cache_ttl_seconds = ttl;
                        }
                        Id::ProviderModelBody => {
                            model.model.body = serde_json::from_str(value).map_err(json_error)?
                        }
                        Id::ProviderModelVariants => {
                            let variants: BTreeMap<String, BTreeMap<String, toml::Value>> =
                                serde_json::from_str(value).map_err(json_error)?;
                            model.model.variants = variants
                                .into_iter()
                                .map(|(name, body)| (name, ConfiguredModelVariant { body }))
                                .collect();
                        }
                        _ => return Err("Not a provider setting.".into()),
                    }
                }
            }
            Ok(())
        })();
        Some(result)
    }

    pub(crate) fn provider_save_result(&mut self, error: Option<String>) {
        if let Some(error) = error {
            self.error = Some(error);
        } else {
            self.provider_editor = None;
            self.selected = 0;
            self.scroll = 0;
            self.query.clear();
            self.error = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_forms_keep_drafts_and_never_paint_keys() {
        use ratatui::backend::TestBackend;
        let mut screen = SettingsScreen::default();
        screen.provider_action(SettingId::ProviderAdd).unwrap();
        screen
            .provider_edit(SettingId::ProviderId, "work")
            .unwrap()
            .unwrap();
        screen
            .provider_edit(SettingId::ProviderEndpoint, "http://localhost:11434/v1")
            .unwrap()
            .unwrap();
        screen
            .provider_edit(SettingId::ProviderKey, "saved-secret-token")
            .unwrap()
            .unwrap();
        assert!(screen.custom_providers.is_empty());
        let rows = editor_rows(screen.provider_editor.as_ref().unwrap());
        screen.selected = rows
            .iter()
            .position(|row| row.id == SettingId::ProviderKey)
            .unwrap();
        screen.editing = Some((SettingId::ProviderKey, "typed-secret-token".into()));
        let mut terminal = ratatui::Terminal::new(TestBackend::new(112, 34)).unwrap();
        terminal
            .draw(|frame| screen.render(frame, &rows, None, "⚙"))
            .unwrap();
        let visible = terminal
            .backend()
            .buffer()
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(!visible.contains("saved-secret-token"));
        assert!(!visible.contains("typed-secret-token"));
        assert!(visible.contains('•'));
        screen.editing = None;
        screen.provider_action(SettingId::ProviderModelAdd).unwrap();
        screen
            .provider_edit(SettingId::ProviderModelId, "vendor/model")
            .unwrap()
            .unwrap();
        assert!(
            screen
                .provider_edit(SettingId::ProviderModelContext, "-1")
                .unwrap()
                .is_err()
        );
        screen
            .provider_edit(SettingId::ProviderModelContext, "32768")
            .unwrap()
            .unwrap();
        screen
            .provider_edit(
                SettingId::ProviderModelVariants,
                r#"{"Creative":{"temperature":0.8}}"#,
            )
            .unwrap()
            .unwrap();
        screen
            .provider_action(SettingId::ProviderModelSave)
            .unwrap();
        let Some(SettingsEffect::Apply(action)) = screen.provider_action(SettingId::ProviderSave)
        else {
            panic!("expected one provider save");
        };
        let UiAction::SaveCustomProvider {
            original,
            id,
            provider,
            ..
        } = *action
        else {
            panic!("expected one provider save");
        };
        assert!(original.is_none());
        assert_eq!(id, "work");
        assert_eq!(
            provider.models["vendor/model"].context_window_tokens,
            Some(32768)
        );
        assert!(
            provider.models["vendor/model"]
                .variants
                .contains_key("Creative")
        );
        screen.provider_action(SettingId::ProviderBack).unwrap();
        assert!(screen.provider_editor.is_none());
        assert!(screen.custom_providers.is_empty());
    }
}

#[cfg(test)]
mod terminal_tests {
    use super::*;

    #[tokio::test]
    #[ignore = "requires a PTY; run under script with a sized terminal"]
    async fn provider_setup_is_available_inside_settings_without_a_model_switch() {
        let directory = tempfile::tempdir().unwrap();
        let mut terminal = BorgTerminal::enter(
            directory.path(),
            Uuid::new_v4(),
            directory.path().to_path_buf(),
            &KeybindingConfig::default(),
        )
        .unwrap();
        terminal.open_settings();
        let input = |event| TerminalInputEvent {
            event,
            scroll_repetitions: 1,
        };
        let key = |code| input(Event::Key(KeyEvent::new(code, KeyModifiers::NONE)));
        terminal.handle_event(key(KeyCode::Tab)).unwrap();
        terminal.handle_event(key(KeyCode::Enter)).unwrap();
        // Select the ID field after the back button, enter a draft, then leave it.
        terminal.handle_event(key(KeyCode::Down)).unwrap();
        terminal.handle_event(key(KeyCode::Enter)).unwrap();
        terminal
            .handle_event(input(Event::Paste("work".into())))
            .unwrap();
        terminal.handle_event(key(KeyCode::Enter)).unwrap();
        terminal.handle_event(key(KeyCode::Down)).unwrap();
        terminal.handle_event(key(KeyCode::Enter)).unwrap();
        terminal
            .handle_event(input(Event::Paste("Work gateway".into())))
            .unwrap();
        terminal.handle_event(key(KeyCode::Enter)).unwrap();
        terminal.handle_event(key(KeyCode::Down)).unwrap();
        terminal.handle_event(key(KeyCode::Enter)).unwrap();
        terminal
            .handle_event(input(Event::Paste("https://models.example.com/v1".into())))
            .unwrap();
        terminal.handle_event(key(KeyCode::Enter)).unwrap();
        terminal.draw().unwrap();
        let screen = terminal.settings.as_ref().unwrap();
        let draft = screen.provider_editor.as_ref().unwrap();
        assert_eq!(draft.id, "work");
        assert_eq!(draft.provider.base_url, "https://models.example.com/v1");
        assert!(terminal.custom_providers.is_empty());
        assert!(terminal.transcript.config.is_none());
        terminal.handle_event(key(KeyCode::Esc)).unwrap();
        assert!(
            terminal
                .settings
                .as_ref()
                .unwrap()
                .provider_editor
                .is_none()
        );
    }
}
