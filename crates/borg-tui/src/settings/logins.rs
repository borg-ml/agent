//! Every built-in provider's logins on the Providers page: an overview row per
//! provider and one detail page shared by all of them. Subscription logins and
//! pay-per-use API keys are listed together; selecting one also chooses how the
//! provider bills.

use super::*;
use borg_provider::subscriptions::{AccountKind, Lane, Strategy};

/// One lane as the page draws it, read from the host's credential store and
/// the provider capability snapshot.
#[derive(Clone, Debug)]
pub(crate) struct LaneView {
    pub(crate) lane: Lane,
    pub(crate) strategy: Strategy,
    /// Subscriptions first, then API keys.
    pub(crate) accounts: Vec<AccountView>,
    /// A sign-in running for this lane.
    pub(crate) sign_in: Option<SignIn>,
}

/// A browser or device sign-in the host is waiting on.
#[derive(Clone, Debug)]
pub(crate) struct SignIn {
    /// The provider's latest instruction, such as the device code to enter.
    pub(crate) status: String,
    pub(crate) url: Option<String>,
}

/// One login on a lane.
#[derive(Clone, Debug)]
pub(crate) struct AccountView {
    pub(crate) kind: AccountKind,
    pub(crate) name: String,
    pub(crate) active: bool,
    /// This login's own quota windows, when the provider reports them.
    pub(crate) usage: Option<borg_remote::ProviderUsage>,
    /// Why usage is missing, when it is.
    pub(crate) detail: Option<String>,
}

/// Text typed for a lane's next login, kept between frames so a redraw never
/// erases what the user is entering.
#[derive(Default)]
pub(crate) struct AccountEditor {
    names: [String; Lane::ALL.len()],
    keys: [String; Lane::ALL.len()],
}

impl AccountEditor {
    pub(crate) fn name(&self, lane: Lane) -> &str {
        &self.names[lane_index(lane)]
    }

    pub(crate) fn key(&self, lane: Lane) -> &str {
        &self.keys[lane_index(lane)]
    }

    /// Forget the entry once a login has been connected: keeping a key in the
    /// screen after it was stored serves no one.
    pub(crate) fn clear(&mut self, lane: Lane) {
        let index = lane_index(lane);
        self.names[index].clear();
        self.keys[index].clear();
    }
}

pub(crate) fn lane_index(lane: Lane) -> usize {
    Lane::ALL
        .iter()
        .position(|candidate| *candidate == lane)
        .expect("every lane is in Lane::ALL")
}

fn row(
    id: SettingId,
    label: impl Into<std::borrow::Cow<'static, str>>,
    description: String,
    control: SettingControl,
) -> SettingRow {
    SettingRow {
        id,
        section: SettingsSection::Providers,
        label: label.into(),
        description,
        control,
        enabled: true,
    }
}

/// One short line per login, for the control on the right of its row.
pub(crate) fn capacity(account: &AccountView) -> String {
    if account.kind == AccountKind::ApiKey {
        return "API key · pay per use".to_string();
    }
    let Some(usage) = &account.usage else {
        return "no quota reported".to_string();
    };
    if usage.windows.is_empty() {
        return usage
            .detail
            .clone()
            .unwrap_or_else(|| "no quota reported".to_string());
    }
    usage
        .windows
        .iter()
        .map(|window| {
            let left = 100_u8.saturating_sub(window.used_percent.min(100));
            format!("{} {left}%", window.label)
        })
        .collect::<Vec<_>>()
        .join(" · ")
}

/// The focused row's description: the full picture for one login, including
/// when each window resets.
fn detail(account: &AccountView) -> String {
    let mut text = String::new();
    if let Some(usage) = &account.usage
        && !usage.windows.is_empty()
    {
        for window in &usage.windows {
            let left = 100_u8.saturating_sub(window.used_percent.min(100));
            if !text.is_empty() {
                text.push_str(" · ");
            }
            text.push_str(&format!("{} {left}% left", window.label));
            if let Some(reset) = window.resets_at {
                text.push_str(&format!(
                    " (resets {})",
                    reset.with_timezone(&Local).format("%H:%M on %d %b")
                ));
            }
        }
    } else {
        text.push_str(&capacity(account));
    }
    if let Some(detail) = &account.detail
        && !text.contains(detail)
    {
        text.push_str(&format!(" · {detail}"));
    }
    text
}

/// The overview: one row per provider saying which login it spends.
pub(crate) fn overview_rows(lanes: &[LaneView]) -> Vec<SettingRow> {
    lanes
        .iter()
        .map(|view| {
            let lane = view.lane;
            let active = view.accounts.iter().find(|account| account.active);
            let summary = match (active, view.accounts.len()) {
                (_, 0) => "Not connected".to_string(),
                (Some(account), _) => format!("{} · {}", account.name, capacity(account)),
                (None, 1) => "1 login, none selected".to_string(),
                (None, count) => format!("{count} logins, none selected"),
            };
            let subscriptions = view
                .accounts
                .iter()
                .filter(|account| account.kind == AccountKind::Subscription)
                .count();
            let keys = view.accounts.len() - subscriptions;
            row(
                SettingId::Lane(lane_index(lane)),
                lane.label(),
                format!(
                    "{} · {} · {}. Enter to add, select or remove logins.",
                    lane.label(),
                    plural(subscriptions, "subscription login"),
                    plural(keys, "API key")
                ),
                SettingControl::Action(if view.sign_in.is_some() {
                    "Signing in…".to_string()
                } else {
                    summary
                }),
            )
        })
        .collect()
}

fn plural(count: usize, noun: &str) -> String {
    match count {
        0 => format!("no {noun}s"),
        1 => format!("1 {noun}"),
        count => format!("{count} {noun}s"),
    }
}

/// The detail page of one provider. Every provider gets the same page; rows
/// for a kind of login the provider does not offer are simply absent.
pub(crate) fn detail_rows(
    view: &LaneView,
    editor: &AccountEditor,
    confirm_remove: Option<SettingId>,
) -> Vec<SettingRow> {
    use SettingId as Id;
    let lane = view.lane;
    let position = lane_index(lane);
    let label = lane.label();
    let mut rows = vec![row(
        Id::LaneBack,
        "Back to providers",
        "Return to every provider.".to_string(),
        SettingControl::Action("‹ Providers".to_string()),
    )];
    if let Some(sign_in) = &view.sign_in {
        rows.push(row(
            Id::LaneSignIn(position),
            "Signing in",
            format!(
                "{label} · {} · Borg keeps working while it waits, and the current login keeps working until the new one succeeds.",
                sign_in.status
            ),
            SettingControl::Action(if sign_in.url.is_some() {
                "Open sign-in page".to_string()
            } else {
                "Starting…".to_string()
            }),
        ));
    }
    for (index, account) in view.accounts.iter().enumerate() {
        let id = Id::LaneAccount(position, index);
        let removable = lane.keys(account.kind).is_some();
        let state = if account.active {
            "Turns on this provider use this login now."
        } else if account.kind == AccountKind::ApiKey && lane.has_subscriptions() {
            "Enter bills turns to this key instead of the subscription."
        } else {
            "Enter makes this the login turns spend."
        };
        rows.push(row(
            id,
            format!(
                "{} {}",
                if account.active { "●" } else { "○" },
                account.name
            ),
            format!(
                "{} · {state}{}",
                detail(account),
                if removable { " Delete removes it." } else { "" }
            ),
            SettingControl::Action(if confirm_remove == Some(id) {
                format!("Press Delete again to remove {}", account.name)
            } else {
                capacity(account)
            }),
        ));
    }
    let subscriptions = view
        .accounts
        .iter()
        .filter(|account| account.kind == AccountKind::Subscription)
        .count();
    if subscriptions > 1 {
        rows.push(row(
            Id::LaneStrategy(position),
            "Switching",
            match view.strategy {
                Strategy::Manual => "Manual keeps the subscription you pick until you pick another.",
                Strategy::Headroom => {
                    "Most headroom spends the subscription with the most quota left, switching only when the selected one runs out or falls well behind."
                }
                Strategy::Drain => {
                    "Soonest reset spends the subscription whose remaining allowance expires first, so quota that would be lost at its reset is used before quota that has longer to live. On a plan that reports no quota, turns rotate instead."
                }
            }
            .to_string()
                + " API keys are never switched to automatically.",
            SettingControl::Choice {
                options: Strategy::ALL
                    .iter()
                    .map(|strategy| strategy.label().to_string())
                    .collect(),
                selected: Strategy::ALL
                    .iter()
                    .position(|strategy| *strategy == view.strategy),
                disabled: vec![false; Strategy::ALL.len()],
            },
        ));
    }
    rows.push(row(
        Id::LaneName(position),
        "New login name",
        "Name for the next login: 1-40 letters, digits, '-' or '_'.".to_string(),
        SettingControl::Text(editor.name(lane).to_string()),
    ));
    let key_kinds: Vec<AccountKind> = [AccountKind::Subscription, AccountKind::ApiKey]
        .into_iter()
        .filter(|kind| lane.keys(*kind).is_some())
        .collect();
    if !key_kinds.is_empty() {
        rows.push(row(
            Id::LaneKey(position),
            "New login key",
            format!(
                "The key for that login. {}It is kept in Borg's credential store, never in a session or a journal.",
                if lane == Lane::OpenCode { "Get it at opencode.ai/auth. " } else { "" }
            ),
            SettingControl::Secret(if editor.key(lane).is_empty() {
                String::new()
            } else {
                "•".repeat(editor.key(lane).chars().count().min(24))
            }),
        ));
    }
    if lane.auth_provider().is_some() && view.sign_in.is_none() {
        rows.push(row(
            Id::LaneSignIn(position),
            "Add subscription",
            format!(
                "Sign in to another {label} subscription under the name above. Your browser opens; the current login keeps working until the new one succeeds."
            ),
            SettingControl::Action("Sign in with browser".to_string()),
        ));
    }
    for kind in key_kinds {
        let (label_text, control, description) = match kind {
            AccountKind::Subscription => (
                "Add subscription",
                "Save subscription key",
                format!(
                    "Connect another {label} subscription with the name and key above, and select it."
                ),
            ),
            AccountKind::ApiKey => (
                "Add API key",
                "Save API key",
                if lane.has_subscriptions() {
                    format!(
                        "Keep a pay-per-use {label} API key under the name above. Turns keep spending the subscription until you select the key."
                    )
                } else {
                    format!("Keep a {label} API key under the name above, and select it.")
                },
            ),
        };
        rows.push(row(
            Id::LaneSaveKey(position, kind),
            label_text,
            description,
            SettingControl::Action(control.to_string()),
        ));
    }
    rows
}

/// Accept the text a name or key field just committed. Both are kept in the
/// editor until a Save or Sign in row uses them.
pub(crate) fn edit(editor: &mut AccountEditor, id: SettingId, value: &str) -> Option<Result<()>> {
    match id {
        SettingId::LaneName(position) => {
            Lane::ALL.get(position)?;
            if value.is_empty() {
                editor.names[position].clear();
                return Some(Ok(()));
            }
            Some(
                borg_provider::subscription_accounts::validate_name(value).map(|name| {
                    editor.names[position] = name.to_string();
                }),
            )
        }
        SettingId::LaneKey(position) => {
            Lane::ALL.get(position)?;
            if !value.is_empty() {
                editor.keys[position] = value.to_string();
            }
            Some(Ok(()))
        }
        _ => None,
    }
}

fn view(lanes: &[LaneView], position: usize) -> Option<&LaneView> {
    let lane = *Lane::ALL.get(position)?;
    lanes.iter().find(|view| view.lane == lane)
}

/// The removal a Delete on this row asks for, if the row is a removable login.
pub(crate) fn removal(id: SettingId, lanes: &[LaneView]) -> Option<UiAction> {
    let SettingId::LaneAccount(position, index) = id else {
        return None;
    };
    let view = view(lanes, position)?;
    let account = view.accounts.get(index)?;
    view.lane.keys(account.kind)?;
    Some(UiAction::RemoveAccount {
        lane: view.lane,
        kind: account.kind,
        account: account.name.clone(),
    })
}

/// The action a row performs, if it acts on a login rather than a field.
pub(crate) fn action(
    id: SettingId,
    lanes: &[LaneView],
    editor: &AccountEditor,
) -> Option<UiAction> {
    match id {
        SettingId::LaneAccount(position, index) => {
            let view = view(lanes, position)?;
            let account = view.accounts.get(index)?;
            (!account.active).then(|| UiAction::SwitchAccount {
                lane: view.lane,
                kind: account.kind,
                account: account.name.clone(),
            })
        }
        SettingId::LaneSignIn(position) => {
            let view = view(lanes, position)?;
            if let Some(sign_in) = &view.sign_in {
                return sign_in
                    .url
                    .clone()
                    .map(|url| UiAction::TerminalIo(TerminalIoRequest::open_link(url)));
            }
            if let Some(busy) = lanes.iter().find(|view| view.sign_in.is_some()) {
                return Some(UiAction::Notice(format!(
                    "Finish the {} sign-in first.",
                    busy.lane.label()
                )));
            }
            Some(match named(view.lane, editor) {
                Ok(account) => UiAction::ConnectAccount {
                    lane: view.lane,
                    account,
                },
                Err(notice) => notice,
            })
        }
        SettingId::LaneSaveKey(position, kind) => {
            let view = view(lanes, position)?;
            let account = match named(view.lane, editor) {
                Ok(account) => account,
                Err(notice) => return Some(notice),
            };
            let key = editor.key(view.lane).trim();
            if key.is_empty() {
                return Some(UiAction::Notice(format!(
                    "Paste the {} key for that login first.",
                    view.lane.label()
                )));
            }
            Some(UiAction::AddKeyAccount {
                lane: view.lane,
                kind,
                account,
                key: key.to_string(),
            })
        }
        _ => None,
    }
}

fn named(lane: Lane, editor: &AccountEditor) -> std::result::Result<String, UiAction> {
    let name = editor.name(lane).trim();
    if name.is_empty() {
        return Err(UiAction::Notice(format!(
            "Type a name for the new {} login first.",
            lane.label()
        )));
    }
    Ok(name.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn usage(label: &str, used: u8) -> borg_remote::ProviderUsage {
        borg_remote::ProviderUsage {
            availability: borg_remote::ProviderUsageAvailability::Available,
            windows: vec![borg_remote::ProviderUsageWindow {
                label: label.to_string(),
                used_percent: used,
                resets_at: Some(
                    chrono::Utc
                        .with_ymd_and_hms(2026, 11, 8, 14, 59, 0)
                        .unwrap(),
                ),
                global: true,
            }],
            detail: None,
            plan: None,
        }
    }

    fn account(kind: AccountKind, name: &str, active: bool, used: Option<u8>) -> AccountView {
        AccountView {
            kind,
            name: name.to_string(),
            active,
            usage: used.map(|used| usage("5-hour", used)),
            detail: None,
        }
    }

    fn claude() -> LaneView {
        LaneView {
            lane: Lane::Claude,
            strategy: Strategy::Drain,
            accounts: vec![
                account(AccountKind::Subscription, "default", true, Some(100)),
                account(AccountKind::Subscription, "work", false, Some(38)),
                account(AccountKind::ApiKey, "default", false, None),
            ],
            sign_in: None,
        }
    }

    fn control(row: &SettingRow) -> &str {
        match &row.control {
            SettingControl::Action(value) => value,
            other => panic!("expected an action, got {other:?}"),
        }
    }

    // Every provider's page has to answer, at a glance, which login a turn
    // spends, what each one has left, and whether it bills per use.
    #[test]
    fn the_page_lists_subscriptions_and_keys_with_what_they_cost() {
        let rows = detail_rows(&claude(), &AccountEditor::default(), None);
        let accounts: Vec<_> = rows
            .iter()
            .filter(|row| matches!(row.id, SettingId::LaneAccount(..)))
            .collect();
        assert_eq!(accounts.len(), 3);
        assert_eq!(accounts[0].label, "● default");
        assert!(control(accounts[0]).contains("5-hour 0%"));
        assert_eq!(accounts[1].label, "○ work");
        assert!(control(accounts[1]).contains("5-hour 62%"));
        assert_eq!(control(accounts[2]), "API key · pay per use");
        // Selecting the key must say it changes billing.
        assert!(
            accounts[2]
                .description
                .contains("instead of the subscription")
        );
        assert!(rows.iter().any(|row| row.id == SettingId::LaneStrategy(0)));
        let overview = overview_rows(&[claude()]);
        assert_eq!(control(&overview[0]), "default · 5-hour 0%");
    }

    // A login of either kind is selected with the kind it has, so a Claude
    // login and an Anthropic key that share a name never select each other.
    #[test]
    fn selecting_a_login_keeps_its_kind() {
        let lanes = [claude()];
        let editor = AccountEditor::default();
        assert!(matches!(
            action(SettingId::LaneAccount(0, 2), &lanes, &editor),
            Some(UiAction::SwitchAccount { kind: AccountKind::ApiKey, account, .. }) if account == "default"
        ));
        assert!(action(SettingId::LaneAccount(0, 0), &lanes, &editor).is_none());
        // An OAuth login owns rotating tokens and cannot be removed here.
        assert!(removal(SettingId::LaneAccount(0, 0), &lanes).is_none());
        assert!(matches!(
            removal(SettingId::LaneAccount(0, 2), &lanes),
            Some(UiAction::RemoveAccount {
                kind: AccountKind::ApiKey,
                ..
            })
        ));
    }

    // While a browser sign-in is pending, its lane offers the page again
    // instead of a second sign-in, and no other lane can start one.
    #[test]
    fn a_pending_sign_in_reopens_its_page_and_blocks_another() {
        let mut lanes = vec![claude()];
        lanes[0].sign_in = Some(SignIn {
            status: "Open https://claude.ai/x to connect".to_string(),
            url: Some("https://claude.ai/x".to_string()),
        });
        lanes.push(LaneView {
            lane: Lane::Codex,
            strategy: Strategy::Drain,
            accounts: Vec::new(),
            sign_in: None,
        });
        let mut editor = AccountEditor::default();
        editor.names[lane_index(Lane::Codex)] = "work".to_string();
        let claude = SettingId::LaneSignIn(lane_index(Lane::Claude));
        let row = detail_rows(&lanes[0], &editor, None)
            .into_iter()
            .find(|row| row.id == claude)
            .unwrap();
        assert!(row.description.contains("https://claude.ai/x"));
        assert!(matches!(
            action(claude, &lanes, &editor),
            Some(UiAction::TerminalIo(_))
        ));
        let codex = SettingId::LaneSignIn(lane_index(Lane::Codex));
        assert!(matches!(
            action(codex, &lanes, &editor),
            Some(UiAction::Notice(notice)) if notice.contains("Claude")
        ));
    }

    // Adding a login must not fire with half the input; a key needs both the
    // name and the key before anything is written.
    #[test]
    fn adding_a_key_requires_its_name_and_key() {
        let lanes = [LaneView {
            lane: Lane::OpenRouter,
            strategy: Strategy::Drain,
            accounts: Vec::new(),
            sign_in: None,
        }];
        let position = lane_index(Lane::OpenRouter);
        let save = SettingId::LaneSaveKey(position, AccountKind::ApiKey);
        let mut editor = AccountEditor::default();
        assert!(matches!(
            action(save, &lanes, &editor),
            Some(UiAction::Notice(_))
        ));
        edit(&mut editor, SettingId::LaneName(position), "home")
            .unwrap()
            .unwrap();
        assert!(matches!(
            action(save, &lanes, &editor),
            Some(UiAction::Notice(_))
        ));
        edit(&mut editor, SettingId::LaneKey(position), "sk-or")
            .unwrap()
            .unwrap();
        assert!(matches!(
            action(save, &lanes, &editor),
            Some(UiAction::AddKeyAccount { account, key, .. }) if account == "home" && key == "sk-or"
        ));
        // OpenRouter has no subscription, so it offers no sign-in or switching.
        let rows = detail_rows(&lanes[0], &editor, None);
        assert!(!rows.iter().any(|row| matches!(
            row.id,
            SettingId::LaneSignIn(_) | SettingId::LaneStrategy(_)
        )));
    }
}
