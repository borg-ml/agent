//! The Subscriptions settings page: every subscription lane, its logins, what
//! each one has left, and whether turns may switch between them.

use super::*;
use borg_provider::subscriptions::{Lane, Strategy};

/// One lane as the page draws it, read from the host's credential store and
/// the provider capability snapshot.
#[derive(Clone, Debug)]
pub(crate) struct LaneView {
    pub(crate) lane: Lane,
    pub(crate) strategy: Strategy,
    pub(crate) accounts: Vec<AccountView>,
    /// A sign-in running for this lane, drawn in place of its `Add login` row.
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
    /// Lanes whose add-login form is open.
    open: [bool; Lane::ALL.len()],
}

impl AccountEditor {
    pub(crate) fn open(&self, lane: Lane) -> bool {
        self.open[lane_index(lane)]
    }

    /// Open or close a lane's add-login form; closing it forgets the entry.
    pub(crate) fn toggle(&mut self, lane_position: usize) {
        let Some(lane) = Lane::ALL.get(lane_position) else {
            return;
        };
        if self.open[lane_position] {
            self.clear(*lane);
        } else {
            self.open[lane_position] = true;
        }
    }

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
        self.open[index] = false;
    }
}

fn lane_index(lane: Lane) -> usize {
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
        section: SettingsSection::Subscriptions,
        label: label.into(),
        description,
        control,
        enabled: true,
    }
}

/// One short line per login, for the control on the right of its row.
pub(crate) fn capacity(account: &AccountView) -> String {
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

pub(crate) fn list_rows(lanes: &[LaneView], editor: &AccountEditor) -> Vec<SettingRow> {
    use SettingId as Id;
    let mut rows = Vec::new();
    for view in lanes {
        let lane = view.lane;
        // Rows are keyed by the lane's own position, not its position in the
        // slice, so a row always acts on the lane it names.
        let lane_position = lane_index(lane);
        let count = view.accounts.len();
        let adding = editor.open(lane);
        let key_lane = lane.key_lane().is_some();
        // The lane's heading row opens and closes its add-login form, so a
        // provider with no logins costs one row.
        rows.push(row(
            Id::SubscriptionAdd(lane_position),
            lane.label(),
            match (adding, count) {
                (true, _) => format!("Close the form for a new {} login.", lane.label()),
                (false, 0) => format!("Connect a {} subscription.", lane.label()),
                (false, _) => format!(
                    "Connect another {} subscription alongside {}.",
                    lane.label(),
                    match count {
                        1 => "the current login".to_string(),
                        count => format!("the {count} current logins"),
                    }
                ),
            },
            SettingControl::Action(
                match (adding, count) {
                    (true, _) => "Cancel",
                    (false, 0) => "Connect",
                    (false, _) => "Add login",
                }
                .to_string(),
            ),
        ));
        if let Some(sign_in) = &view.sign_in {
            rows.push(row(
                Id::SubscriptionConnect(lane_position),
                "  Signing in",
                format!(
                    "{} · {} · Borg keeps working while it waits, and the current login keeps working until the new one succeeds.",
                    lane.label(),
                    sign_in.status
                ),
                SettingControl::Action(if sign_in.url.is_some() {
                    "Open sign-in page".to_string()
                } else {
                    "Starting…".to_string()
                }),
            ));
        } else if adding {
            rows.push(row(
                Id::SubscriptionName(lane_position),
                "  Name",
                format!(
                    "Name for the new {} login: 1-40 letters, digits, '-' or '_'.",
                    lane.label()
                ),
                SettingControl::Text(editor.name(lane).to_string()),
            ));
            if key_lane {
                rows.push(row(
                    Id::SubscriptionKey(lane_position),
                    "  Key",
                    format!(
                        "The {} key for that login. {}It is kept in Borg's credential store, never in a session or a journal.",
                        lane.label(),
                        if lane == Lane::OpenCode { "Get it at opencode.ai/auth. " } else { "" }
                    ),
                    SettingControl::Secret(if editor.key(lane).is_empty() {
                        String::new()
                    } else {
                        "•".repeat(editor.key(lane).chars().count().min(24))
                    }),
                ));
            }
            rows.push(row(
                Id::SubscriptionConnect(lane_position),
                "",
                if key_lane {
                    format!(
                        "Connect the {} login with the name and key above.",
                        lane.label()
                    )
                } else {
                    format!(
                        "Sign in to {} under the name above. Your browser opens; the current login keeps working until the new one succeeds.",
                        lane.label()
                    )
                },
                SettingControl::Action(
                    if key_lane { "Save login" } else { "Sign in" }.to_string(),
                ),
            ));
        }
        for (account_position, account) in view.accounts.iter().enumerate() {
            let state = if account.active {
                "Turns on this provider use this login now."
            } else {
                "Press Enter to make this the login turns spend."
            };
            rows.push(row(
                Id::SubscriptionAccount(lane_position, account_position),
                format!(
                    "  {} {}",
                    if account.active { "●" } else { "○" },
                    account.name
                ),
                format!(
                    "{} login {} · {} · {state}",
                    lane.label(),
                    account.name,
                    detail(account)
                ),
                SettingControl::Action(capacity(account)),
            ));
        }
        if count > 1 {
            rows.push(row(
                Id::SubscriptionStrategy(lane_position),
                "  Switching",
                match view.strategy {
                    Strategy::Manual => {
                        "Manual keeps the login you pick until you pick another."
                    }
                    Strategy::Headroom => {
                        "Most headroom spends the login with the most quota left, switching only when the selected one runs out or falls well behind."
                    }
                    Strategy::Drain => {
                        "Soonest reset spends the login whose remaining allowance expires first, so quota that would be lost at its reset is used before quota that has longer to live. On a plan that reports no quota, turns rotate instead."
                    }
                }
                .to_string(),
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
            if key_lane && let Some(active) = view.accounts.iter().find(|account| account.active) {
                rows.push(row(
                    Id::SubscriptionRemove(lane_position),
                    "  Remove login",
                    format!(
                        "Disconnect the {} login named {} from this machine. Another connected login takes over.",
                        lane.label(),
                        active.name
                    ),
                    SettingControl::Action(format!("Remove {}", active.name)),
                ));
            }
        }
    }
    rows
}

/// Accept the text a `Name` or `Key` field just committed.
/// Both are kept in the editor so `Sign in` or `Save login` can use them, so there is
/// nothing to send yet.
pub(crate) fn edit(editor: &mut AccountEditor, id: SettingId, value: &str) -> Option<Result<()>> {
    match id {
        SettingId::SubscriptionName(lane_position) => {
            Lane::ALL.get(lane_position)?;
            if value.is_empty() {
                editor.names[lane_position].clear();
                return Some(Ok(()));
            }
            Some(
                borg_provider::subscription_accounts::validate_name(value).map(|name| {
                    editor.names[lane_position] = name.to_string();
                }),
            )
        }
        SettingId::SubscriptionKey(lane_position) => {
            Lane::ALL.get(lane_position)?;
            if !value.is_empty() {
                editor.keys[lane_position] = value.to_string();
            }
            Some(Ok(()))
        }
        _ => None,
    }
}

/// The action a row performs, if it acts on a login rather than a field.
pub(crate) fn action(
    id: SettingId,
    lanes: &[LaneView],
    editor: &AccountEditor,
) -> Option<UiAction> {
    let view = |lane_position: usize| {
        let lane = *Lane::ALL.get(lane_position)?;
        lanes.iter().find(|view| view.lane == lane)
    };
    match id {
        SettingId::SubscriptionAccount(lane_position, account_position) => {
            let view = view(lane_position)?;
            let account = view.accounts.get(account_position)?;
            (!account.active).then(|| UiAction::SwitchAccount {
                lane: view.lane,
                account: account.name.clone(),
            })
        }
        SettingId::SubscriptionRemove(lane_position) => {
            let view = view(lane_position)?;
            let active = view.accounts.iter().find(|account| account.active)?;
            Some(UiAction::RemoveAccount {
                lane: view.lane,
                account: active.name.clone(),
            })
        }
        SettingId::SubscriptionConnect(lane_position) => {
            let view = view(lane_position)?;
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
            let name = editor.name(view.lane).trim();
            if name.is_empty() {
                return Some(UiAction::Notice(format!(
                    "Type a name for the new {} login first.",
                    view.lane.label()
                )));
            }
            if view.lane.key_lane().is_some() {
                let key = editor.key(view.lane).trim();
                if key.is_empty() {
                    return Some(UiAction::Notice(format!(
                        "Paste the {} key for that login first.",
                        view.lane.label()
                    )));
                }
                return Some(UiAction::AddKeyAccount {
                    lane: view.lane,
                    account: name.to_string(),
                    key: key.to_string(),
                });
            }
            Some(UiAction::ConnectAccount {
                lane: view.lane,
                account: name.to_string(),
            })
        }
        _ => None,
    }
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

    fn lanes() -> Vec<LaneView> {
        vec![
            LaneView {
                lane: Lane::Claude,
                strategy: Strategy::Drain,
                accounts: vec![
                    AccountView {
                        name: "default".to_string(),
                        active: true,
                        usage: Some(usage("5-hour", 100)),
                        detail: None,
                    },
                    AccountView {
                        name: "work".to_string(),
                        active: false,
                        usage: Some(usage("5-hour", 38)),
                        detail: None,
                    },
                ],
                sign_in: None,
            },
            LaneView {
                lane: Lane::OpenCode,
                strategy: Strategy::Drain,
                accounts: vec![AccountView {
                    name: "default".to_string(),
                    active: true,
                    usage: None,
                    detail: Some(
                        "this plan reports no quota; turns rotate between its logins".to_string(),
                    ),
                }],
                sign_in: None,
            },
        ]
    }

    // While a browser sign-in is pending, its lane offers the page again
    // instead of a second sign-in, and no other lane can start one.
    #[test]
    fn a_pending_sign_in_reopens_its_page_and_blocks_another() {
        let mut lanes = lanes();
        lanes[0].sign_in = Some(SignIn {
            status: "Open https://claude.ai/x to connect".to_string(),
            url: Some("https://claude.ai/x".to_string()),
        });
        let mut editor = AccountEditor::default();
        editor.names[lane_index(Lane::OpenCode)] = "work".to_string();
        let claude = SettingId::SubscriptionConnect(lane_index(Lane::Claude));
        let row = list_rows(&lanes, &editor)
            .into_iter()
            .find(|row| row.id == claude)
            .unwrap();
        assert!(row.description.contains("https://claude.ai/x"));
        assert!(matches!(
            action(claude, &lanes, &editor),
            Some(UiAction::TerminalIo(_))
        ));
        let opencode = SettingId::SubscriptionConnect(lane_index(Lane::OpenCode));
        assert!(matches!(
            action(opencode, &lanes, &editor),
            Some(UiAction::Notice(notice)) if notice.contains("Claude")
        ));
    }

    // The page has to answer two questions per provider at a glance: which
    // login a turn spends, and what each one has left.
    #[test]
    fn the_page_shows_each_login_with_its_remaining_quota() {
        let rows = list_rows(&lanes(), &AccountEditor::default());
        let accounts = rows
            .iter()
            .filter(|row| matches!(row.id, SettingId::SubscriptionAccount(..)))
            .map(|row| {
                (
                    format!("{} {}", row.label, row.description),
                    match &row.control {
                        SettingControl::Action(value) => value.clone(),
                        other => panic!("an account row must be an action, got {other:?}"),
                    },
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(accounts.len(), 3);
        assert!(accounts[0].0.contains("● default") && accounts[0].1.contains("5-hour 0%"));
        assert!(accounts[1].0.contains("○ work") && accounts[1].1.contains("5-hour 62%"));
        assert!(accounts[2].1.contains("no quota reported"));
        assert!(
            rows.iter()
                .any(|row| row.id == SettingId::SubscriptionStrategy(0))
        );
    }

    // Every strategy the page offers must be reachable: a drawn choice with no
    // action is a dead control that silently leaves the old policy in place.
    #[test]
    fn every_switching_choice_selects_its_strategy() {
        for row in list_rows(&lanes(), &AccountEditor::default()) {
            let SettingControl::Choice { options, .. } = &row.control else {
                continue;
            };
            let SettingId::SubscriptionStrategy(lane) = row.id else {
                continue;
            };
            for (index, option) in options.iter().enumerate() {
                match choice_action(&row, index) {
                    Some(UiAction::SetAccountStrategy {
                        lane: chosen,
                        strategy,
                    }) => {
                        assert_eq!(chosen, Lane::ALL[lane]);
                        assert_eq!(strategy.label(), option);
                    }
                    other => panic!("{option} has no strategy action: {other:?}"),
                }
            }
        }
    }

    // Adding a login must not fire with half the input; a named key plan needs
    // both the name and the key before anything is written.
    #[test]
    fn adding_a_login_requires_its_name_and_key() {
        let mut editor = AccountEditor::default();
        assert!(matches!(
            action(SettingId::SubscriptionConnect(0), &lanes(), &editor),
            Some(UiAction::Notice(_))
        ));
        edit(&mut editor, SettingId::SubscriptionName(0), "work")
            .unwrap()
            .unwrap();
        assert!(matches!(
            action(SettingId::SubscriptionConnect(0), &lanes(), &editor),
            Some(UiAction::ConnectAccount { account, .. }) if account == "work"
        ));
        // OpenCode Go is billed by a key, so its name alone is not enough.
        // Rows are keyed by the lane's own position in `Lane::ALL`.
        edit(&mut editor, SettingId::SubscriptionName(2), "home")
            .unwrap()
            .unwrap();
        assert!(matches!(
            action(SettingId::SubscriptionConnect(2), &lanes(), &editor),
            Some(UiAction::Notice(_))
        ));
        edit(&mut editor, SettingId::SubscriptionKey(2), "sk-go")
            .unwrap()
            .unwrap();
        assert!(matches!(
            action(SettingId::SubscriptionConnect(2), &lanes(), &editor),
            Some(UiAction::AddKeyAccount { account, key, .. }) if account == "home" && key == "sk-go"
        ));
    }
}
