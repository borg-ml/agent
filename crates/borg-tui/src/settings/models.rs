//! The Models settings page: the models a user pinned, and the ordered chain a
//! session falls back through when a route reaches its usage limit.

use super::*;

/// What a fallback row offers. Ordered as drawn, so `index` maps directly.
/// The segments after the route. Index 1 in a row is "Move up" because the
/// route itself occupies segment 0.
const FALLBACK_MOVES: [&str; 3] = ["↑ up", "↓ down", "✕ remove"];

pub(crate) fn list_rows(
    pinned: &[String],
    chain: &[String],
    editable: bool,
    new_route: &str,
) -> Vec<SettingRow> {
    use SettingId as Id;
    let mut rows = Vec::new();
    if pinned.is_empty() {
        rows.push(SettingRow {
            id: Id::ModelPin(usize::MAX),
            section: SettingsSection::Models,
            label: "Pinned models".into(),
            description: format!(
                "No models pinned yet. Open the model picker and press {} on a model to pin it here, first in every picker on this machine.",
                borg_ui::control_shortcut('p')
            ),
            control: SettingControl::Action("None".to_string()),
            enabled: false,
        });
    }
    for (index, model) in pinned.iter().enumerate() {
        rows.push(SettingRow {
            id: Id::ModelPin(index),
            section: SettingsSection::Models,
            label: "Pinned model".into(),
            description: format!(
                "{model} · pinned, so it appears first in every model picker. Press Enter to unpin it."
            ),
            control: SettingControl::Action(model.clone()),
            enabled: true,
        });
    }
    if chain.is_empty() {
        rows.push(SettingRow {
            id: Id::FallbackEntry(usize::MAX),
            section: SettingsSection::Models,
            label: "Fallback chain".into(),
            description: "No fallback chain. A session runs the first route that has quota; when that route reaches its usage limit it continues on the next one, and only waits when every route is limited.".to_string(),
            control: SettingControl::Action("Empty".to_string()),
            enabled: false,
        });
    }
    for (index, spec) in chain.iter().enumerate() {
        // The route is the first segment, so the chain reads top to bottom at a
        // glance; the segments after it are the one-shot moves. The value
        // segment is disabled because choosing it would mean nothing.
        let mut options = vec![format!("{}. {spec}", index + 1)];
        options.extend(FALLBACK_MOVES.iter().map(|name| (*name).to_string()));
        let mut disabled = vec![true, false, false, false];
        if index == 0 {
            disabled[1] = true;
        }
        if index + 1 == chain.len() {
            disabled[2] = true;
        }
        if !editable {
            options = vec![
                format!("{}. {spec}", index + 1),
                "edit in agent.toml".into(),
            ];
            disabled = vec![true, true];
        }
        rows.push(SettingRow {
            id: Id::FallbackEntry(index),
            section: SettingsSection::Models,
            label: "Fallback".into(),
            description: format!(
                "Step {} of {}: {spec}. A turn runs the first route with quota and continues down the chain when a route hits its usage limit. “↑ up” moves it earlier, “↓ down” later, “✕ remove” drops it.",
                index + 1,
                chain.len()
            ),
            control: SettingControl::Choice {
                selected: Some(0),
                options,
                disabled,
            },
            enabled: editable,
        });
    }
    if editable {
        rows.push(SettingRow {
            id: Id::FallbackNew,
            section: SettingsSection::Models,
            label: "New fallback".into(),
            description: "A route to add at the end of the chain: a model id, `model@effort`, or `provider/model@effort`.".to_string(),
            control: SettingControl::Text(new_route.to_string()),
            enabled: true,
        });
        rows.push(SettingRow {
            id: Id::FallbackAdd,
            section: SettingsSection::Models,
            label: "Add fallback".into(),
            description: "Append the route above to the chain. It is saved to the agent configuration, so sessions started afterwards use it."
                .to_string(),
            control: SettingControl::Action("Add".to_string()),
            enabled: true,
        });
    }
    rows
}

/// The chain after moving, removing or appending one entry.
pub(crate) fn chain_action(
    id: SettingId,
    chain: &[String],
    new_route: &str,
    index: Option<usize>,
) -> Option<UiAction> {
    match id {
        SettingId::ModelPin(_) => None,
        SettingId::FallbackEntry(position) => {
            let position = position.min(chain.len().saturating_sub(1));
            let choice = index?;
            let mut next = chain.to_vec();
            match choice {
                1 if position > 0 => {
                    next.swap(position, position - 1);
                }
                2 if position + 1 < next.len() => {
                    next.swap(position, position + 1);
                }
                3 => {
                    next.remove(position);
                }
                _ => return Some(UiAction::None),
            }
            Some(UiAction::SetModelFallback(next))
        }
        SettingId::FallbackAdd => {
            let spec = new_route.trim();
            if spec.is_empty() {
                return Some(UiAction::Notice(
                    "Type a model route to add, such as claude-opus-5-5@max.".to_string(),
                ));
            }
            if let Err(error) = borg_remote::ModelRoute::parse(spec) {
                return Some(UiAction::Notice(format!("Invalid model route: {error}")));
            }
            let mut next = chain.to_vec();
            if next.iter().any(|existing| existing == spec) {
                return Some(UiAction::Notice(format!("{spec} is already in the chain.")));
            }
            next.push(spec.to_string());
            Some(UiAction::SetModelFallback(next))
        }
        _ => None,
    }
}

/// Accept the typed route so the next frame draws it.
pub(crate) fn edit(state: &mut FallbackEditor, id: SettingId, value: &str) -> Option<()> {
    if id != SettingId::FallbackNew {
        return None;
    }
    state.new_route = value.to_string();
    Some(())
}

/// The route being typed on the Models page, kept between frames.
#[derive(Default)]
pub(crate) struct FallbackEditor {
    pub(crate) new_route: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chain() -> Vec<String> {
        vec![
            "claude-opus-5-5@max".to_string(),
            "gpt-6-sol@xhigh".to_string(),
        ]
    }

    // Every move a user can reach must produce the next chain, and the ends of
    // the chain must not offer a move that would do nothing.
    #[test]
    fn the_chain_can_be_reordered_and_trimmed() {
        let rows = list_rows(&[], &chain(), true, "");
        let reorder = rows
            .iter()
            .find(|row| row.id == SettingId::FallbackEntry(0))
            .expect("first entry");
        let SettingControl::Choice { disabled, .. } = &reorder.control else {
            panic!("a fallback row is a choice");
        };
        assert!(disabled[1], "the first entry cannot move up");
        assert!(!disabled[2]);

        let moved = chain_action(SettingId::FallbackEntry(1), &chain(), "", Some(1));
        assert!(matches!(
            moved,
            Some(UiAction::SetModelFallback(next)) if next == ["gpt-6-sol@xhigh", "claude-opus-5-5@max"]
        ));
        let removed = chain_action(SettingId::FallbackEntry(0), &chain(), "", Some(3));
        assert!(matches!(
            removed,
            Some(UiAction::SetModelFallback(next)) if next == ["gpt-6-sol@xhigh"]
        ));
    }

    #[test]
    fn adding_a_route_validates_it_before_writing() {
        assert!(matches!(
            chain_action(SettingId::FallbackAdd, &chain(), "", None),
            Some(UiAction::Notice(_))
        ));
        assert!(matches!(
            chain_action(SettingId::FallbackAdd, &chain(), "not a/model", None),
            Some(UiAction::Notice(_))
        ));
        assert!(matches!(
            chain_action(
                SettingId::FallbackAdd,
                &chain(),
                "claude-opus-5-5@max",
                None
            ),
            Some(UiAction::Notice(_))
        ));
        assert!(matches!(
            chain_action(SettingId::FallbackAdd, &chain(), "opencode-go/deepseek-v4.1", None),
            Some(UiAction::SetModelFallback(next)) if next.len() == 3
        ));
    }

    // A `chain:` or table chain is hand-edited, so the page must say so rather
    // than offering moves that would flatten it.
    #[test]
    fn an_uneditable_chain_offers_no_moves() {
        let rows = list_rows(&[], &chain(), false, "");
        assert!(
            rows.iter()
                .all(|row| row.id != SettingId::FallbackNew && row.id != SettingId::FallbackAdd)
        );
        assert!(
            rows.iter()
                .filter(|row| matches!(row.id, SettingId::FallbackEntry(_)))
                .all(|row| !row.enabled)
        );
    }
}
