use super::*;

const KEY_HINT_LABELS: &str = "1234567890";

#[derive(Clone, Debug, PartialEq, Eq)]
struct KeyHintTarget {
    area: Rect,
    point: Position,
    badge: Rect,
    identity: String,
}

#[derive(Default)]
pub(super) struct KeyHints {
    frame: Vec<KeyHintTarget>,
    activated_key: Option<KeyCode>,
    pub(super) active: Option<KeyHintSession>,
}

pub(super) struct KeyHintSession {
    targets: Vec<KeyHintTarget>,
    held: Option<KeyCode>,
    stale: bool,
    captured: bool,
    invalid_code: bool,
}

impl KeyHints {
    pub(super) fn observe_event(&mut self, event: &Event) {
        match event {
            Event::FocusLost => {
                self.active = None;
                self.activated_key = None;
            }
            Event::Resize(..) | Event::Paste(_) => self.invalidate(),
            Event::Mouse(mouse) if mouse.kind != MouseEventKind::Moved => self.invalidate(),
            _ => {}
        }
    }

    pub(super) fn invalidate(&mut self) {
        if let Some(active) = &mut self.active {
            active.stale = true;
        }
    }

    fn start(&mut self, held: Option<KeyCode>) {
        self.active = Some(KeyHintSession {
            targets: self.frame.clone(),
            held,
            stale: false,
            captured: false,
            invalid_code: false,
        });
    }

    pub(super) fn render(&mut self, frame: &mut ratatui::Frame, candidates: Vec<(Rect, String)>) {
        let viewport = frame.area();
        let mut targets: Vec<KeyHintTarget> = Vec::new();
        let badge_width = 1;
        for (area, identity) in candidates {
            let area = area.intersection(viewport);
            if area.is_empty()
                || badge_width > viewport.width
                || targets.iter().any(|target| target.identity == identity)
            {
                continue;
            }
            // Skip whole occluding intervals rather than scanning every terminal cell.
            let placement = (area.y..area.bottom()).find_map(|y| {
                let mut x = area.x;
                while x < area.right() {
                    let point = Position::new(x, y);
                    if let Some(blocker) = targets.iter().find(|target| target.area.contains(point))
                    {
                        x = blocker.area.right();
                        continue;
                    }
                    let badge = Rect::new(x.min(viewport.right() - badge_width), y, badge_width, 1);
                    if let Some(blocker) =
                        targets.iter().find(|target| target.badge.intersects(badge))
                    {
                        x = blocker.badge.right().max(x.saturating_add(1));
                        continue;
                    }
                    return Some((point, badge));
                }
                None
            });
            if let Some((point, badge)) = placement {
                targets.push(KeyHintTarget {
                    area,
                    point,
                    badge,
                    identity,
                });
            }
        }
        targets.sort_by_key(|target| (target.point.y, target.point.x));
        targets.truncate(KEY_HINT_LABELS.len());
        if let Some(active) = &mut self.active {
            if !active.captured {
                active.targets = targets.clone();
                active.captured = true;
            } else {
                active.stale |= active.targets != targets;
            }
            if !active.stale {
                for (index, target) in targets.iter().enumerate() {
                    let label = KEY_HINT_LABELS.as_bytes()[index] as char;
                    let label = label.to_string();
                    frame.render_widget(
                        Paragraph::new(label).style(
                            Style::default()
                                .fg(Color::Black)
                                .bg(Color::Yellow)
                                .add_modifier(Modifier::BOLD),
                        ),
                        target.badge,
                    );
                }
            }
            let label = if active.stale {
                "Hints changed · release / F12 to retry".to_string()
            } else if active.invalid_code {
                "No such target · 1–9 / 0 · Esc/F12 cancel".to_string()
            } else {
                "1–9 / 0 activate · Esc/F12 cancel".to_string()
            };
            if viewport.height > 0 {
                let y = viewport.bottom() - 1;
                let end = targets
                    .iter()
                    .filter(|target| target.area.y <= y && target.area.bottom() > y)
                    .map(|target| target.area.x)
                    .min()
                    .unwrap_or(viewport.right());
                let area = Rect::new(viewport.x, y, end.saturating_sub(viewport.x), 1);
                if area.width > 0 {
                    frame.render_widget(Clear, area);
                    frame.render_widget(
                        Paragraph::new(label)
                            .style(Style::default().fg(Color::Yellow).bg(Color::Black)),
                        area,
                    );
                }
            }
        }
        self.frame = targets;
    }
}

enum HintKey {
    Pass,
    Consumed,
    Click(Position),
}

impl KeyHints {
    fn key(&mut self, key: KeyEvent) -> HintKey {
        if self.activated_key == Some(key.code) {
            if key.kind == KeyEventKind::Repeat {
                return HintKey::Consumed;
            }
            self.activated_key = None;
        }
        use crossterm::event::ModifierKeyCode;
        let modifier = matches!(
            key.code,
            KeyCode::Modifier(
                ModifierKeyCode::LeftControl
                    | ModifierKeyCode::RightControl
                    | ModifierKeyCode::LeftSuper
                    | ModifierKeyCode::RightSuper
            )
        );
        if modifier {
            if key.kind == KeyEventKind::Release {
                if self
                    .active
                    .as_ref()
                    .is_some_and(|active| active.held == Some(key.code))
                {
                    self.active = None;
                }
            } else if key.kind == KeyEventKind::Press && self.active.is_none() {
                self.start(Some(key.code));
            }
            return HintKey::Consumed;
        }
        if key.kind == KeyEventKind::Release {
            return HintKey::Pass;
        }
        if key.code == KeyCode::F(12) && key.modifiers.is_empty() {
            if key.kind == KeyEventKind::Press {
                if self.active.is_some() {
                    self.active = None;
                } else {
                    self.start(None);
                }
            }
            return HintKey::Consumed;
        }
        let Some(active) = &mut self.active else {
            return HintKey::Pass;
        };
        if !key
            .modifiers
            .intersects(KeyModifiers::ALT | KeyModifiers::SHIFT)
        {
            match key.code {
                KeyCode::Char(digit) if digit.is_ascii_digit() => {
                    if key.kind != KeyEventKind::Press || !active.captured || active.stale {
                        return HintKey::Consumed;
                    }
                    let label = digit;
                    let index = KEY_HINT_LABELS.find(label).expect("hint label");
                    if let Some(target) = active.targets.get(index) {
                        let point = target.point;
                        self.active = None;
                        self.activated_key = Some(key.code);
                        return HintKey::Click(point);
                    }
                    active.invalid_code = true;
                    return HintKey::Consumed;
                }
                KeyCode::Esc => {
                    self.active = None;
                    return HintKey::Consumed;
                }
                _ => {}
            }
        }
        self.active = None;
        HintKey::Pass
    }
}

impl BorgTerminal {
    pub(super) fn handle_key_hint(&mut self, key: KeyEvent) -> Result<Option<UiAction>> {
        if matches!(key.code, KeyCode::Char(digit) if digit.is_ascii_digit())
            && key.kind == KeyEventKind::Press
            && self.key_hints.active.is_some()
        {
            // Validate against the frame about to receive the click, not a cached layout.
            self.draw_for_interaction()?;
        }
        match self.key_hints.key(key) {
            HintKey::Pass => Ok(None),
            HintKey::Consumed => {
                if self
                    .key_hints
                    .active
                    .as_ref()
                    .is_some_and(|active| !active.captured)
                {
                    self.draw_for_interaction()?;
                }
                Ok(Some(UiAction::None))
            }
            HintKey::Click(point) => {
                let down = self.handle_event(TerminalInputEvent {
                    event: Event::Mouse(MouseEvent {
                        kind: MouseEventKind::Down(MouseButton::Left),
                        column: point.x,
                        row: point.y,
                        modifiers: KeyModifiers::NONE,
                    }),
                    scroll_repetitions: 1,
                })?;
                let up = self.handle_event(TerminalInputEvent {
                    event: Event::Mouse(MouseEvent {
                        kind: MouseEventKind::Up(MouseButton::Left),
                        column: point.x,
                        row: point.y,
                        modifiers: KeyModifiers::NONE,
                    }),
                    scroll_repetitions: 1,
                })?;
                Ok(Some(if matches!(down, UiAction::None) {
                    up
                } else {
                    down
                }))
            }
        }
    }
}

impl Transcript {
    pub(super) fn key_hint_identity(&self, index: usize) -> String {
        if let Some(id) = self.message_id_at(index) {
            return format!("message:{id}");
        }
        let mut tools: Vec<_> = self
            .tools
            .iter()
            .filter_map(|(id, row)| (*row == index).then_some(id.as_str()))
            .collect();
        tools.sort_unstable();
        if !tools.is_empty() {
            return format!(
                "tool:{tools:?}:{}:{:?}",
                self.tool_is_expanded(index),
                self.tool_click_behavior
            );
        }
        use std::hash::{Hash, Hasher};
        let mut hash = std::collections::hash_map::DefaultHasher::new();
        match self.order.get(index) {
            Some(TranscriptEntry::Action {
                kind,
                label,
                detail,
                body,
                time,
                state,
                expanded,
            }) => {
                format!("{kind:?}:{state:?}:{expanded}").hash(&mut hash);
                (label, detail, body, time).hash(&mut hash);
            }
            Some(TranscriptEntry::Plan {
                items,
                previous,
                time,
                expanded,
            }) => {
                format!("{items:?}:{previous:?}:{time}:{expanded}").hash(&mut hash);
            }
            Some(TranscriptEntry::Goal { goal, .. }) => {
                goal.id.hash(&mut hash);
            }
            Some(TranscriptEntry::Compaction {
                sequence,
                expanded,
                complete,
                summary,
                ..
            }) => {
                (sequence, expanded, complete, summary).hash(&mut hash);
            }
            Some(TranscriptEntry::Info { title, text, time }) => {
                (title, text, time).hash(&mut hash);
            }
            Some(TranscriptEntry::Activity { text, time }) => {
                (text, time).hash(&mut hash);
            }
            _ => {}
        }
        format!("entry:{}", hash.finish())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::ModifierKeyCode;
    use ratatui::backend::TestBackend;

    fn render(hints: &mut KeyHints, candidates: Vec<(Rect, String)>) {
        let mut terminal = Terminal::new(TestBackend::new(30, 20)).unwrap();
        terminal
            .draw(|frame| hints.render(frame, candidates))
            .unwrap();
    }

    #[test]
    fn key_hints_hold_release_and_shortcuts() {
        let mut hints = KeyHints::default();
        let control = KeyCode::Modifier(ModifierKeyCode::LeftControl);
        assert!(matches!(
            hints.key(KeyEvent::new(control, KeyModifiers::CONTROL)),
            HintKey::Consumed
        ));
        assert!(hints.active.is_some());
        assert!(matches!(
            hints.key(KeyEvent::new(KeyCode::Enter, KeyModifiers::CONTROL)),
            HintKey::Pass
        ));
        hints.key(KeyEvent::new(control, KeyModifiers::CONTROL));
        assert!(matches!(
            hints.key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)),
            HintKey::Pass
        ));
        assert!(hints.active.is_none());
        hints.key(KeyEvent::new(control, KeyModifiers::CONTROL));
        hints.key(KeyEvent::new(KeyCode::Char('1'), KeyModifiers::CONTROL));
        // Kitty releases carry no modifier bits. Never activate on release.
        assert!(matches!(
            hints.key(KeyEvent::new_with_kind(
                control,
                KeyModifiers::NONE,
                KeyEventKind::Release
            )),
            HintKey::Consumed
        ));
        assert!(hints.active.is_none());
    }

    #[test]
    fn key_hints_focus_loss_and_resize_prevent_activation() {
        let mut hints = KeyHints::default();
        hints.start(None);
        assert!(matches!(
            hints.key(KeyEvent::new(KeyCode::Char('1'), KeyModifiers::NONE)),
            HintKey::Consumed
        ));
        render(&mut hints, vec![(Rect::new(0, 0, 5, 1), "action".into())]);
        hints.observe_event(&Event::Resize(31, 20));
        assert!(matches!(
            hints.key(KeyEvent::new(KeyCode::Char('1'), KeyModifiers::NONE)),
            HintKey::Consumed
        ));
        hints.observe_event(&Event::FocusLost);
        assert!(hints.active.is_none());
        hints.key(KeyEvent::new(KeyCode::F(12), KeyModifiers::NONE));
        hints.key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));
        assert!(hints.active.is_none());
    }

    #[test]
    fn key_hints_single_digit_activation_and_tenth_zero() {
        for count in [9, 10, 12, 14] {
            let mut hints = KeyHints::default();
            hints.start(None);
            render(
                &mut hints,
                (0..count)
                    .rev()
                    .map(|i| (Rect::new(29, i, 1, 1), format!("row{i}")))
                    .collect(),
            );
            assert_eq!(hints.frame.len(), usize::from(count.min(10)));
            assert!(
                hints
                    .frame
                    .iter()
                    .all(|target| target.badge.right() <= 30 && target.badge.width == 1)
            );
            let result = hints.key(KeyEvent::new(KeyCode::Char('0'), KeyModifiers::NONE));
            if count == 9 {
                assert!(matches!(result, HintKey::Consumed));
                assert!(hints.active.as_ref().unwrap().invalid_code);
                assert!(matches!(
                    hints.key(KeyEvent::new(KeyCode::Char('1'), KeyModifiers::NONE)),
                    HintKey::Click(Position { x: 29, y: 0 })
                ));
            } else {
                assert!(matches!(result, HintKey::Click(Position { x: 29, y: 9 })));
            }
            assert!(hints.active.is_none());
        }
    }

    #[test]
    fn key_hints_activation_repeat_does_not_leak_into_composer() {
        let mut hints = KeyHints::default();
        hints.start(None);
        render(&mut hints, vec![(Rect::new(0, 0, 5, 1), "action".into())]);
        let key = KeyCode::Char('1');
        assert!(matches!(
            hints.key(KeyEvent::new(key, KeyModifiers::NONE)),
            HintKey::Click(_)
        ));
        assert!(matches!(
            hints.key(KeyEvent::new_with_kind(
                key,
                KeyModifiers::NONE,
                KeyEventKind::Repeat
            )),
            HintKey::Consumed
        ));
        hints.key(KeyEvent::new_with_kind(
            key,
            KeyModifiers::NONE,
            KeyEventKind::Release,
        ));
        assert!(matches!(
            hints.key(KeyEvent::new(key, KeyModifiers::NONE)),
            HintKey::Pass
        ));
    }

    #[test]
    fn key_hints_never_intercept_zoom_keys() {
        for key in ['-', '+', '='] {
            let mut hints = KeyHints::default();
            hints.start(None);
            let event = KeyEvent::new(KeyCode::Char(key), KeyModifiers::CONTROL);
            assert!(matches!(hints.key(event), HintKey::Pass));
            assert!(hints.active.is_none());
        }
    }

    #[test]
    fn key_hints_reject_changed_identity_or_layout_but_keep_unchanged_targets() {
        for changed in [
            (Rect::new(1, 1, 8, 1), "replacement"),
            (Rect::new(1, 2, 8, 1), "original"),
        ] {
            let mut hints = KeyHints::default();
            let original = vec![(Rect::new(1, 1, 8, 1), "original".to_string())];
            render(&mut hints, original.clone());
            hints.start(None);
            render(&mut hints, original);
            assert!(!hints.active.as_ref().unwrap().stale);
            render(&mut hints, vec![(changed.0, changed.1.to_string())]);
            assert!(matches!(
                hints.key(KeyEvent::new(KeyCode::Char('1'), KeyModifiers::NONE)),
                HintKey::Consumed
            ));
            assert!(hints.active.as_ref().unwrap().stale);
        }
    }

    #[test]
    fn key_hints_clip_and_resolve_overlapping_click_targets() {
        let mut hints = KeyHints::default();
        render(
            &mut hints,
            vec![
                (Rect::new(0, 0, 3, 1), "link".into()),
                (Rect::new(0, 0, 10, 1), "message".into()),
                (Rect::new(0, 0, 3, 1), "shadowed".into()),
                (Rect::new(0, 21, 1, 1), "offscreen".into()),
            ],
        );
        assert_eq!(hints.frame.len(), 2);
        assert_eq!(hints.frame[0].identity, "link");
        assert_eq!(hints.frame[1].point, Position::new(3, 0));
        assert!(!hints.frame[0].badge.intersects(hints.frame[1].badge));
    }

    #[test]
    fn key_hints_tool_replacement_changes_identity_at_same_index() {
        let mut transcript = Transcript::default();
        transcript.tools.insert("original-call".into(), 0);
        let original = transcript.key_hint_identity(0);
        transcript.tools.clear();
        transcript.tools.insert("replacement-call".into(), 0);
        assert_ne!(original, transcript.key_hint_identity(0));
    }
}
