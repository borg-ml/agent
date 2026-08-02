use super::*;

#[test]
fn focused_child_transcript_round_trips_back_to_director() {
    let child_id = Uuid::new_v4();
    let mut displayed = Transcript::default();
    displayed.order.push(TranscriptEntry::Activity {
        text: "director event".to_string(),
        time: "now".to_string(),
    });
    let mut child = Transcript::default();
    child.order.push(TranscriptEntry::Activity {
        text: "child event".to_string(),
        time: "now".to_string(),
    });
    let mut director = None;
    let mut children = HashMap::from([(child_id, child)]);

    switch_to_child_transcript(&mut displayed, &mut director, &mut children, child_id);
    assert_eq!(displayed.order.len(), 1);
    assert!(director.is_some());
    assert!(children.is_empty());

    switch_to_director_transcript(&mut displayed, &mut director, &mut children, child_id);
    assert_eq!(displayed.order.len(), 1);
    assert!(director.is_none());
    assert_eq!(children[&child_id].order.len(), 1);
}

#[test]
fn root_history_page_cannot_replace_a_focused_child_transcript() {
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    let mut displayed = Transcript::default();
    displayed.order.push(TranscriptEntry::Activity {
        text: "focused child event".to_string(),
        time: "now".to_string(),
    });
    let mut director = Some(Box::new(Transcript::default()));
    let root_event = SessionEvent::new(
        session_id,
        1,
        SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text: "older root history".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: Some(PromptDelivery::Steer),
        },
    );

    assert!(!replace_root_transcript_history(
        &mut displayed,
        &mut director,
        true,
        &[root_event],
    ));
    assert!(matches!(
        &displayed.order[0],
        TranscriptEntry::Activity { text, .. } if text == "focused child event"
    ));
    assert!(
        director
            .as_deref()
            .is_some_and(|transcript| transcript.messages.contains_key(&message_id))
    );
}

#[test]
fn older_root_history_cannot_regress_an_authoritatively_stopped_child() {
    let root = Uuid::new_v4();
    let child = Uuid::new_v4();
    let now = Utc::now();
    let stale_running = SubagentSnapshot {
        session_id: child,
        parent_session_id: root,
        task_name: "/root/worker".to_string(),
        status: SubagentStatus::Running,
        provider: CodingProvider::Codex,
        model: Some("gpt-test".to_string()),
        effort: Some("high".to_string()),
        cwd: PathBuf::from("/workspace"),
        created_at: now - chrono::Duration::minutes(1),
        updated_at: now - chrono::Duration::seconds(1),
        detail: Some("turn phase: provider active".to_string()),
        final_text: None,
        usage: borg_remote::SubagentUsage::default(),
    };
    let stale_parent_event = SessionEvent::new(
        root,
        10,
        SessionEventKind::SubagentActivity {
            activity: SubagentActivityKind::Updated,
            agent: stale_running.clone(),
            event: None,
        },
    );
    let mut stopped = stale_running;
    stopped.status = SubagentStatus::Stopped;
    stopped.updated_at = now;
    stopped.detail = Some("crash cleanup completed".to_string());

    let mut displayed = Transcript::default();
    displayed.upsert_subagent_snapshot(&stopped);
    assert_eq!(displayed.active_subagent_count(), 0);
    let mut director = None;

    assert!(replace_root_transcript_history(
        &mut displayed,
        &mut director,
        false,
        &[stale_parent_event],
    ));

    assert_eq!(displayed.active_subagent_count(), 0);
    assert_eq!(displayed.subagents[&child], SubagentStatus::Stopped);
    assert_eq!(
        displayed.subagent_snapshots[&child].detail.as_deref(),
        Some("crash cleanup completed")
    );
}

#[test]
fn child_history_merge_prefers_completion_over_a_late_partial_snapshot() {
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    let now = Utc::now();
    let mut complete = SessionEvent::new(
        session_id,
        8,
        SessionEventKind::Message {
            message_id,
            actor: EventActor::Assistant,
            text: "I am complete".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
    );
    complete.created_at = now;
    let mut stale_partial = SessionEvent::new(
        session_id,
        0,
        SessionEventKind::Message {
            message_id,
            actor: EventActor::Assistant,
            text: "I".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: None,
        },
    );
    stale_partial.created_at = now + chrono::Duration::seconds(1);

    let merged = merge_child_history(&[complete], vec![stale_partial]);

    assert_eq!(merged.len(), 1);
    assert!(matches!(
        &merged[0].kind,
        SessionEventKind::Message {
            text,
            status: MessageStatus::Complete,
            ..
        } if text == "I am complete"
    ));
}

#[test]
fn child_transcript_starts_with_a_director_context_boundary() {
    let mut transcript = Transcript::default();
    transcript.show_director_context_boundary();

    assert!(matches!(
        transcript.order.first(),
        Some(TranscriptEntry::Activity { text, .. }) if text == DIRECTOR_CONTEXT_BOUNDARY
    ));
}

#[test]
fn focused_transcript_can_switch_directly_between_children() {
    let first_id = Uuid::new_v4();
    let second_id = Uuid::new_v4();
    let mut displayed = Transcript::default();
    displayed.order.push(TranscriptEntry::Activity {
        text: "first child".to_string(),
        time: "now".to_string(),
    });
    let mut second = Transcript::default();
    second.order.push(TranscriptEntry::Activity {
        text: "second child".to_string(),
        time: "now".to_string(),
    });
    let mut children = HashMap::from([(second_id, second)]);

    switch_between_child_transcripts(&mut displayed, &mut children, first_id, second_id);

    assert!(matches!(
        &displayed.order[0],
        TranscriptEntry::Activity { text, .. } if text == "second child"
    ));
    assert!(matches!(
        &children[&first_id].order[0],
        TranscriptEntry::Activity { text, .. } if text == "first child"
    ));
    assert!(!children.contains_key(&second_id));
}

#[test]
fn focusing_a_new_child_still_shows_the_director_context_boundary() {
    let child_id = Uuid::new_v4();
    let mut displayed = Transcript::default();
    displayed.order.push(TranscriptEntry::Activity {
        text: "director event".to_string(),
        time: "now".to_string(),
    });
    let mut director = None;
    let mut children = HashMap::new();

    switch_to_child_transcript(&mut displayed, &mut director, &mut children, child_id);

    assert!(matches!(
        displayed.order.first(),
        Some(TranscriptEntry::Activity { text, .. }) if text == DIRECTOR_CONTEXT_BOUNDARY
    ));
}

#[test]
fn team_roster_hover_is_visually_distinct_from_focus_and_idle() {
    let hovered = team_roster_row_style(false, true, true);
    let focused = team_roster_row_style(true, false, true);
    let idle_subagent = team_roster_row_style(false, false, true);
    let idle_director = team_roster_row_style(false, false, false);

    assert_eq!(hovered.bg, Some(SUBAGENT_PINK));
    assert_eq!(hovered.fg, Some(Color::Black));
    assert!(hovered.add_modifier.contains(Modifier::BOLD));
    assert_eq!(focused.fg, Some(SUBAGENT_PINK));
    assert_eq!(idle_subagent.fg, Some(SUBAGENT_PINK));
    assert_eq!(idle_director.fg, Some(Color::White));
    assert_ne!(hovered, focused);
    assert_ne!(hovered, idle_subagent);
}

#[test]
fn transcript_attachments_preserve_the_explicit_image_number() {
    let path = PathBuf::from("9619ebf5-b115-43af-9fa1-feea11842109.png");
    let mut next_image_number = 1;

    let numbered = number_message_attachments(
        "the focused view looks wrong [Image 6]",
        std::slice::from_ref(&path),
        &mut next_image_number,
    );

    assert_eq!(numbered, [(6, path)]);
    assert_eq!(next_image_number, 7);
}

#[test]
fn subagent_activity_rows_use_the_shared_hot_pink_identity_colour() {
    let mut transcript = Transcript::default();
    transcript.order.push(TranscriptEntry::Activity {
        text: "agent · /root/worker · report · complete".to_string(),
        time: "2026-08-01 20:40".to_string(),
    });

    let line = transcript
        .lines(100)
        .into_iter()
        .find(|line| line.to_string().contains("/root/worker"))
        .expect("subagent activity row");

    assert_eq!(line.spans.last().unwrap().style.fg, Some(SUBAGENT_PINK));
}

#[test]
fn focused_subagent_status_preserves_semantic_failures_and_uses_pink_for_work() {
    assert_eq!(
        focused_subagent_status_color(SessionStatus::Running, true),
        SUBAGENT_PINK
    );
    assert_eq!(
        focused_subagent_status_color(SessionStatus::Ready, true),
        SUBAGENT_PINK
    );
    assert_eq!(
        focused_subagent_status_color(SessionStatus::Failed, true),
        Color::LightRed
    );
    assert_eq!(
        focused_subagent_status_color(SessionStatus::Running, false),
        RUNNING_STATUS_PEACH
    );
}

#[test]
fn a_late_message_snapshot_can_supply_the_attachment_without_renumbering_it() {
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    let mut transcript = Transcript::default();
    transcript.apply(&SessionEvent::new(
        session_id,
        1,
        SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text: "upload follows".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: None,
        },
    ));
    let path = PathBuf::from("capture.png");
    transcript.apply(&SessionEvent::new(
        session_id,
        2,
        SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text: "upload follows [Image 6]".to_string(),
            attachments: vec![path.clone()],
            status: MessageStatus::Complete,
            delivery: None,
        },
    ));

    assert!(matches!(
        transcript.order.first(),
        Some(TranscriptEntry::Message { attachments, .. })
            if attachments == &vec![(6, path)]
    ));
}

#[test]
fn hover_redraw_gate_ignores_motion_inside_one_target() {
    let idle = HoverState {
        hovered_tool: None,
        hovered_tool_run_header: None,
        hovered_entry: None,
        hovered_message: None,
        hovered_picker_option: None,
        hovered_team_roster: None,
        status_hovered: false,
        goal_status_hovered: false,
        todo_status_hovered: false,
        agents_status_hovered: false,
        model_status_hovered: false,
        effort_status_hovered: false,
        fast_status_hovered: false,
        permission_status_hovered: false,
        back_to_director_hovered: false,
        scrollbar_hovered: false,
        jump_to_bottom_hovered: false,
        keybindings_hovered: false,
    };
    let running = HoverState {
        status_hovered: true,
        ..idle
    };

    assert!(!hover_state_changed(idle, idle));
    assert!(hover_state_changed(idle, running));
}

#[test]
fn team_roster_hit_testing_selects_each_exact_agent_row() {
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    let hit_areas = [
        (Rect::new(4, 10, 40, 1), None),
        (Rect::new(4, 11, 40, 1), Some(first)),
        (Rect::new(4, 12, 40, 1), Some(second)),
    ];

    assert_eq!(
        team_roster_target_at(&hit_areas, Position::new(8, 10)),
        Some((0, None))
    );
    assert_eq!(
        team_roster_target_at(&hit_areas, Position::new(8, 11)),
        Some((1, Some(first)))
    );
    assert_eq!(
        team_roster_target_at(&hit_areas, Position::new(8, 12)),
        Some((2, Some(second)))
    );
    assert_eq!(
        team_roster_target_at(&hit_areas, Position::new(8, 13)),
        None
    );
}

#[test]
fn model_and_effort_pickers_use_the_provider_catalog() {
    let catalog = CodingProvider::Codex
        .model_catalog()
        .expect("Codex catalog");
    let options = model_picker_options(Some(CodingProvider::Codex), None);
    let values = options
        .iter()
        .map(|option| option.value.as_str())
        .collect::<Vec<_>>();
    // Codex leads the canonical catalog order, and every other catalog-backed
    // provider is still selectable below it.
    assert_eq!(
        values[..catalog.selectable_models.len()],
        catalog
            .selectable_models
            .iter()
            .map(|(model, _)| *model)
            .collect::<Vec<_>>()[..]
    );
    assert_eq!(
        effort_picker_options(Some(CodingProvider::Codex)),
        catalog.effort_levels
    );
    assert!(values.contains(&"gpt-5.6-luna"));
    for (model, _) in borg_provider::CLAUDE_SELECTABLE_MODELS {
        assert!(values.contains(&model), "{model} missing from picker");
    }

    let claude_options = model_picker_options(Some(CodingProvider::Claude), None);
    assert_eq!(claude_options[0].section.as_deref(), Some("Codex"));
    assert!(
        claude_options
            .iter()
            .any(|option| option.section.as_deref() == Some("Claude"))
    );
    assert_eq!(
        effort_picker_options(Some(CodingProvider::Claude)),
        &["low", "medium", "high", "xhigh", "max"]
    );
}

#[test]
fn keybinding_help_is_action_first_and_uses_configuration() {
    let config = crate::agent_config::KeybindingConfig {
        send: vec!["ctrl+s".to_string()],
        ..Default::default()
    };
    let keymap = KeyMap::from_config(&config).expect("keymap");
    assert_eq!(
        primary_controls_line(&keymap),
        "send ctrl+s · commands / · palette tab or ?"
    );
    let help = keybinding_lines(&keymap, 60)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(help.contains("send"));
    assert!(help.find("send").unwrap() < help.find("ctrl+s").unwrap());
    assert!(help.contains("queue next turn"));
}

#[test]
fn reflow_respects_cell_width_and_grapheme_boundaries() {
    let input = "alpha 👩🏽‍💻 漢字 omega";
    for width in [4, 7, 12] {
        let wrapped = wrap_display(input, width);
        assert!(
            wrapped
                .iter()
                .all(|line| UnicodeWidthStr::width(line.as_str()) <= width)
        );
        assert_eq!(wrapped.concat(), input);
    }
}

#[test]
fn reflow_prefers_word_boundaries_without_losing_text() {
    assert_eq!(wrap_display("alpha beta", 7), vec!["alpha ", "beta"]);
}

#[test]
fn composer_deletes_one_extended_grapheme() {
    let mut composer = Composer::default();
    composer.insert("a👩🏽‍💻b");
    composer.move_left();
    composer.backspace();
    assert_eq!(composer.text, "ab");
}

#[test]
fn composer_deletes_the_previous_unicode_word() {
    let mut composer = Composer::default();
    composer.insert("ship polished интерфейс");
    composer.backspace_word();
    assert_eq!(composer.text, "ship polished ");
    assert_eq!(composer.cursor, composer.text.len());
}

#[test]
fn terminal_word_delete_shortcuts_cover_common_encodings() {
    for event in [
        KeyEvent::new(KeyCode::Backspace, KeyModifiers::CONTROL),
        KeyEvent::new(KeyCode::Backspace, KeyModifiers::ALT),
        KeyEvent::new(KeyCode::Char('h'), KeyModifiers::CONTROL),
        KeyEvent::new(KeyCode::Char('w'), KeyModifiers::CONTROL),
    ] {
        assert!(deletes_previous_word(&event), "{event:?}");
    }
    assert!(!deletes_previous_word(&KeyEvent::new(
        KeyCode::Backspace,
        KeyModifiers::NONE
    )));
}

#[test]
fn composer_treats_an_image_token_as_one_editable_unit() {
    let mut composer = Composer::default();
    composer.insert("before after");
    composer.cursor = "before ".len();
    assert_eq!(
        composer.insert_attachment(PathBuf::from("capture.png")),
        "Image 1"
    );
    assert_eq!(composer.text, "before [Image 1]after");

    composer.move_left();
    assert_eq!(composer.cursor, "before ".len());
    composer.move_right();
    composer.backspace();
    assert_eq!(composer.text, "before after");
    assert!(composer.attachments.is_empty());
}

#[test]
fn edit_tool_accepts_the_array_diff_contract() {
    let input = serde_json::json!([
        {"diff": "@@ -1 +1 @@\n-old\n+new"}
    ]);
    assert_eq!(
        tool_code_view("Edit", &input),
        Some(("diff".to_string(), "@@ -1 +1 @@\n-old\n+new".to_string()))
    );

    let rust_input = serde_json::json!([
        {
            "path": "src/main.rs",
            "diff": "@@ -1 +1 @@\n-fn old() {}\n+fn main() {}"
        }
    ]);
    assert_eq!(
        tool_code_view("Edit", &rust_input),
        Some((
            "diff:rs".to_string(),
            "@@ -1 +1 @@\n-fn old() {}\n+fn main() {}".to_string()
        ))
    );
}

#[test]
fn completed_file_creation_replaces_null_diff_placeholder() {
    let session_id = Uuid::new_v4();
    let input = serde_json::json!({
        "diff": null,
        "file_path": "src/new.rs",
        "paths": ["src/new.rs"]
    });
    let mut transcript = Transcript::default();
    transcript.apply(&SessionEvent::new(
        session_id,
        1,
        SessionEventKind::ToolStarted {
            tool_call_id: "create-1".to_string(),
            name: "Edit".to_string(),
            input: input.clone(),
            input_ref: None,
        },
    ));
    transcript.apply(&SessionEvent::new(
        session_id,
        2,
        SessionEventKind::ToolCompleted {
            tool_call_id: "create-1".to_string(),
            output: r#"[{"diff":"fn main() {}\n","kind":{"type":"add"},"path":"src/new.rs"}]"#
                .to_string(),
            output_ref: None,
            is_error: false,
            input: Some(input),
            input_ref: None,
        },
    ));

    assert!(matches!(
        transcript.order.last(),
        Some(TranscriptEntry::Tool {
            name,
            code_view: Some((language, text)),
            expanded: true,
            ..
        }) if name == "Create file"
            && language == "diff:rs"
            && text.contains("+fn main() {}")
    ));
}

#[test]
fn running_tool_uses_animated_spinner() {
    let mut transcript = Transcript::default();
    transcript.apply(&SessionEvent::new(
        Uuid::new_v4(),
        1,
        SessionEventKind::ToolStarted {
            tool_call_id: "running-1".to_string(),
            name: "command_execution".to_string(),
            input: serde_json::json!({"command": "cargo check"}),
            input_ref: None,
        },
    ));

    assert!(transcript.tool_spinner_cache_tick().is_some());
    let rendered = transcript
        .lines(100)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(rendered.chars().any(|glyph| "⠋⠙⠹⠸⠼⠴⠦⠧".contains(glyph)));
    assert!(!rendered.contains('↳'));
}

#[test]
fn composer_cursor_has_a_stable_software_blink_phase() {
    assert!(cursor_blink_visible(Duration::ZERO));
    assert!(cursor_blink_visible(Duration::from_millis(499)));
    assert!(!cursor_blink_visible(Duration::from_millis(500)));
    assert!(!cursor_blink_visible(Duration::from_millis(999)));
    assert!(cursor_blink_visible(Duration::from_millis(1_000)));
}

#[test]
fn completed_tool_duration_is_frozen_at_the_right_edge() {
    let session_id = Uuid::new_v4();
    let started_at = DateTime::parse_from_rfc3339("2026-07-29T10:00:00Z")
        .unwrap()
        .with_timezone(&Utc);
    let mut started = SessionEvent::new(
        session_id,
        1,
        SessionEventKind::ToolStarted {
            tool_call_id: "timed-1".to_string(),
            name: "command_execution".to_string(),
            input: serde_json::json!({"command": "cargo check"}),
            input_ref: None,
        },
    );
    started.created_at = started_at;
    let mut completed = SessionEvent::new(
        session_id,
        2,
        SessionEventKind::ToolCompleted {
            tool_call_id: "timed-1".to_string(),
            output: String::new(),
            output_ref: None,
            is_error: false,
            input: None,
            input_ref: None,
        },
    );
    completed.created_at = started_at + chrono::Duration::milliseconds(12_345);

    let mut transcript = Transcript::default();
    transcript.apply(&started);
    transcript.apply(&completed);
    let line = transcript
        .lines(80)
        .into_iter()
        .find(|line| line.to_string().contains("cargo check"))
        .expect("tool summary");

    assert_eq!(line.width(), 80);
    assert!(line.to_string().ends_with("12.3s"));
}

#[test]
fn tool_duration_appears_only_from_one_tenth_of_a_second() {
    let started_at = DateTime::parse_from_rfc3339("2026-07-29T10:00:00Z")
        .unwrap()
        .with_timezone(&Utc);

    assert_eq!(format_tool_elapsed(started_at, Some(started_at)), None);
    assert_eq!(
        format_tool_elapsed(
            started_at,
            Some(started_at + chrono::Duration::milliseconds(99))
        ),
        None
    );
    assert_eq!(
        format_tool_elapsed(
            started_at,
            Some(started_at + chrono::Duration::milliseconds(100))
        ),
        Some("0.1s".to_string())
    );
}

#[test]
fn a_new_edit_or_message_collapses_the_previous_diff() {
    let session_id = Uuid::new_v4();
    let mut transcript = Transcript::default();
    for (sequence, path) in [(1, "src/first.rs"), (2, "src/second.rs")] {
        transcript.apply(&SessionEvent::new(
            session_id,
            sequence,
            SessionEventKind::ToolStarted {
                tool_call_id: format!("edit-{sequence}"),
                name: "Edit".to_string(),
                input: serde_json::json!([{
                    "path": path,
                    "diff": "@@ -1 +1 @@\n-old\n+new"
                }]),
                input_ref: None,
            },
        ));
    }

    assert!(matches!(
        transcript.order.first(),
        Some(TranscriptEntry::Tool {
            expanded: false,
            ..
        })
    ));
    assert!(matches!(
        transcript.order.get(1),
        Some(TranscriptEntry::Tool { expanded: true, .. })
    ));

    transcript.apply(&SessionEvent::new(
        session_id,
        3,
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::Assistant,
            text: "Edits are complete.".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
    ));
    assert!(matches!(
        transcript.order.get(1),
        Some(TranscriptEntry::Tool {
            expanded: false,
            ..
        })
    ));
}

#[test]
fn live_tail_updates_reuse_completed_message_markdown() {
    let session_id = Uuid::new_v4();
    let mut transcript = Transcript::default();
    for (sequence, actor, text) in [
        (1, EventActor::User, "A **formatted** request"),
        (2, EventActor::Assistant, "A completed response"),
    ] {
        transcript.apply(&SessionEvent::new(
            session_id,
            sequence,
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor,
                text: text.to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        ));
    }

    let _ = transcript.lines(80);
    assert_eq!(transcript.message_markdown_cache.borrow().misses, 2);

    transcript.apply(&SessionEvent::new(
        session_id,
        3,
        SessionEventKind::ReasoningDelta {
            text: "new live tail".to_string(),
        },
    ));
    let _ = transcript.lines(80);

    assert_eq!(
        transcript.message_markdown_cache.borrow().misses,
        2,
        "a live tail update must not reparse completed history"
    );
}

#[test]
fn live_tail_updates_reuse_completed_tool_bodies() {
    let session_id = Uuid::new_v4();
    let mut transcript = Transcript::default();
    transcript.apply(&SessionEvent::new(
        session_id,
        1,
        SessionEventKind::ToolStarted {
            tool_call_id: "edit-1".to_string(),
            name: "Edit".to_string(),
            input: serde_json::json!([{
                "path": "src/main.rs",
                "diff": "@@ -1 +1 @@\n-old\n+new"
            }]),
            input_ref: None,
        },
    ));
    transcript.apply(&SessionEvent::new(
        session_id,
        2,
        SessionEventKind::ToolCompleted {
            tool_call_id: "edit-1".to_string(),
            output: String::new(),
            output_ref: None,
            is_error: false,
            input: None,
            input_ref: None,
        },
    ));

    let _ = transcript.lines(80);
    assert_eq!(transcript.tool_body_cache.borrow().misses, 1);

    transcript.apply(&SessionEvent::new(
        session_id,
        3,
        SessionEventKind::ReasoningDelta {
            text: "new live tail".to_string(),
        },
    ));
    let _ = transcript.lines(80);

    assert_eq!(
        transcript.tool_body_cache.borrow().misses,
        1,
        "a live tail update must not re-render completed tool bodies"
    );
}

#[test]
#[ignore = "explicit large-transcript TUI render p95 performance gate"]
fn large_transcript_live_tail_render_p95_gate() {
    const COMPLETED_MESSAGES: usize = 200;
    const SAMPLES: usize = 60;

    let session_id = Uuid::new_v4();
    let mut transcript = Transcript::default();
    for sequence in 1..=COMPLETED_MESSAGES {
        transcript.apply(&SessionEvent::new(
            session_id,
            sequence as u64,
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: if sequence % 2 == 0 {
                    EventActor::Assistant
                } else {
                    EventActor::User
                },
                text: format!(
                    "## Message {sequence}\n\n{}\n\n- first item\n- second item\n- third item",
                    "A representative long transcript sentence with **formatting**. ".repeat(8)
                ),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        ));
    }
    let live_message_id = Uuid::new_v4();
    transcript.apply(&SessionEvent::new(
        session_id,
        COMPLETED_MESSAGES as u64 + 1,
        SessionEventKind::Message {
            message_id: live_message_id,
            actor: EventActor::Assistant,
            text: "starting".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: None,
        },
    ));
    let large_diff = (0..1_000)
        .map(|line| format!("+let generated_{line} = {line};"))
        .collect::<Vec<_>>()
        .join("\n");
    transcript.apply(&SessionEvent::new(
        session_id,
        COMPLETED_MESSAGES as u64 + 2,
        SessionEventKind::ToolStarted {
            tool_call_id: "large-edit".to_string(),
            name: "Edit".to_string(),
            input: serde_json::json!([{
                "path": "src/generated.rs",
                "diff": format!("@@ -0,0 +1,1000 @@\n{large_diff}")
            }]),
            input_ref: None,
        },
    ));
    transcript.apply(&SessionEvent::new(
        session_id,
        COMPLETED_MESSAGES as u64 + 3,
        SessionEventKind::ToolCompleted {
            tool_call_id: "large-edit".to_string(),
            output: String::new(),
            output_ref: None,
            is_error: false,
            input: None,
            input_ref: None,
        },
    ));
    for tool in 0..9 {
        let tool_call_id = format!("search-{tool}");
        transcript.apply(&SessionEvent::new(
            session_id,
            (COMPLETED_MESSAGES + 4 + tool * 2) as u64,
            SessionEventKind::ToolStarted {
                tool_call_id: tool_call_id.clone(),
                name: "Search".to_string(),
                input: serde_json::json!({"query": format!("term {tool}")}),
                input_ref: None,
            },
        ));
        transcript.apply(&SessionEvent::new(
            session_id,
            (COMPLETED_MESSAGES + 5 + tool * 2) as u64,
            SessionEventKind::ToolCompleted {
                tool_call_id,
                output: String::new(),
                output_ref: None,
                is_error: false,
                input: None,
                input_ref: None,
            },
        ));
    }
    let _ = transcript.render(120, None, None, None);

    let mut samples = Vec::with_capacity(SAMPLES);
    for sample in 0..SAMPLES {
        transcript.apply(&SessionEvent::new(
            session_id,
            COMPLETED_MESSAGES as u64 + 2 + sample as u64,
            SessionEventKind::Message {
                message_id: live_message_id,
                actor: EventActor::Assistant,
                text: format!(
                    "streaming response snapshot {sample} {}",
                    "word ".repeat(40)
                ),
                attachments: Vec::new(),
                status: MessageStatus::InProgress,
                delivery: None,
            },
        ));
        let started = Instant::now();
        let render = transcript.render(120, None, None, None);
        assert!(!render.0.is_empty());
        samples.push(started.elapsed());
    }

    samples.sort_unstable();
    let p95 = samples[(samples.len() * 95).div_ceil(100).saturating_sub(1)];
    let mut uncached_samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        transcript
            .message_markdown_cache
            .borrow_mut()
            .messages
            .clear();
        transcript.tool_body_cache.borrow_mut().lines.clear();
        let started = Instant::now();
        let render = transcript.render(120, None, None, None);
        assert!(!render.0.is_empty());
        uncached_samples.push(started.elapsed());
    }
    uncached_samples.sort_unstable();
    let uncached_p95 = uncached_samples[(uncached_samples.len() * 95)
        .div_ceil(100)
        .saturating_sub(1)];
    eprintln!("large transcript live-tail render p95: cached {p95:?}, uncached {uncached_p95:?}");
    assert!(
        p95 < Duration::from_millis(16),
        "large transcript live-tail render p95 exceeded one 60 Hz frame: {p95:?}"
    );
    assert!(
        p95 < uncached_p95,
        "completed-history caching did not improve render p95: cached {p95:?}, uncached {uncached_p95:?}"
    );
}

#[test]
fn projection_only_events_keep_the_transcript_layout_cache() {
    assert!(!session_event_changes_transcript(
        &SessionEventKind::UsageUpdated {
            provider_duration_ms: 10,
            input_tokens: 1,
            output_tokens: 2,
            cached_input_tokens: 0,
            cache_creation_input_tokens: 0,
            total_tokens: 3,
            cost_microusd: None,
            cost_basis: String::new(),
            cost_usd: None,
            context_tokens: Some(1),
            context_window_tokens: Some(100),
        }
    ));
    assert!(!session_event_changes_transcript(
        &SessionEventKind::ProviderEvent {
            provider: CodingProvider::Codex,
            kind: "response.progress".to_string(),
            payload: serde_json::json!({}),
        }
    ));
    assert!(session_event_changes_transcript(
        &SessionEventKind::ProviderEvent {
            provider: CodingProvider::Codex,
            kind: "context_compaction".to_string(),
            payload: serde_json::json!({}),
        }
    ));
    assert!(session_event_changes_transcript(
        &SessionEventKind::StatusChanged {
            status: SessionStatus::Ready,
            detail: None,
        }
    ));
    assert!(!session_event_changes_transcript(
        &SessionEventKind::StatusChanged {
            status: SessionStatus::Running,
            detail: None,
        }
    ));

    let child_id = Uuid::new_v4();
    let agent = SubagentSnapshot {
        session_id: child_id,
        parent_session_id: Uuid::new_v4(),
        task_name: "/root/inspect_ui".to_string(),
        status: SubagentStatus::Running,
        provider: CodingProvider::Codex,
        model: None,
        effort: None,
        cwd: PathBuf::from("/workspace"),
        detail: None,
        final_text: None,
        usage: Default::default(),
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    };
    assert!(!session_event_changes_transcript(
        &SessionEventKind::SubagentActivity {
            activity: SubagentActivityKind::Updated,
            agent: agent.clone(),
            event: Some(Box::new(SessionEvent::new(
                child_id,
                1,
                SessionEventKind::ReasoningDelta {
                    text: "hidden chatter".to_string(),
                },
            ))),
        }
    ));
    assert!(session_event_changes_transcript(
        &SessionEventKind::SubagentActivity {
            activity: SubagentActivityKind::Updated,
            agent,
            event: Some(Box::new(SessionEvent::new(
                child_id,
                2,
                SessionEventKind::Message {
                    message_id: Uuid::new_v4(),
                    actor: EventActor::Assistant,
                    text: "visible report".to_string(),
                    attachments: Vec::new(),
                    status: MessageStatus::Complete,
                    delivery: None,
                },
            ))),
        }
    ));
}

#[test]
fn effort_changes_do_not_relabel_usage_from_the_active_turn() {
    let session_id = Uuid::new_v4();
    let old_turn = Uuid::new_v4();
    let new_turn = Uuid::new_v4();
    let mut transcript = Transcript::default();
    let configured = |effort: &str| SessionEventKind::SessionConfigured {
        cwd: PathBuf::from("/workspace"),
        provider: CodingProvider::Codex,
        model: Some("gpt-5.4".to_string()),
        effort: Some(effort.to_string()),
        fast: false,
        response_language: ResponseLanguage::English,
        permission_mode: PermissionMode::FullAccess,
    };
    let started = |message_id, effort: &str| SessionEventKind::TurnStarted {
        message_id,
        provider: CodingProvider::Codex,
        model: Some("gpt-5.4".to_string()),
        effort: Some(effort.to_string()),
        fast: false,
    };
    let usage = |cached_input_tokens| SessionEventKind::UsageUpdated {
        provider_duration_ms: 10,
        input_tokens: 1_000,
        output_tokens: 100,
        cached_input_tokens,
        cache_creation_input_tokens: 0,
        total_tokens: 1_100 + cached_input_tokens,
        cost_microusd: None,
        cost_basis: String::new(),
        cost_usd: None,
        context_tokens: Some(10_000),
        context_window_tokens: Some(100_000),
    };
    let mut sequence = 1;
    let mut apply = |transcript: &mut Transcript, kind| {
        transcript.apply(&SessionEvent::new(session_id, sequence, kind));
        sequence += 1;
    };

    apply(&mut transcript, configured("medium"));
    apply(&mut transcript, started(old_turn, "medium"));
    apply(&mut transcript, usage(0));
    apply(&mut transcript, configured("high"));
    apply(&mut transcript, usage(9_000));

    assert_eq!(transcript.cache_status(Utc::now()), None);
    assert_eq!(
        transcript
            .active_turn
            .as_ref()
            .and_then(|turn| turn.effort.as_deref()),
        Some("medium")
    );

    apply(
        &mut transcript,
        SessionEventKind::TurnCompleted {
            message_id: old_turn,
            provider_session_id: None,
            final_text: String::new(),
            error: None,
        },
    );
    assert!(
        transcript
            .cache_status(Utc::now())
            .is_some_and(|status| status.label.contains("effort changed"))
    );

    apply(&mut transcript, started(new_turn, "high"));
    apply(&mut transcript, usage(9_000));
    assert_eq!(transcript.cache_status(Utc::now()), None);
}

#[test]
fn deferred_tool_input_loads_when_the_card_is_expanded() {
    let session_id = Uuid::new_v4();
    let payload = SessionPayloadRef {
        id: Uuid::new_v4(),
        kind: SessionPayloadKind::ToolInput,
        byte_len: 1_000_000,
    };
    let input = serde_json::json!([{
        "path": "src/main.rs",
        "diff": "@@ -1 +1 @@\n-old\n+new"
    }]);
    let mut transcript = Transcript {
        auto_expand_edits: true,
        ..Transcript::default()
    };
    transcript.apply(&SessionEvent::new(
        session_id,
        1,
        SessionEventKind::ToolStarted {
            tool_call_id: "deferred-edit".to_string(),
            name: "Edit".to_string(),
            input: serde_json::json!({"borg_payload_deferred": true}),
            input_ref: Some(payload.clone()),
        },
    ));

    assert!(matches!(
        transcript.order.first(),
        Some(TranscriptEntry::Tool {
            expanded: false,
            ..
        })
    ));
    assert_eq!(transcript.toggle_tool(0)[0].id, payload.id);
    transcript
        .hydrate_payload(&payload, serde_json::to_vec(&input).unwrap())
        .unwrap();
    assert!(matches!(
        transcript.order.first(),
        Some(TranscriptEntry::Tool {
            code_view: Some((language, source)),
            payload_refs,
            expanded: true,
            ..
        }) if language.starts_with("diff")
            && source.contains("+new")
            && payload_refs.is_empty()
    ));
}

#[test]
fn composer_cursor_uses_terminal_cell_width() {
    assert_eq!(composer_cursor_position("a漢b", "a漢".len(), 3), (1, 0));
    assert_eq!(composer_cursor_position("a漢b", "a漢b".len(), 3), (1, 1));
    assert_eq!(composer_cursor_position("abc", 3, 3), (1, 0));
}

#[test]
fn secret_provider_input_is_masked_without_losing_cursor_position() {
    let (masked, cursor) = mask_secret_composer_text("ab漢\nc", "ab漢".len());
    assert_eq!(masked, "•••\n•");
    assert_eq!(cursor, "•••".len());
    assert!(provider_interaction_contains_secret(&serde_json::json!({
        "questions": [{"isSecret": true}]
    })));
}

#[test]
fn composer_wraps_every_line_after_the_prompt_marker() {
    assert_eq!(composer_cursor_x_offset(false), 3);
    assert_eq!(composer_cursor_x_offset(true), 4);

    let mut composer = Composer::default();
    composer.insert("abcdef");
    let rendered = composer.styled_lines(3, " > ");
    assert_eq!(rendered[0].to_string(), " > abc");
    assert_eq!(rendered[1].to_string(), "   def");
}

#[test]
fn composer_moves_vertically_across_wrapped_lines() {
    let mut composer = Composer::default();
    composer.insert("alpha beta gamma");
    composer.cursor = "alpha be".len();
    composer.move_vertical(1, 7);
    assert_eq!(composer.cursor, "alpha beta ga".len());
    composer.move_vertical(-1, 7);
    assert_eq!(composer.cursor, "alpha be".len());
}

#[test]
fn composer_moves_by_unicode_words() {
    let mut composer = Composer::default();
    composer.insert("alpha beta gamma");
    composer.move_word_left();
    assert_eq!(&composer.text[composer.cursor..], "gamma");
    composer.move_word_left();
    assert_eq!(&composer.text[composer.cursor..], "beta gamma");
    composer.move_word_right();
    assert_eq!(&composer.text[composer.cursor..], "gamma");
}

#[test]
fn composer_clear_discards_the_unsent_prompt_and_attachments() {
    let mut composer = Composer::default();
    composer.insert("unsent prompt");
    composer.insert_attachment(PathBuf::from("/tmp/image.png"));
    composer.insert_pasted_text("large pasted payload".to_string());

    composer.clear();

    assert!(composer.text.is_empty());
    assert!(composer.attachments.is_empty());
    assert!(composer.pasted_texts.is_empty());
    assert_eq!(composer.cursor, 0);
}

#[test]
fn composer_expands_numbered_pasted_text_tokens_on_submit() {
    let mut composer = Composer::default();
    composer.insert("before ");
    assert_eq!(
        composer.insert_pasted_text("x".repeat(LARGE_PASTE_CHAR_THRESHOLD + 1)),
        "Pasted Text 1"
    );
    composer.insert(" after");

    assert_eq!(composer.text, "before [Pasted Text 1] after");
    let rendered = composer.styled_lines(80, " > ");
    assert!(
        rendered[0]
            .spans
            .iter()
            .any(|span| span.content == "[Pasted Text 1]"
                && span.style.fg == Some(Color::LightYellow))
    );

    let (submitted, attachments) = composer.take();
    assert_eq!(
        submitted,
        format!(
            "before {} after",
            "x".repeat(LARGE_PASTE_CHAR_THRESHOLD + 1)
        )
    );
    assert!(attachments.is_empty());
    assert!(composer.pasted_texts.is_empty());
}

#[test]
fn composer_treats_pasted_text_tokens_as_atomic() {
    let mut composer = Composer::default();
    composer.insert("prefix ");
    composer.insert_pasted_text("payload".to_string());
    let token_start = "prefix ".len();
    composer.move_left();
    assert_eq!(composer.cursor, token_start);
    composer.move_right();
    composer.backspace();
    assert_eq!(composer.text, "prefix ");
    assert!(composer.pasted_texts.is_empty());
}

#[test]
fn active_goal_cache_key_advances_once_per_elapsed_minute() {
    let mut transcript = Transcript::default();
    let goal = SessionGoal::new("Keep the elapsed timer live".to_string(), None);
    transcript.goal = Some(goal.clone());

    assert_eq!(
        transcript.active_goal_cache_tick_at(goal.updated_at),
        Some(0)
    );
    assert_eq!(
        transcript.active_goal_cache_tick_at(goal.updated_at + chrono::Duration::seconds(1)),
        Some(0)
    );
    assert_eq!(
        transcript.active_goal_cache_tick_at(goal.updated_at + chrono::Duration::seconds(60)),
        Some(1)
    );
}

#[test]
fn actionable_inactive_goals_remain_in_the_status_line() {
    let mut transcript = Transcript::default();
    let mut goal = SessionGoal::new("Wait for operator input".to_string(), None);
    goal.time_used_seconds = 125;
    let session_id = Uuid::new_v4();

    goal.status = GoalStatus::Paused;
    transcript.apply(&SessionEvent::new(
        session_id,
        1,
        SessionEventKind::GoalUpdated { goal: goal.clone() },
    ));
    assert_eq!(transcript.goal_status().as_deref(), Some("paused /goal 2m"));

    goal.status = GoalStatus::Blocked;
    transcript.apply(&SessionEvent::new(
        session_id,
        2,
        SessionEventKind::GoalUpdated { goal: goal.clone() },
    ));
    assert_eq!(
        transcript.goal_status().as_deref(),
        Some("blocked /goal 2m")
    );

    goal.status = GoalStatus::Complete;
    transcript.apply(&SessionEvent::new(
        session_id,
        3,
        SessionEventKind::GoalUpdated { goal },
    ));
    assert_eq!(transcript.goal_status(), None);
}

#[test]
fn todo_status_counts_open_items_and_tooltip_matches_plan_order_and_clipping() {
    let mut transcript = Transcript::default();
    transcript.todos = vec![
        PlanItem {
            id: Uuid::new_v4(),
            content: "Ship the hover affordance".to_string(),
            status: PlanItemStatus::InProgress,
        },
        PlanItem {
            id: Uuid::new_v4(),
            content: "Keep the completed item visible".to_string(),
            status: PlanItemStatus::Completed,
        },
        PlanItem {
            id: Uuid::new_v4(),
            content: "Run the regression tests".to_string(),
            status: PlanItemStatus::Pending,
        },
        PlanItem {
            id: Uuid::new_v4(),
            content: "Document the reload boundary".to_string(),
            status: PlanItemStatus::Pending,
        },
        PlanItem {
            id: Uuid::new_v4(),
            content: "Archive the old screenshot".to_string(),
            status: PlanItemStatus::Completed,
        },
        PlanItem {
            id: Uuid::new_v4(),
            content: "Publish the release notes".to_string(),
            status: PlanItemStatus::Pending,
        },
    ];

    assert_eq!(transcript.todo_status().as_deref(), Some("4 open to-dos"));
    let rows = transcript.todo_tooltip_rows(false);
    assert_eq!(rows.len(), MAX_COLLAPSED_PLAN_ITEMS + 1);
    assert!(rows[0].starts_with("●  "));
    assert!(rows[0].contains("Ship the hover affordance"));
    assert!(rows[1].starts_with("○  "));
    assert!(rows[1].contains("Run the regression tests"));
    assert!(
        rows.last()
            .is_some_and(|row| row.contains("click to expand"))
    );
    let expanded = transcript.todo_tooltip_rows(true);
    assert!(
        expanded
            .iter()
            .any(|row| row.contains("Keep the completed item visible"))
    );
    let expanded_with_status = transcript.todo_tooltip_rows_with_status(true);
    assert!(
        expanded_with_status.iter().any(|(row, completed)| {
            *completed && row.contains("Keep the completed item visible")
        })
    );
    assert!(
        todo_tooltip_row_style(true)
            .add_modifier
            .contains(Modifier::CROSSED_OUT)
    );
    assert!(
        !todo_tooltip_row_style(false)
            .add_modifier
            .contains(Modifier::CROSSED_OUT)
    );
    assert!(expanded.last().is_some_and(|row| row.contains("show less")));
}

/// Clicking the goal segment must submit exactly the slash command the user
/// would type, and must stay inert where there is no run state to flip.
#[test]
fn the_goal_status_segment_toggles_only_what_it_can() {
    let mut goal = SessionGoal::new("Ship the terminal polish".to_string(), None);

    goal.status = GoalStatus::Active;
    assert_eq!(goal_toggle_command(&goal), Some("/goal pause"));
    assert_eq!(goal_tooltip_title(&goal), " Goal · click to manage ");

    for status in [
        GoalStatus::Paused,
        GoalStatus::Blocked,
        GoalStatus::UsageLimited,
    ] {
        goal.status = status;
        assert_eq!(goal_toggle_command(&goal), Some("/goal resume"));
        assert_eq!(goal_tooltip_title(&goal), " Goal · click to manage ");
    }

    for status in [GoalStatus::BudgetLimited, GoalStatus::Complete] {
        goal.status = status;
        assert_eq!(goal_toggle_command(&goal), None);
        assert_eq!(goal_tooltip_title(&goal), " Goal · click to manage ");
    }
}

/// The click is only honest if the command it submits is one the parser
/// accepts; a rename on either side must break this.
#[test]
fn every_goal_toggle_command_round_trips_through_the_parser() {
    use borg_remote::GoalAction;

    let mut goal = SessionGoal::new("Ship the terminal polish".to_string(), None);
    for (status, expected) in [
        (GoalStatus::Active, GoalAction::Pause),
        (GoalStatus::Paused, GoalAction::Resume),
        (GoalStatus::Blocked, GoalAction::Resume),
        (GoalStatus::UsageLimited, GoalAction::Resume),
    ] {
        goal.status = status;
        let command = goal_toggle_command(&goal).expect("status is toggleable");
        assert_eq!(
            crate::remote_commands::parse_goal_action(command).expect("parser accepts the click"),
            expected
        );
    }
}

#[test]
fn goal_management_modal_contains_toggle_clear_and_cancel() {
    let mut goal = SessionGoal::new("Ship the terminal polish".to_string(), None);
    goal.status = GoalStatus::Active;
    let options = goal_picker_options(&goal);
    assert_eq!(
        options
            .iter()
            .map(|option| option.value.as_str())
            .collect::<Vec<_>>(),
        ["/goal pause", "/goal clear", "cancel"]
    );

    goal.status = GoalStatus::Complete;
    let options = goal_picker_options(&goal);
    assert_eq!(
        options
            .iter()
            .map(|option| option.value.as_str())
            .collect::<Vec<_>>(),
        ["/goal clear", "cancel"]
    );
}

/// An edit with no diff on screen yet has nothing for the user to watch, and
/// edits take a while. The row says one is coming until the diff itself lands.
#[test]
fn an_edit_reads_as_preparing_until_its_diff_is_on_screen() {
    let session_id = Uuid::new_v4();
    let rendered = |transcript: &Transcript| {
        transcript
            .lines(120)
            .into_iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
            .join("\n")
    };
    let started = |name: &str, input: serde_json::Value| {
        let mut transcript = Transcript::default();
        transcript.apply(&SessionEvent::new(
            session_id,
            1,
            SessionEventKind::ToolStarted {
                tool_call_id: "tool-1".to_string(),
                name: name.to_string(),
                input,
                input_ref: None,
            },
        ));
        transcript
    };

    // Nothing at all yet: the payload has not hydrated.
    let bodyless = rendered(&started("apply_patch", serde_json::Value::Null));
    assert!(bodyless.contains("preparing edit"), "{bodyless}");
    assert!(!bodyless.contains("· running"), "{bodyless}");

    // A body, but not a diff: the patch is still being assembled, so there is
    // still nothing to look at.
    let no_diff_yet = rendered(&started(
        "apply_patch",
        serde_json::json!({ "file_path": "src/main.rs" }),
    ));
    assert!(no_diff_yet.contains("preparing edit"), "{no_diff_yet}");
    assert!(!no_diff_yet.contains("· running"), "{no_diff_yet}");

    // The diff is on screen and speaks for itself.
    let with_diff = started(
        "apply_patch",
        serde_json::json!({
            "file_path": "src/main.rs",
            "patch": "--- a/src/main.rs\n+++ b/src/main.rs\n@@ -1 +1 @@\n-one\n+two\n",
        }),
    );
    assert!(
        matches!(
            &with_diff.order[0],
            TranscriptEntry::Tool { code_view: Some((language, _)), .. }
                if is_diff_language(language)
        ),
        "the fixture must actually produce a diff body"
    );
    let with_diff = rendered(&with_diff);
    assert!(!with_diff.contains("preparing edit"), "{with_diff}");
    assert!(with_diff.contains("running"), "{with_diff}");

    // A bodyless non-edit tool is never described as preparing one.
    let read = rendered(&started("read_file", serde_json::Value::Null));
    assert!(!read.contains("preparing edit"), "{read}");
    assert!(read.contains("running"), "{read}");
}

#[test]
fn status_path_uses_fish_style_parent_abbreviations() {
    let separator = std::path::MAIN_SEPARATOR;
    assert_eq!(
        fish_style_path_with_home(
            Path::new("/home/shulgin/borg-cli"),
            Some(Path::new("/home/shulgin"))
        ),
        format!("~{separator}borg-cli")
    );
    assert_eq!(
        fish_style_path_with_home(
            Path::new("/home/shulgin/projects/borg-cli"),
            Some(Path::new("/home/shulgin"))
        ),
        format!("~{separator}p{separator}borg-cli")
    );
    assert_eq!(
        fish_style_path_with_home(
            Path::new("/home/shulgin/.config/borg"),
            Some(Path::new("/home/shulgin"))
        ),
        format!("~{separator}.c{separator}borg")
    );
    assert_eq!(
        fish_style_path_with_home(Path::new("/home/shulgin"), Some(Path::new("/home/shulgin"))),
        "~"
    );
    assert_eq!(
        fish_style_path_with_home(
            Path::new("/srv/workspace"),
            Some(Path::new("/home/shulgin"))
        ),
        format!("{separator}s{separator}workspace")
    );
    assert_eq!(
        fish_style_path(Path::new("/workspace")),
        format!("{separator}workspace")
    );
    assert_eq!(
        fish_style_path(Path::new("projects/borg-cli")),
        format!("p{separator}borg-cli")
    );
    assert_eq!(fish_style_path(Path::new("/")), separator.to_string());
}

#[test]
fn footer_metadata_preserves_the_full_working_directory_path() {
    assert_eq!(
        footer_metadata_text("94% context left", "~/borg-cli", usize::MAX),
        "94% context left · ~/borg-cli "
    );
    let metadata = footer_metadata_text("94% context left", "~/borg-cli", 18);
    assert_eq!(metadata, "94%… · ~/borg-cli ");
    assert!(
        metadata.ends_with("~/borg-cli "),
        "footer metadata must keep the complete path: {metadata}"
    );
    assert_eq!(
        footer_metadata_text("", "~/a/very-long-directory", 8),
        "~/a/very-long-directory "
    );
}

#[test]
fn footer_metadata_highlights_only_imminent_compaction() {
    let line = footer_metadata_line(
        "compaction imminent (20% left)",
        "~/borg-cli",
        true,
        usize::MAX,
    );

    assert_eq!(line.spans[0].style.fg, Some(Color::Yellow));
    assert_eq!(line.spans[1].content, STATUS_SEPARATOR);
    assert_eq!(line.spans[1].style.fg, Some(Color::Gray));
    assert_eq!(line.spans[2].style.fg, Some(Color::Gray));

    let cwd_only = footer_metadata_line(
        "compaction imminent (20% left)",
        "~/a/very-long-directory",
        true,
        8,
    );
    assert_eq!(cwd_only.spans[0].style.fg, Some(Color::Gray));
}

#[test]
fn git_worktree_status_is_compact_and_includes_divergence_and_dirty_state() {
    let status = parse_git_worktree_status(
        "## feature/ui...origin/feature/ui [ahead 2, behind 1]\n M src/main.rs\n",
    )
    .expect("git status");

    assert_eq!(status.branch, "feature/ui");
    assert!(status.dirty);
    assert_eq!(status.compact_label(), "git:feature/ui · ↑2 · ↓1 · dirty");
    assert_eq!(
        parse_git_worktree_status("## HEAD (no branch)\n")
            .expect("detached status")
            .compact_label(),
        "git:detached"
    );
    assert!(parse_git_worktree_status("not a git status header").is_none());
}

#[test]
fn git_status_falls_back_cleanly_outside_a_worktree() {
    let missing = std::env::temp_dir().join(format!("borg-missing-worktree-{}", Uuid::new_v4()));
    assert_eq!(read_git_worktree_status(&missing), None);
}

#[test]
fn focused_transcript_configuration_switches_cwd_metadata() {
    let config = |cwd: &str| SessionDisplayConfig {
        cwd: PathBuf::from(cwd),
        provider: CodingProvider::Codex,
        model: None,
        effort: None,
        response_language: ResponseLanguage::default(),
        fast: false,
        permission_mode: PermissionMode::FullAccess,
    };
    let child_id = Uuid::new_v4();
    let mut displayed = Transcript::default();
    displayed.config = Some(config("/workspace/director"));
    let mut child = Transcript::default();
    child.config = Some(config("/workspace/child"));
    let mut director = None;
    let mut children = HashMap::from([(child_id, child)]);

    switch_to_child_transcript(&mut displayed, &mut director, &mut children, child_id);
    assert!(displayed.config_statuses().4.ends_with("child"));
    switch_to_director_transcript(&mut displayed, &mut director, &mut children, child_id);
    assert!(displayed.config_statuses().4.ends_with("director"));
}

#[test]
fn command_palette_keybinding_columns_keep_a_space_before_chords() {
    let keymap = KeyMap::from_config(&KeybindingConfig::default()).expect("default keymap");
    let scroll = command_palette_options(&keymap)
        .into_iter()
        .find(|option| option.label.starts_with("scroll transcript"))
        .expect("scroll keybinding row");
    assert!(
        scroll.label.starts_with("scroll transcript "),
        "{}",
        scroll.label
    );
    assert!(
        scroll.label.ends_with("pageup/pagedown"),
        "{}",
        scroll.label
    );
}

#[test]
fn completed_goal_crosses_out_only_its_objective() {
    let mut transcript = Transcript::default();
    let mut goal = SessionGoal::new("Ship the terminal polish".to_string(), None);
    goal.status = GoalStatus::Complete;
    transcript.order.push(TranscriptEntry::Goal {
        goal,
        time: "12:00".to_string(),
    });

    let lines = transcript.lines(80);
    let header = lines
        .iter()
        .flat_map(|line| line.spans.iter())
        .find(|span| span.content.contains("Goal"))
        .expect("goal header");
    let objective = lines
        .iter()
        .flat_map(|line| line.spans.iter())
        .find(|span| span.content.contains("Ship the terminal polish"))
        .expect("goal objective");

    assert!(!header.style.add_modifier.contains(Modifier::CROSSED_OUT));
    assert_eq!(objective.style.fg, Some(Color::DarkGray));
    assert!(objective.style.add_modifier.contains(Modifier::CROSSED_OUT));
}

#[test]
fn ctrl_c_only_exits_when_repeated_quickly() {
    let start = Instant::now();
    let mut last = None;

    assert!(!repeated_ctrl_c(&mut last, start));
    assert!(repeated_ctrl_c(&mut last, start + DOUBLE_CTRL_C_WINDOW));
    assert!(last.is_none());
    assert!(!repeated_ctrl_c(
        &mut last,
        start + DOUBLE_CTRL_C_WINDOW + Duration::from_millis(1)
    ));
}

#[test]
fn shift_or_alt_enter_inserts_a_composer_newline() {
    let keymap = KeyMap::from_config(&KeybindingConfig::default()).unwrap();
    assert!(is_composer_newline(
        &keymap,
        &KeyEvent::new(KeyCode::Enter, KeyModifiers::SHIFT)
    ));
    assert!(is_composer_newline(
        &keymap,
        &KeyEvent::new(KeyCode::Enter, KeyModifiers::ALT)
    ));
    assert!(!is_composer_newline(
        &keymap,
        &KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE)
    ));
}

#[test]
fn composer_history_restores_the_unsent_draft() {
    let mut composer = Composer::default();
    composer.insert("first");
    let _ = composer.take();
    composer.insert("second");
    let _ = composer.take();
    composer.insert("draft");

    composer.history_previous();
    assert_eq!(composer.text, "second");
    composer.history_previous();
    assert_eq!(composer.text, "first");
    composer.history_next();
    assert_eq!(composer.text, "second");
    composer.history_next();
    assert_eq!(composer.text, "draft");
}

#[test]
fn queued_prompt_recall_concatenates_text_and_preserves_image_tokens() {
    let mut composer = Composer::default();
    let first = PathBuf::from("/tmp/first.png");
    let second = PathBuf::from("/tmp/second.png");

    composer.append_recalled("first [Image 1]".to_string(), vec![first.clone()]);
    composer.append_recalled("second [Image 2]".to_string(), vec![second.clone()]);

    let (text, attachments) = composer.take();
    assert_eq!(text, "first [Image 1]\n\nsecond [Image 2]");
    assert_eq!(attachments, [first, second]);
}

#[test]
fn composer_history_rehydrates_completed_user_prompts_from_the_session_journal() {
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    let message = |sequence| {
        SessionEvent::new(
            session_id,
            sequence,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: "persistent prompt".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        )
    };
    let mut composer = Composer::default();
    composer.seed_session_events(&[message(1), message(2)]);
    composer.history_previous();
    assert_eq!(composer.text, "persistent prompt");
    assert_eq!(composer.history.len(), 1);
}

#[test]
fn composer_history_keeps_previous_resume_prompts_outside_the_visible_tail() {
    let session_id = Uuid::new_v4();
    let message = |sequence, text: &str| {
        SessionEvent::new(
            session_id,
            sequence,
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::User,
                text: text.to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        )
    };
    let previous_run = message(1, "prompt from the previous resume");
    let visible_tail = message(20_000, "prompt in the visible tail");
    let mut composer = Composer::default();

    composer.seed_session_events(std::slice::from_ref(&previous_run));
    composer.seed_session_events(&[previous_run, visible_tail]);
    composer.history_previous();
    assert_eq!(composer.text, "prompt in the visible tail");
    composer.history_previous();
    assert_eq!(composer.text, "prompt from the previous resume");
    assert_eq!(
        composer.history.len(),
        2,
        "overlapping seeds deduplicate by message id"
    );
}

#[test]
fn completed_external_prompt_joins_existing_composer_history_once() {
    let session_id = Uuid::new_v4();
    let mut composer = Composer::default();
    composer.insert("typed locally");
    let _ = composer.take();
    let local_completion = SessionEvent::new(
        session_id,
        1,
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::User,
            text: "typed locally".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
    );
    let external_completion = SessionEvent::new(
        session_id,
        2,
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::User,
            text: "sent from an attached client".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
    );

    composer.seed_session_events(&[local_completion, external_completion]);
    composer.history_previous();
    assert_eq!(composer.text, "sent from an attached client");
    composer.history_previous();
    assert_eq!(composer.text, "typed locally");
    assert_eq!(composer.history.len(), 2);
}

#[test]
fn composer_rehydration_advances_past_persisted_image_labels() {
    let session_id = Uuid::new_v4();
    let mut composer = Composer::default();
    composer.seed_session_events(&[SessionEvent::new(
        session_id,
        1,
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::User,
            text: "existing [Image 6]".to_string(),
            attachments: vec![PathBuf::from("existing.png")],
            status: MessageStatus::Complete,
            delivery: None,
        },
    )]);

    assert_eq!(
        composer.insert_attachment(PathBuf::from("next.png")),
        "Image 7"
    );

    let mut restored = Composer::default();
    restored.restore(
        "queued [Image 6]".to_string(),
        vec![PathBuf::from("queued.png")],
    );
    assert_eq!(
        restored.insert_attachment(PathBuf::from("after-queue.png")),
        "Image 7"
    );
}

#[test]
fn dumb_or_explicit_plain_terminals_use_the_line_input_fallback() {
    assert!(!rich_terminal_supported(Some("dumb"), None));
    assert!(!rich_terminal_supported(
        Some("xterm-256color"),
        Some("plain")
    ));
    assert!(rich_terminal_supported(Some("xterm-256color"), None));
}

#[test]
fn keyboard_enhancement_does_not_request_release_events() {
    let flags = keyboard_enhancement_flags();

    assert!(flags.contains(KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES));
    assert!(flags.contains(KeyboardEnhancementFlags::REPORT_ALL_KEYS_AS_ESCAPE_CODES));
    assert!(!flags.contains(KeyboardEnhancementFlags::REPORT_EVENT_TYPES));
}

#[test]
fn slash_command_picker_selects_the_highlighted_match() {
    assert_eq!(slash_matches("/int")[0].0, "/interrupt");
    assert_eq!(slash_selected_command("/mo", 0), Some("/model"));
    assert_eq!(slash_selected_command("/eff", 0), Some("/effort"));
    assert_eq!(slash_selected_command("/lang", 0), Some("/language"));
    assert_eq!(slash_selected_command("/st", 1), Some("/stop"));
    assert_eq!(slash_selected_command("/goal add", 0), None);
    assert!(slash_matches("/todo add").is_empty());
    assert!(slash_matches("plain prompt").is_empty());
    assert!(slash_matches("/").len() > 1);
}

#[test]
fn slash_command_picker_navigates_matches_beyond_the_visible_window() {
    let matches = slash_matches("/");
    let selected = 7;

    assert_eq!(
        slash_selected_command("/", selected),
        Some(matches[selected].0)
    );
    let rendered = slash_suggestion_lines("/", selected)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();
    assert_eq!(rendered.len(), 5);
    assert!(
        rendered
            .iter()
            .any(|line| line.contains(matches[selected].0))
    );
    assert!(rendered.iter().any(|line| line.contains('›')));
}

#[test]
fn markdown_tables_render_headers_rows_and_narrow_fallbacks() {
    let markdown = "| Matter | Risk |\n|:--|--:|\n| Acme | High |";
    let wide = markdown_lines(markdown, 40, None)
        .into_iter()
        .map(|line| {
            line.spans
                .into_iter()
                .map(|span| span.content.into_owned())
                .collect::<String>()
        })
        .collect::<Vec<_>>();
    assert!(wide.iter().any(|line| line.contains("Matter")));
    assert!(wide.iter().any(|line| line.contains("Acme")));
    assert!(
        wide.iter().any(|line| line.contains('┼')),
        "rendered table: {wide:?}"
    );

    let narrow = markdown_lines(markdown, 7, None)
        .into_iter()
        .flat_map(|line| line.spans)
        .map(|span| span.content.into_owned())
        .collect::<String>();
    assert!(narrow.contains("Matter:"));
    assert!(narrow.contains("Acme"));
    assert!(narrow.contains("Risk:"));
    assert!(narrow.contains("High"));
}

#[test]
fn markdown_semantics_are_visible_without_source_delimiters() {
    let rendered = markdown_lines(
        "## Architecture\nUse `V120` with **care**, *measure*, ~~discard~~, and [notes](https://example.com).\n\n1. First\n2. Second",
        80,
        Some(Color::White),
    );
    let spans = rendered
        .iter()
        .flat_map(|line| line.spans.iter())
        .collect::<Vec<_>>();

    let heading = spans
        .iter()
        .find(|span| span.content == "Architecture")
        .expect("heading");
    assert_eq!(heading.style.fg, Some(BORG_ORANGE_HOVER));
    assert!(heading.style.add_modifier.contains(Modifier::BOLD));

    let code = spans
        .iter()
        .find(|span| span.content == "V120")
        .expect("inline code");
    assert_eq!(code.style.fg, Some(Color::LightCyan));
    assert!(code.style.add_modifier.contains(Modifier::BOLD));
    assert!(
        spans.iter().all(|span| !span.content.contains('`')),
        "inline code delimiters should not be rendered"
    );

    let strong = spans
        .iter()
        .find(|span| span.content == "care")
        .expect("strong text");
    assert!(strong.style.add_modifier.contains(Modifier::BOLD));
    let emphasis = spans
        .iter()
        .find(|span| span.content == "measure")
        .expect("emphasized text");
    assert!(emphasis.style.add_modifier.contains(Modifier::ITALIC));
    let struck = spans
        .iter()
        .find(|span| span.content == "discard")
        .expect("struck text");
    assert!(struck.style.add_modifier.contains(Modifier::CROSSED_OUT));
    let link = spans
        .iter()
        .find(|span| span.content == "notes")
        .expect("link");
    assert_eq!(link.style.fg, Some(Color::LightBlue));
    assert!(link.style.add_modifier.contains(Modifier::UNDERLINED));
    assert!(spans.iter().any(|span| {
        span.content == "1. "
            && span.style.fg == Some(BORG_ORANGE_HOVER)
            && span.style.add_modifier.contains(Modifier::BOLD)
    }));
}

#[test]
fn markdown_links_retain_only_clickable_http_destinations() {
    let markdown = "[Borg docs](https://example.com/docs) and [local](file:///tmp/private.txt)";
    let lines = markdown_lines(markdown, 80, None);

    assert_eq!(
        markdown_link_ranges(markdown, &lines),
        vec![LinkRowRange {
            row: 0,
            start: 0,
            end: 9,
            url: "https://example.com/docs".to_string(),
        }]
    );
}

#[test]
fn markdown_math_uses_a_real_terminal_layout() {
    let rendered = markdown_lines(
        "A $2 \\times 2$ map satisfies\n\n$$\n\\frac{a}{b} = \\bar{z}\n$$",
        80,
        Some(Color::White),
    );
    let text = rendered
        .iter()
        .map(Line::to_string)
        .collect::<Vec<_>>()
        .join("\n");

    assert!(text.contains("2 × 2"));
    assert!(text.contains('─'));
    assert!(text.contains('‾'));
    assert!(!text.contains("\\times"));
    assert!(!text.contains("\\bar"));
}

#[test]
fn markdown_currency_does_not_become_terminal_math() {
    let rendered = markdown_lines(
        "It costs only $0.1667/hour (~$4/day) and currently holds:",
        80,
        Some(Color::White),
    );
    let text = rendered
        .iter()
        .map(Line::to_string)
        .collect::<Vec<_>>()
        .join("\n");

    assert!(text.contains("$0.1667/hour (~$4/day)"), "{text}");
    assert!(!text.contains('─'), "{text}");
}

#[test]
fn quoted_pasted_text_keeps_gutter_without_code_line_numbers() {
    let markdown = "> ```text\n> pasted first line\n> pasted second line\n> ```";
    let rendered = markdown_lines(markdown, 80, None)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>();

    assert!(
        rendered
            .iter()
            .filter(|line| !line.is_empty())
            .all(|line| line.starts_with("│ ")),
        "rendered quote: {rendered:?}"
    );
    assert!(
        rendered
            .iter()
            .any(|line| line.contains("pasted first line"))
    );
    assert!(!rendered.iter().any(|line| line.contains("1 │")));
}

#[test]
fn tool_call_summaries_cover_cli_display_contract() {
    assert_eq!(
        tool_call_summary(
            "functions.exec_command",
            &serde_json::json!({"cmd": "rg -g '*.rs' -n 'tool.?call' crates/borg-cli"})
        ),
        ("Search".to_string(), "“tool.?call”".to_string())
    );
    assert_eq!(
        tool_call_summary(
            "functions.exec_command",
            &serde_json::json!({
                "cmd": "/usr/bin/bash -c \"rg -n 'tool.?call' crates/borg-cli\""
            })
        ),
        ("Search".to_string(), "“tool.?call”".to_string())
    );
    assert_eq!(
        tool_code_view(
            "functions.exec_command",
            &serde_json::json!({"cmd": "cargo check -p borg"})
        ),
        Some(("command".to_string(), "cargo check -p borg".to_string()))
    );
    assert_eq!(
        tool_code_view(
            "functions.exec_command",
            &serde_json::json!({
                "cmd": "/usr/bin/bash -c \"sed -n '1,20p' src/main.rs\""
            })
        ),
        Some((
            "command".to_string(),
            "sed -n '1,20p' src/main.rs".to_string()
        ))
    );
    assert_eq!(
        tool_call_summary(
            "mcp__filesystem__read_file",
            &serde_json::json!({"path": "/workspace/src/main.rs"})
        ),
        ("Read".to_string(), "/workspace/src/main.rs".to_string())
    );
    assert_eq!(
        tool_call_summary(
            "web.run",
            &serde_json::json!({
                "search_query": [
                    {"q": "Borg CLI"},
                    {"q": "terminal UI"}
                ]
            })
        ),
        (
            "Search web".to_string(),
            "“Borg CLI · terminal UI”".to_string()
        )
    );
    assert_eq!(
        tool_call_summary(
            "web_search",
            &serde_json::json!({"query": "Borg queue semantics"})
        ),
        (
            "Search web".to_string(),
            "“Borg queue semantics”".to_string()
        )
    );
    assert_eq!(
        tool_call_summary(
            "functions.apply_patch",
            &serde_json::json!("*** Begin Patch\n*** Update File: src/main.rs\n")
        ),
        ("Edit".to_string(), "src/main.rs".to_string())
    );
    assert_eq!(
        tool_call_summary(
            "mcp__borg_agent__update_plan",
            &serde_json::json!({"plan": [{"content": "Inspect"}, {"content": "Verify"}]})
        ),
        ("Update plan".to_string(), "2 steps".to_string())
    );
    assert_eq!(
        tool_call_summary(
            "mcp__example__custom_action",
            &serde_json::json!({"value": 42})
        ),
        ("Custom action".to_string(), "value: 42".to_string())
    );
    assert_eq!(
        tool_call_summary(
            "mcp__borg_agent__spawn_agent",
            &serde_json::json!({
                "task_name": "inspect_ui",
                "message": "Inspect the renderer",
                "provider": "codex"
            })
        ),
        ("Spawn agent".to_string(), "inspect_ui · codex".to_string())
    );
    assert_eq!(
        tool_call_summary(
            "functions.collaboration.followup_task",
            &serde_json::json!({"target": "inspect_ui", "message": "Run focused tests"})
        ),
        (
            "Follow up".to_string(),
            "inspect_ui · Run focused tests".to_string()
        )
    );
    assert_eq!(
        tool_call_summary(
            "mcp__borg_agent__create_goal",
            &serde_json::json!({"objective": "Ship readable events", "token_budget": 4000})
        ),
        (
            "Create goal".to_string(),
            "Ship readable events · 4000 tokens".to_string()
        )
    );
    assert_eq!(
        tool_call_summary(
            "mcp__borg_agent__lsp_definition",
            &serde_json::json!({"path": "src/main.rs", "line": 42, "character": 7})
        ),
        (
            "Go to definition".to_string(),
            "src/main.rs:42:7".to_string()
        )
    );
}

#[test]
fn borg_control_results_render_compact_roster_plan_goal_and_follow_up() {
    let roster = borg_control_tool_output_view(
        "mcp__borg_agent__list_agents",
        None,
        r#"{"agents":[{"task_name":"inspect_ui","status":"running","model":"codex","effort":"low","message":"Inspect the renderer"}]}"#,
    )
    .expect("structured roster");
    assert!(roster.contains("TEAM · 1 subagent"));
    assert!(roster.contains("running  inspect_ui · codex/low"));
    assert!(roster.contains("Inspect the renderer"));
    assert!(!roster.contains("Debug payload"));
    assert!(!roster.contains("\"agents\""));

    let plan = borg_control_tool_output_view(
        "functions.update_plan",
        None,
        r#"{"plan":[{"status":"in_progress","step":"Render team activity"}]}"#,
    )
    .expect("structured plan");
    assert!(plan.contains("PLAN · 1 step"));
    assert!(plan.contains("in_progress  Render team activity"));

    let goal = borg_control_tool_output_view(
        "mcp__borg_agent__get_goal",
        None,
        r#"{"goal":{"status":"active","objective":"Ship calm controls"}}"#,
    )
    .expect("structured goal");
    assert_eq!(goal, "GOAL · active · Ship calm controls");

    let follow_up = borg_control_tool_output_view(
        "functions.collaboration.followup_task",
        Some(&serde_json::json!({"target":"inspect_ui","message":"Run focused tests"})),
        "{}",
    )
    .expect("structured follow-up");
    assert!(follow_up.contains("FOLLOW UP · inspect_ui"));
    assert!(follow_up.contains("Run focused tests"));
}

#[test]
fn borg_control_results_tolerate_partial_payloads_and_leave_unknown_tools_generic() {
    let partial = borg_control_tool_output_view(
        "functions.collaboration.send_message",
        Some(&serde_json::json!({"target":"research","message":"Please check tests"})),
        "{}",
    )
    .expect("message activity");
    assert!(partial.contains("MESSAGE · research"));
    assert!(partial.contains("Please check tests"));
    assert!(borg_control_tool_output_view("mcp__example__new_tool", None, "{}").is_none());
    assert!(
        borg_control_tool_output_view("mcp__borg_agent__list_agents", None, "not json").is_none()
    );
}

#[test]
fn unread_team_messages_render_the_structured_payload_not_the_mcp_envelope() {
    let output = serde_json::json!({
        "_meta": null,
        "content": [{
            "type": "text",
            "text": "[{\"delivery\":\"queue\",\"message_id\":\"message-1\",\"text\":\"Please inspect the failing benchmark\"}]"
        }],
        "structuredContent": [{
            "delivery": "queue",
            "message_id": "message-1",
            "text": "Please inspect the failing benchmark"
        }]
    })
    .to_string();

    let rendered = borg_control_tool_output_view(
        "mcp__borg_agent__list_unread_team_messages",
        Some(&serde_json::json!({})),
        &output,
    )
    .expect("structured unread messages");

    assert_eq!(
        rendered,
        "UNREAD · 1 message\n       queue  Please inspect the failing benchmark"
    );
    assert!(!rendered.contains("structuredContent"));
    assert!(!rendered.contains("_meta"));
}

#[test]
fn wait_spawn_and_lsp_diagnostics_use_compact_stable_result_shapes() {
    let wait = borg_control_tool_output_view(
        "wait_agent",
        Some(&serde_json::json!({"target":"review"})),
        r#"{"task_name":"review","status":"completed","model":"codex","effort":"low","final_text":"Reviewed the patch"}"#,
    )
    .expect("structured wait");
    assert!(wait.contains("WAIT · completed · review · codex/low"));
    assert!(wait.contains("Reviewed the patch"));

    let spawn = borg_control_tool_output_view(
        "spawn_agent",
        Some(&serde_json::json!({"task_name":"lint"})),
        r#"{"agent":{"task_name":"lint","status":"running","id":"session-7"}}"#,
    )
    .expect("structured spawn");
    assert!(spawn.contains("SPAWN · running · lint"));

    let diagnostics = borg_lsp_diagnostics_view(
        "lsp_diagnostics",
        Some(&serde_json::json!({"path":"src/main.rs"})),
        r#"{"items":[{"severity":1,"message":"expected expression","range":{"start":{"line":4}}}]}"#,
    )
    .expect("structured diagnostics");
    assert!(diagnostics.contains("DIAGNOSTICS · src/main.rs · 1 issue"));
    assert!(diagnostics.contains("error:5  expected expression"));
    assert!(borg_lsp_diagnostics_view("lsp_diagnostics", None, "{}").is_none());
}

#[test]
fn active_subagent_count_tracks_only_working_children() {
    let mut transcript = Transcript::default();
    assert_eq!(transcript.active_subagent_count(), 0);

    transcript
        .subagents
        .insert(Uuid::new_v4(), SubagentStatus::Running);
    transcript
        .subagents
        .insert(Uuid::new_v4(), SubagentStatus::WaitingForApproval);
    transcript
        .subagents
        .insert(Uuid::new_v4(), SubagentStatus::Stopped);
    transcript
        .subagents
        .insert(Uuid::new_v4(), SubagentStatus::Ready);

    assert_eq!(transcript.active_subagent_count(), 2);

    transcript
        .subagents
        .values_mut()
        .for_each(|status| *status = SubagentStatus::Ready);
    assert_eq!(transcript.active_subagent_count(), 0);
}

#[test]
fn agents_status_label_counts_only_working_children() {
    let working = agents_status_label(1).expect("one child is working");
    let larger_team = agents_status_label(2).expect("two children are working");
    let idle = agents_status_label(0);

    assert_eq!(working, "1 subagent");
    assert_eq!(larger_team, "2 subagents");
    assert_eq!(idle, None);
}

#[test]
fn agent_roster_contains_only_currently_working_children() {
    let parent_id = Uuid::new_v4();
    let now = chrono::Utc::now();
    let snapshot = |name: &str, status, age_minutes| SubagentSnapshot {
        session_id: Uuid::new_v4(),
        parent_session_id: parent_id,
        task_name: format!("/root/{name}"),
        status,
        provider: CodingProvider::Codex,
        model: None,
        effort: None,
        cwd: PathBuf::from("/workspace"),
        created_at: now,
        updated_at: now - chrono::Duration::minutes(age_minutes),
        detail: None,
        final_text: None,
        usage: borg_remote::SubagentUsage::default(),
    };
    let agents = [
        snapshot("z_live", SubagentStatus::Running, 20),
        snapshot("a_starting", SubagentStatus::Starting, 10),
        snapshot("b_waiting", SubagentStatus::WaitingForApproval, 9),
        snapshot("idle", SubagentStatus::Ready, 8),
        snapshot("oldest", SubagentStatus::Failed, 6),
        snapshot("older", SubagentStatus::Stopped, 5),
    ];
    let mut transcript = Transcript::default();
    for agent in agents {
        transcript
            .subagent_snapshots
            .insert(agent.session_id, agent);
    }

    let rows = transcript
        .agent_roster_entries()
        .into_iter()
        .map(|(row, _)| row)
        .collect::<Vec<_>>();

    assert_eq!(rows.len(), 4);
    assert!(rows[1].starts_with("a_starting "));
    assert!(rows[2].starts_with("b_waiting "));
    assert!(rows[3].starts_with("z_live "));
    assert!(!rows.iter().any(|row| row.contains("idle")));
    assert!(!rows.iter().any(|row| row.contains("oldest")));
    assert!(!rows.iter().any(|row| row.contains("older")));
}

#[test]
fn agents_status_hover_underlines_only_the_label() {
    let spinner = agents_status_spinner_style(true);
    let label = agents_status_text_style(true);

    assert!(!spinner.add_modifier.contains(Modifier::UNDERLINED));
    assert!(label.add_modifier.contains(Modifier::UNDERLINED));
}

#[test]
fn model_effort_and_permission_hover_show_bottom_interaction_hints() {
    let hint = |agents, model, effort, permission| {
        bottom_interaction_hint(BottomInteractionHintState {
            agents_status_hovered: agents,
            model_status_hovered: model,
            effort_status_hovered: effort,
            permission_status_hovered: permission,
            ..BottomInteractionHintState::default()
        })
    };

    assert_eq!(
        hint(true, false, false, false),
        Some("left click to open subagents menu")
    );
    assert_eq!(
        hint(false, true, false, false),
        Some("left click change model")
    );
    assert_eq!(
        hint(false, false, true, false),
        Some("left click change effort")
    );
    assert_eq!(
        hint(false, false, false, true),
        Some("left click change permissions")
    );
    assert_eq!(hint(false, false, false, false), None);
}

#[test]
fn effort_and_permission_status_colors_reflect_their_values() {
    assert_eq!(effort_status_color("low"), Color::LightGreen);
    assert_eq!(effort_status_color("medium"), Color::Cyan);
    assert_eq!(effort_status_color("high"), Color::Yellow);
    assert_eq!(effort_status_color("xhigh"), Color::LightMagenta);
    assert_eq!(effort_status_color("max"), Color::LightRed);
    assert_eq!(effort_status_color("ultra"), Color::LightRed);
    assert_eq!(effort_status_color("custom"), Color::Gray);

    assert_eq!(
        permission_status_color("manual approvals"),
        Color::LightGreen
    );
    assert_eq!(permission_status_color("auto approvals"), Color::Yellow);
    assert_eq!(permission_status_color("full access"), Color::LightRed);
    assert_eq!(permission_status_color("custom"), Color::Gray);
}

#[test]
fn value_colored_status_segments_keep_hover_styling() {
    let mut resting = Vec::new();
    push_interactive_status_segment(
        &mut resting,
        Some("high".to_string()),
        false,
        effort_status_color("high"),
    );
    assert_eq!(resting[1].style.fg, Some(Color::Yellow));
    assert!(!resting[1].style.add_modifier.contains(Modifier::UNDERLINED));

    let mut hovered = Vec::new();
    push_interactive_status_segment(
        &mut hovered,
        Some("full access".to_string()),
        true,
        permission_status_color("full access"),
    );
    assert_eq!(hovered[1].style.fg, Some(Color::White));
    assert!(hovered[1].style.add_modifier.contains(Modifier::BOLD));
    assert!(hovered[1].style.add_modifier.contains(Modifier::UNDERLINED));

    let mut fast = Vec::new();
    push_interactive_status_segment(
        &mut fast,
        Some("fast".to_string()),
        false,
        Color::LightYellow,
    );
    assert_eq!(fast[1].style.fg, Some(Color::LightYellow));
}

#[test]
fn subagent_activity_collapses_chatter_and_keeps_terminal_result() {
    let parent_id = Uuid::new_v4();
    let child_id = Uuid::new_v4();
    let now = chrono::Utc::now();
    let mut agent = SubagentSnapshot {
        session_id: child_id,
        parent_session_id: parent_id,
        task_name: "inspect_ui".to_string(),
        status: SubagentStatus::Running,
        provider: CodingProvider::Codex,
        model: None,
        effort: None,
        cwd: PathBuf::from("/workspace"),
        created_at: now,
        updated_at: now,
        detail: None,
        final_text: None,
        usage: borg_remote::SubagentUsage::default(),
    };
    let activity = |sequence, activity, agent: &SubagentSnapshot, event| {
        SessionEvent::new(
            parent_id,
            sequence,
            SessionEventKind::SubagentActivity {
                activity,
                agent: agent.clone(),
                event,
            },
        )
    };
    let mut transcript = Transcript::default();
    transcript.apply(&activity(1, SubagentActivityKind::Started, &agent, None));
    assert_eq!(transcript.order.len(), 1);
    transcript.apply(&activity(
        2,
        SubagentActivityKind::Updated,
        &agent,
        Some(Box::new(SessionEvent::new(
            child_id,
            1,
            SessionEventKind::ReasoningDelta {
                text: "working chatter".to_string(),
            },
        ))),
    ));
    assert_eq!(transcript.order.len(), 1);
    transcript.apply(&activity(
        3,
        SubagentActivityKind::Updated,
        &agent,
        Some(Box::new(SessionEvent::new(
            child_id,
            2,
            SessionEventKind::Message {
                message_id: Uuid::new_v4(),
                actor: EventActor::Assistant,
                text: "Found the renderer issue without another user prompt.".to_string(),
                attachments: Vec::new(),
                status: MessageStatus::Complete,
                delivery: None,
            },
        ))),
    ));
    assert!(matches!(
        &transcript.order[0],
        TranscriptEntry::Activity { text, .. }
            if text == "agent · inspect_ui · report · Found the renderer issue without another user prompt."
    ));
    agent.status = SubagentStatus::WaitingForApproval;
    transcript.apply(&activity(
        4,
        SubagentActivityKind::Updated,
        &agent,
        Some(Box::new(SessionEvent::new(
            child_id,
            3,
            SessionEventKind::ApprovalRequested {
                approval_id: "approval-1".to_string(),
                title: "Run focused tests?".to_string(),
                detail: "Cargo will compile the CLI".to_string(),
                command: None,
            },
        ))),
    ));
    assert_eq!(transcript.order.len(), 1);
    assert!(matches!(
        &transcript.order[0],
        TranscriptEntry::Activity { text, .. }
            if text == "agent · inspect_ui · needs approval · Run focused tests?"
    ));
    agent.status = SubagentStatus::Stopped;
    agent.final_text = Some("Found the renderer issue.\nExtra detail".to_string());
    transcript.apply(&activity(5, SubagentActivityKind::Completed, &agent, None));

    assert_eq!(transcript.order.len(), 1);
    assert!(matches!(
        &transcript.order[0],
        TranscriptEntry::Activity { text, .. }
            if text == "agent · inspect_ui · completed · Found the renderer issue."
    ));
}

#[test]
fn transcript_copy_selection_can_move_beyond_last_assistant_message() {
    let mut transcript = Transcript::default();
    transcript.order.push(TranscriptEntry::Message {
        actor: EventActor::Assistant,
        text: "answer".to_string(),
        attachments: Vec::new(),
        model: None,
        effort: None,
        time: "12:00".to_string(),
        status: MessageStatus::Complete,
        complete: true,
    });
    transcript.order.push(TranscriptEntry::Activity {
        text: "subagent · completed".to_string(),
        time: "12:01".to_string(),
    });

    assert_eq!(transcript.copy_text(), Some("answer"));
    transcript.select_previous();
    assert_eq!(transcript.copy_text(), Some("subagent · completed"));
    transcript.select_previous();
    assert_eq!(transcript.copy_text(), Some("answer"));
    transcript.select_next();
    assert_eq!(transcript.copy_text(), Some("subagent · completed"));
}

#[test]
fn assistant_message_actions_stay_out_of_the_transcript() {
    let mut transcript = Transcript::default();
    transcript.order.push(TranscriptEntry::Message {
        actor: EventActor::Assistant,
        text: "answer".to_string(),
        attachments: Vec::new(),
        model: None,
        effort: None,
        time: "12:00".to_string(),
        status: MessageStatus::Complete,
        complete: true,
    });

    let idle = transcript.lines(80);
    assert!(
        !idle
            .iter()
            .any(|line| line.to_string().contains("Copy response"))
    );
    let actions = Picker::new(
        PickerKind::MessageActions,
        "Message actions",
        ["Revert to here", "Copy response"],
        None,
    );
    assert_eq!(
        actions
            .options
            .iter()
            .map(|option| option.label.as_str())
            .collect::<Vec<_>>(),
        ["Revert to here", "Copy response"]
    );
}

#[test]
fn fuzzy_match_accepts_subsequences_and_rejects_reordering() {
    assert!(fuzzy_matches("/goal", "gl"));
    assert!(fuzzy_matches("/expand-tools", "expt"));
    assert!(fuzzy_matches("/EXPAND-TOOLS", "expand"));
    assert!(fuzzy_matches("scroll transcript    ctrl+u", "scroll"));
    // Spaces in the query span the gap between label and key.
    assert!(fuzzy_matches("send                 enter", "send ent"));
    assert!(!fuzzy_matches("/goal", "lg"));
    assert!(!fuzzy_matches("/goal", "goalx"));
}

/// The palette is one list over two sources, and filtering it must not strip a
/// section off the rows that survive.
#[test]
fn the_command_palette_filters_across_commands_and_keybindings() {
    let keymap = KeyMap::from_config(&crate::agent_config::KeybindingConfig::default())
        .expect("default keymap");
    let mut picker = Picker {
        kind: PickerKind::Commands,
        title: "Commands and keybindings",
        options: command_palette_options(&keymap),
        selected: 0,
        query: Some(String::new()),
    };
    let rendered = |picker: &Picker| picker.display(72);

    let all = rendered(&picker);
    assert!(all.contains("COMMANDS"), "{all}");
    assert!(all.contains("KEYBINDINGS"), "{all}");
    assert!(all.contains("/goal"), "{all}");

    // A query matching only keybinding rows keeps their heading.
    picker.set_query("scroll".to_string());
    let scroll = rendered(&picker);
    assert!(scroll.contains("KEYBINDINGS"), "{scroll}");
    assert!(scroll.contains("scroll transcript"), "{scroll}");
    assert!(!scroll.contains("COMMANDS"), "{scroll}");
    assert!(
        scroll.contains("· scroll"),
        "header echoes the query: {scroll}"
    );

    // The selection follows the filter instead of pointing at a hidden row.
    let selected = picker.options[picker.selected].label.clone();
    assert!(selected.contains("scroll"), "{selected}");

    picker.set_query("zzzz".to_string());
    let empty = rendered(&picker);
    assert!(empty.contains("no match"), "{empty}");
}

#[test]
fn resume_picker_filters_models_and_pages_without_wrapping() {
    let mut local = PickerOption::new("Aug 1 · local", "local");
    local.preview = Some("Latest response\n> **Model:** `gpt-5.6-sol`".to_string());
    local.section = Some("Current directory".to_string());
    let mut global = PickerOption::new("Jul 31 · global", "global");
    global.preview = Some("Older response\n> **Model:** `claude-opus-5`".to_string());
    global.section = Some("All directories".to_string());
    let mut picker = Picker {
        kind: PickerKind::Resume,
        title: "Resume session",
        options: vec![local, global],
        selected: 0,
        query: None,
    };

    picker.set_query("claude-opus".to_string());
    assert_eq!(picker.matches(), vec![1]);
    assert_eq!(picker.selected, 1);
    let rendered = picker.display(112);
    assert!(
        rendered.contains("Resume session · claude-opus"),
        "{rendered}"
    );
    assert!(rendered.contains("ALL DIRECTORIES"), "{rendered}");
    picker.set_query(String::new());
    picker.selected = 0;
    picker.page(12);
    assert_eq!(picker.selected, 1);
    picker.page(12);
    assert_eq!(picker.selected, 1, "page navigation must stop at the end");
}

#[test]
fn resume_picker_scroll_keeps_every_loaded_option_reachable() {
    let options = (0..24)
        .map(|index| {
            let mut option = PickerOption::new(format!("Session {index:02}"), index.to_string());
            option.preview = Some(format!("Preview for session {index:02}"));
            if index == 0 {
                option.section = Some("Current directory".to_string());
            } else if index == 8 {
                option.section = Some("All directories".to_string());
            }
            option
        })
        .collect::<Vec<_>>();
    let mut picker = Picker {
        kind: PickerKind::Resume,
        title: "Resume session",
        options,
        selected: 0,
        query: Some(String::new()),
    };

    assert!(picker.scroll(23));
    assert_eq!(picker.selected, 23);
    let line_count = picker.styled_lines(112, USER_LABEL_BLUE, USER_TEXT).len();
    let offset = picker.scroll_offset(8, line_count);
    let selected_line = picker
        .option_row_offsets()
        .into_iter()
        .find_map(|(index, line)| (index == picker.selected).then_some(line))
        .expect("selected option has a rendered row");
    assert!((offset..offset + 8).contains(&selected_line));
    assert!(
        !picker.scroll(1),
        "wheel scrolling stops at the last option"
    );

    picker.page(-12);
    assert_eq!(picker.selected, 11);
    picker.page(-12);
    assert_eq!(picker.selected, 0);
}

#[test]
fn launch_resume_picker_height_is_stable_and_reserved_once() {
    let short_preview = composer_panel_height(4, 0, 18, true);
    let long_preview = composer_panel_height(40, 0, 18, true);
    assert_eq!(short_preview, 20);
    assert_eq!(long_preview, 20);

    let bounded = bounded_launch_composer_height(short_preview, 24, 1);
    assert_eq!(bounded, 17);
    let chunks = terminal_vertical_chunks(Rect::new(0, 0, 100, 24), 0, bounded, 1, true);
    assert_eq!(chunks[0].height, 23);
    assert_eq!(
        chunks[3].height, 0,
        "nested launch composer is not reserved twice"
    );
    assert!(bounded.saturating_add(6 + 1) <= 24);
}

/// Only commands whose bare form is not a command need finishing by hand;
/// everything else must run outright or the palette is just a typing aid.
#[test]
fn only_argument_taking_commands_are_inserted_rather_than_run() {
    assert!(slash_command_needs_argument("/queue"));
    assert!(slash_command_needs_argument("/steer"));
    for (command, _) in SLASH_COMMANDS
        .iter()
        .filter(|(command, _)| !matches!(*command, "/queue" | "/steer"))
    {
        assert!(
            !slash_command_needs_argument(command),
            "{command} would be inserted instead of run"
        );
    }
}

#[test]
fn resume_picker_uses_a_balanced_two_column_layout() {
    let picker = Picker {
            kind: PickerKind::Resume,
            title: "Resume session",
            options: vec![
                PickerOption {
                    label: "Jul 26 18:57 · We need to overhaul the interaction".to_string(),
                    value: "one".to_string(),
                    preview: Some(
                    "We need **bold decisions** and `typed contracts`.\n\n---\n> **Directory:** `/home/shulgin/borg`"
                        .to_string(),
                    ),
                    section: Some("Current directory".to_string()),
                },
                PickerOption {
                    label: "Jul 26 18:56 · No user prompt recorded".to_string(),
                    value: "two".to_string(),
                    preview: None,
                    section: Some("All directories".to_string()),
                },
            ],
            selected: 0,
            query: None,
        };

    let rendered = picker.display(112);
    let rows = rendered.lines().collect::<Vec<_>>();

    assert!(rows[0].contains("Resume session"));
    assert!(rows[0].contains("type to filter"));
    assert!(rows[0].contains("PgUp/PgDn older"));
    assert!(rows[0].contains("Latest response"));
    assert!(rows.iter().any(|row| row.contains("CURRENT DIRECTORY")));
    assert!(rows.iter().any(|row| row.contains("ALL DIRECTORIES")));
    assert!(
        rows.iter()
            .any(|row| row.contains("We need bold decisions and typed contracts"))
    );
    assert!(
        rows.iter().all(|row| UnicodeWidthStr::width(*row) <= 112),
        "picker rows must fit their actual render width: {rows:?}"
    );

    let styled = picker.styled_lines(112, USER_LABEL_BLUE, USER_TEXT);
    assert!(styled.iter().flat_map(|line| &line.spans).any(|span| {
        span.content == "bold decisions" && span.style.add_modifier.contains(Modifier::BOLD)
    }));
    assert!(styled.iter().flat_map(|line| &line.spans).any(|span| {
        span.content == "typed contracts" && span.style.fg == Some(Color::LightCyan)
    }));
}

#[test]
fn viewport_range_lookup_skips_large_offscreen_history() {
    let rows = (0..100_000)
        .map(|index| (index, index * 3, index * 3 + 2))
        .collect::<Vec<_>>();

    let visible = visible_row_ranges(&rows, 240_000, 40);

    assert!(visible.len() <= 15, "visible ranges: {}", visible.len());
    assert_eq!(visible.first().map(|(_, start, _)| *start), Some(240_000));
    assert!(
        visible
            .iter()
            .all(|(_, start, end)| *end > 240_000 && *start < 240_040)
    );
}

#[test]
fn picker_wheel_hands_off_only_after_reaching_its_boundary() {
    let mut picker = Picker::new(
        PickerKind::MessageActions,
        "Actions",
        ["First", "Second", "Third"],
        Some("Second"),
    );

    assert!(picker.scroll(-1));
    assert_eq!(picker.selected, 0);
    assert!(!picker.scroll(-1));
    assert!(picker.scroll(1));
    assert!(picker.scroll(1));
    assert_eq!(picker.selected, 2);
    assert!(!picker.scroll(1));
}

#[test]
fn picker_hover_selects_the_option_without_a_click() {
    let mut picker = Picker::new(
        PickerKind::MessageActions,
        "Actions",
        ["First", "Second", "Third"],
        None,
    );

    assert!(picker.select_hovered(&MouseEventKind::Moved, Some(2)));

    assert_eq!(picker.selected, 2);
    assert!(!picker.select_hovered(&MouseEventKind::Down(MouseButton::Left), Some(1),));
    assert_eq!(picker.selected, 2);
    assert!(!picker.select_hovered(&MouseEventKind::Moved, None));
    assert!(!picker.select_index(3));
    assert_eq!(picker.selected, 2);
    assert_eq!(picker.selected_value(), "Third");
}

#[test]
fn picker_hover_uses_actual_option_indices_after_filtering() {
    let mut picker = Picker {
        kind: PickerKind::Effort,
        title: "Choose effort",
        options: ["low", "medium", "high"]
            .into_iter()
            .map(|value| PickerOption::new(value, value))
            .collect(),
        selected: 0,
        query: Some("high".to_string()),
    };

    assert_eq!(picker.option_row_offsets(), vec![(2, 1)]);
    assert!(picker.select_hovered(&MouseEventKind::Moved, Some(2)));
    assert_eq!(picker.selected_value(), "high");
}

#[test]
fn picker_hit_offsets_match_rendered_rows_with_sections() {
    let picker = Picker {
        kind: PickerKind::Model,
        title: "Choose model",
        options: vec![
            PickerOption {
                label: "codex-1".to_string(),
                value: "codex-1".to_string(),
                preview: None,
                section: Some("Codex".to_string()),
            },
            PickerOption {
                label: "codex-2".to_string(),
                value: "codex-2".to_string(),
                preview: None,
                section: None,
            },
            PickerOption {
                label: "claude-1".to_string(),
                value: "claude-1".to_string(),
                preview: None,
                section: Some("Claude".to_string()),
            },
        ],
        selected: 2,
        query: None,
    };
    let lines = picker.styled_lines(80, Color::White, Color::White);
    for (index, line) in picker.option_row_offsets() {
        assert!(
            lines[line]
                .to_string()
                .contains(&picker.options[index].label)
        );
    }
}

#[test]
fn picker_numbers_are_visible_and_select_immediately() {
    let mut picker = Picker::new(
        PickerKind::Effort,
        "Choose effort",
        ["low", "medium", "high"],
        Some("low"),
    );

    let display = picker.display(40);
    assert!(display.contains("1. low"));
    assert!(display.contains("2. medium"));
    assert!(picker.select_number('2'));
    assert_eq!(picker.selected, 1);
    assert!(!picker.select_number('0'));
    assert!(!picker.select_number('9'));
}

#[test]
fn accepted_steer_stays_pending_until_user_message_commit() {
    let message_id = Uuid::new_v4();
    let mut queue = Vec::new();
    update_queued_prompts(
        &mut queue,
        &SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text: "follow up".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Queued,
            delivery: Some(PromptDelivery::Steer),
        },
    );
    assert_eq!(
        queue,
        vec![PendingPromptProjection {
            message_id,
            text: "follow up".to_string(),
            delivery: PromptDelivery::Steer,
        }]
    );

    update_queued_prompts(
        &mut queue,
        &SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text: "follow up".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: Some(PromptDelivery::Steer),
        },
    );
    assert_eq!(queue.len(), 1);
    assert_eq!(queue[0].message_id, message_id);

    update_queued_prompts(
        &mut queue,
        &SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text: "follow up".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: Some(PromptDelivery::Steer),
        },
    );
    assert!(queue.is_empty());
}

#[test]
fn optimistic_idle_submission_is_visible_before_session_persistence() {
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    let mut transcript = Transcript::default();
    let optimistic = SessionEvent::new(
        session_id,
        0,
        SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text: "send this now".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: Some(PromptDelivery::Steer),
        },
    );

    transcript.project_optimistic_message(&optimistic);
    assert!(matches!(
        transcript.order.last(),
        Some(TranscriptEntry::Message {
            actor: EventActor::User,
            text,
            status: MessageStatus::Complete,
            ..
        }) if text == "send this now"
    ));

    let queued = SessionEvent::new(
        session_id,
        1,
        SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text: "send this now".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Queued,
            delivery: Some(PromptDelivery::Steer),
        },
    );
    assert!(transcript.apply(&queued).is_some());
}

#[test]
fn optimistic_idle_submission_immediately_hides_cold_cache_guidance() {
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    let mut transcript = Transcript::default();
    transcript.config = Some(SessionDisplayConfig {
        cwd: PathBuf::from("/workspace"),
        provider: CodingProvider::Codex,
        model: Some("gpt-5.6-sol".to_string()),
        effort: Some("high".to_string()),
        response_language: ResponseLanguage::English,
        fast: false,
        permission_mode: PermissionMode::FullAccess,
    });
    transcript.live_turn_closed = true;
    let at = Utc::now() - chrono::Duration::minutes(31);
    transcript.cache_diagnostics.observe(
        at,
        CacheSignature::new(CodingProvider::Codex, Some("gpt-5.6-sol"), Some("high")),
        CacheUsage {
            input_tokens: 1_000,
            cached_input_tokens: 99_000,
            cache_creation_input_tokens: 0,
            cost_microusd: None,
            cost_basis: "unavailable",
        },
    );
    assert!(
        transcript
            .cache_status(Utc::now())
            .is_some_and(|status| status.warning)
    );

    transcript.project_optimistic_message(&SessionEvent::new(
        session_id,
        0,
        SessionEventKind::Message {
            message_id,
            actor: EventActor::User,
            text: "start immediately".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: Some(PromptDelivery::Steer),
        },
    ));

    assert_eq!(
        transcript.active_turn.as_ref().map(|turn| turn.message_id),
        Some(message_id)
    );
    assert!(!transcript.live_turn_closed);
    assert_eq!(transcript.cache_status(Utc::now()), None);
}

#[test]
fn in_progress_steer_never_materializes_as_a_responding_message() {
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    let message = |sequence, status| {
        SessionEvent::new(
            session_id,
            sequence,
            SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: "follow up".to_string(),
                attachments: Vec::new(),
                status,
                delivery: Some(PromptDelivery::Steer),
            },
        )
    };
    let mut transcript = Transcript::default();

    transcript.apply(&message(1, MessageStatus::Queued));
    transcript.apply(&message(2, MessageStatus::InProgress));
    assert!(transcript.order.is_empty());

    transcript.apply(&message(3, MessageStatus::Complete));
    assert_eq!(transcript.order.len(), 1);
    assert!(matches!(
        &transcript.order[0],
        TranscriptEntry::Message {
            actor: EventActor::User,
            text,
            complete: true,
            ..
        } if text == "follow up"
    ));
}

#[test]
fn active_turn_assistant_segments_preserve_event_order() {
    let session_id = Uuid::new_v4();
    let prompt_id = Uuid::new_v4();
    let assistant_id = Uuid::new_v4();
    let tool_call_id = "tool-1".to_string();
    let mut transcript = Transcript::default();

    transcript.apply(&SessionEvent::new(
        session_id,
        1,
        SessionEventKind::Message {
            message_id: prompt_id,
            actor: EventActor::User,
            text: "investigate this".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: Some(PromptDelivery::Steer),
        },
    ));
    transcript.apply(&SessionEvent::new(
        session_id,
        2,
        SessionEventKind::TurnStarted {
            message_id: prompt_id,
            provider: CodingProvider::Codex,
            model: None,
            effort: None,
            fast: false,
        },
    ));
    for (sequence, status) in [(3, MessageStatus::InProgress), (4, MessageStatus::Complete)] {
        transcript.apply(&SessionEvent::new(
            session_id,
            sequence,
            SessionEventKind::Message {
                message_id: assistant_id,
                actor: EventActor::Assistant,
                text: "I am checking the provider trace first.".to_string(),
                attachments: Vec::new(),
                status,
                delivery: None,
            },
        ));
    }
    transcript.apply(&SessionEvent::new(
        session_id,
        5,
        SessionEventKind::ToolStarted {
            tool_call_id: tool_call_id.clone(),
            name: "shell".to_string(),
            input: serde_json::json!({"command": "trace"}),
            input_ref: None,
        },
    ));

    let tool_index = transcript.tools[&tool_call_id];
    let assistant_index = transcript.messages[&assistant_id];
    assert!(assistant_index < tool_index);
    assert!(matches!(
        &transcript.order[assistant_index],
        TranscriptEntry::Message {
            status: MessageStatus::Complete,
            complete: true,
            ..
        }
    ));

    transcript.apply(&SessionEvent::new(
        session_id,
        6,
        SessionEventKind::ToolCompleted {
            tool_call_id,
            output: "trace complete".to_string(),
            output_ref: None,
            is_error: false,
            input: None,
            input_ref: None,
        },
    ));
    let final_id = Uuid::new_v4();
    transcript.apply(&SessionEvent::new(
        session_id,
        7,
        SessionEventKind::Message {
            message_id: final_id,
            actor: EventActor::Assistant,
            text: "The provider trace is clear now.".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
    ));

    assert!(transcript.tools["tool-1"] < transcript.messages[&final_id]);
}

#[test]
fn active_partial_assistant_message_stays_before_later_tool_activity() {
    let session_id = Uuid::new_v4();
    let prompt_id = Uuid::new_v4();
    let assistant_id = Uuid::new_v4();
    let tool_call_id = "tool-after-partial".to_string();
    let mut transcript = Transcript::default();

    transcript.apply(&SessionEvent::new(
        session_id,
        1,
        SessionEventKind::Message {
            message_id: prompt_id,
            actor: EventActor::User,
            text: "investigate this".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: Some(PromptDelivery::Steer),
        },
    ));
    transcript.apply(&SessionEvent::new(
        session_id,
        2,
        SessionEventKind::TurnStarted {
            message_id: prompt_id,
            provider: CodingProvider::Codex,
            model: None,
            effort: None,
            fast: false,
        },
    ));
    transcript.apply(&SessionEvent::new(
        session_id,
        3,
        SessionEventKind::Message {
            message_id: assistant_id,
            actor: EventActor::Assistant,
            text: "I".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: None,
        },
    ));
    transcript.apply(&SessionEvent::new(
        session_id,
        4,
        SessionEventKind::ToolStarted {
            tool_call_id: tool_call_id.clone(),
            name: "shell".to_string(),
            input: serde_json::json!({"command": "trace"}),
            input_ref: None,
        },
    ));
    transcript.apply(&SessionEvent::new(
        session_id,
        5,
        SessionEventKind::ReasoningDelta {
            text: "checking the trace".to_string(),
        },
    ));

    let assistant_index = transcript.messages[&assistant_id];
    let tool_index = transcript.tools[&tool_call_id];
    let reasoning_index = transcript.active_reasoning.expect("active reasoning row");
    assert!(assistant_index < tool_index);
    assert!(tool_index < reasoning_index);
    assert!(matches!(
        &transcript.order[assistant_index],
        TranscriptEntry::Message {
            status: MessageStatus::InProgress,
            complete: false,
            ..
        }
    ));
}

#[test]
fn running_tool_suppresses_stale_response_spinner() {
    let session_id = Uuid::new_v4();
    let prompt_id = Uuid::new_v4();
    let assistant_id = Uuid::new_v4();
    let mut transcript = Transcript::default();
    transcript.apply(&SessionEvent::new(
        session_id,
        1,
        SessionEventKind::Message {
            message_id: prompt_id,
            actor: EventActor::User,
            text: "run the check".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: Some(PromptDelivery::Steer),
        },
    ));
    transcript.apply(&SessionEvent::new(
        session_id,
        2,
        SessionEventKind::TurnStarted {
            message_id: prompt_id,
            provider: CodingProvider::Codex,
            model: None,
            effort: None,
            fast: false,
        },
    ));
    transcript.apply(&SessionEvent::new(
        session_id,
        3,
        SessionEventKind::ToolStarted {
            tool_call_id: "tool-2".to_string(),
            name: "shell".to_string(),
            input: serde_json::json!({"command": "check"}),
            input_ref: None,
        },
    ));
    transcript.apply(&SessionEvent::new(
        session_id,
        4,
        SessionEventKind::Message {
            message_id: assistant_id,
            actor: EventActor::Assistant,
            text: "I will report back after the check.".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: None,
        },
    ));
    let rendered = transcript
        .lines(100)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(!rendered.contains("responding"), "{rendered}");

    transcript.apply(&SessionEvent::new(
        session_id,
        5,
        SessionEventKind::ToolCompleted {
            tool_call_id: "tool-2".to_string(),
            output: "done".to_string(),
            output_ref: None,
            is_error: false,
            input: None,
            input_ref: None,
        },
    ));
    let rendered = transcript
        .lines(100)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(rendered.contains("responding"), "{rendered}");
}

#[test]
fn terminal_boundary_settles_a_late_assistant_live_snapshot() {
    let session_id = Uuid::new_v4();
    let prompt_id = Uuid::new_v4();
    let assistant_id = Uuid::new_v4();
    let mut transcript = Transcript::default();

    transcript.apply(&SessionEvent::new(
        session_id,
        1,
        SessionEventKind::TurnStarted {
            message_id: prompt_id,
            provider: CodingProvider::Codex,
            model: None,
            effort: None,
            fast: false,
        },
    ));
    transcript.apply(&SessionEvent::new(
        session_id,
        2,
        SessionEventKind::Message {
            message_id: assistant_id,
            actor: EventActor::Assistant,
            text: "partial response".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: None,
        },
    ));
    transcript.apply(&SessionEvent::new(
        session_id,
        3,
        SessionEventKind::TurnCompleted {
            message_id: prompt_id,
            provider_session_id: None,
            final_text: "final response".to_string(),
            error: None,
        },
    ));

    assert!(transcript.order.iter().all(|entry| !matches!(
        entry,
        TranscriptEntry::Message {
            actor: EventActor::Assistant,
            status: MessageStatus::InProgress,
            ..
        }
    )));
    assert!(transcript.order.iter().any(|entry| matches!(
        entry,
        TranscriptEntry::Message {
            actor: EventActor::Assistant,
            status: MessageStatus::Complete,
            text,
            ..
        } if text == "partial response"
    )));

    transcript.apply(&SessionEvent::new(
        session_id,
        4,
        SessionEventKind::Message {
            message_id: assistant_id,
            actor: EventActor::Assistant,
            text: "stale live response".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: None,
        },
    ));
    assert!(transcript.order.iter().all(|entry| !matches!(
        entry,
        TranscriptEntry::Message {
            actor: EventActor::Assistant,
            status: MessageStatus::InProgress,
            ..
        }
    )));
}

#[test]
fn resume_mid_turn_accepts_an_assistant_live_snapshot_without_turn_started() {
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    let mut transcript = Transcript::default();
    transcript.seed_session_state(&SessionState {
        status: Some(SessionStatus::Running),
        ..SessionState::default()
    });

    transcript.apply(&SessionEvent::new(
        session_id,
        1,
        SessionEventKind::Message {
            message_id,
            actor: EventActor::Assistant,
            text: "resumed response".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::InProgress,
            delivery: None,
        },
    ));

    assert!(matches!(
        transcript.order.first(),
        Some(TranscriptEntry::Message {
            actor: EventActor::Assistant,
            status: MessageStatus::InProgress,
            text,
            ..
        }) if text == "resumed response"
    ));
}

#[test]
fn internal_team_delivery_never_renders_or_enters_user_prompt_history() {
    let session_id = Uuid::new_v4();
    let message_id = Uuid::new_v4();
    let text = "Team message from /root/worker:\n\nchild result".to_string();
    let current = SessionEvent::new(
        session_id,
        1,
        SessionEventKind::Message {
            message_id,
            actor: EventActor::System,
            text: text.clone(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: Some(PromptDelivery::Queue),
        },
    );

    let mut transcript = Transcript::default();
    transcript.apply(&current);
    assert!(transcript.order.is_empty());

    let mut composer = Composer::default();
    composer.seed_session_events(std::slice::from_ref(&current));
    assert!(composer.history.is_empty());

    let mut pending = Vec::new();
    update_queued_prompts(
        &mut pending,
        &SessionEventKind::Message {
            message_id,
            actor: EventActor::System,
            text,
            attachments: Vec::new(),
            status: MessageStatus::Queued,
            delivery: Some(PromptDelivery::Queue),
        },
    );
    assert!(pending.is_empty());
}

#[test]
fn historical_pages_require_explicit_upward_navigation() {
    assert!(!should_load_history_page(false, 100, 100, 24));
    assert!(!should_load_history_page(false, 0, 0, 24));
    assert!(!should_load_history_page(true, 0, 1_000, 24));
    assert!(should_load_history_page(true, 960, 1_000, 24));
    assert!(should_load_history_page(true, 0, 0, 24));
}

#[test]
fn historical_page_loading_has_an_explicit_animated_label() {
    let rendered = history_loading_line().to_string();
    assert!(rendered.contains("Loading thread history…"));
    assert!(
        ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧"]
            .iter()
            .any(|frame| rendered.contains(frame))
    );
}

#[test]
fn admitted_queued_prompt_is_inserted_at_its_real_transcript_boundary() {
    let session_id = Uuid::new_v4();
    let queued_id = Uuid::new_v4();
    let assistant_id = Uuid::new_v4();
    let mut transcript = Transcript::default();
    transcript.apply(&SessionEvent::new(
        session_id,
        1,
        SessionEventKind::Message {
            message_id: queued_id,
            actor: EventActor::User,
            text: "queued follow-up".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Queued,
            delivery: Some(PromptDelivery::Steer),
        },
    ));
    assert!(transcript.order.is_empty());
    assert!(!transcript.messages.contains_key(&queued_id));

    transcript.apply(&SessionEvent::new(
        session_id,
        2,
        SessionEventKind::Message {
            message_id: assistant_id,
            actor: EventActor::Assistant,
            text: "current turn output".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
    ));
    transcript.apply(&SessionEvent::new(
        session_id,
        3,
        SessionEventKind::Message {
            message_id: queued_id,
            actor: EventActor::User,
            text: "queued follow-up".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: Some(PromptDelivery::Steer),
        },
    ));

    let actors = transcript
        .order
        .iter()
        .filter_map(|entry| match entry {
            TranscriptEntry::Message { actor, .. } => Some(*actor),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(actors, [EventActor::Assistant, EventActor::User]);
    assert_eq!(transcript.messages[&queued_id], 1);
}

#[test]
fn committed_steer_does_not_hide_a_separate_next_turn_queue() {
    let queued_id = Uuid::new_v4();
    let steer_id = Uuid::new_v4();
    let mut queue = Vec::new();
    for (message_id, text, status, delivery) in [
        (
            queued_id,
            "run next",
            MessageStatus::Queued,
            PromptDelivery::Queue,
        ),
        (
            steer_id,
            "steer now",
            MessageStatus::Queued,
            PromptDelivery::Steer,
        ),
        (
            steer_id,
            "steer now",
            MessageStatus::Complete,
            PromptDelivery::Steer,
        ),
    ] {
        update_queued_prompts(
            &mut queue,
            &SessionEventKind::Message {
                message_id,
                actor: EventActor::User,
                text: text.to_string(),
                attachments: Vec::new(),
                status,
                delivery: Some(delivery),
            },
        );
    }

    assert_eq!(
        queue,
        vec![PendingPromptProjection {
            message_id: queued_id,
            text: "run next".to_string(),
            delivery: PromptDelivery::Queue,
        }]
    );
}

#[test]
fn queue_projection_preserves_fifo_and_discards_bypassed_stale_entries() {
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    let queued = |message_id: Uuid, text: &str| SessionEventKind::Message {
        message_id,
        actor: EventActor::User,
        text: text.to_string(),
        attachments: Vec::new(),
        status: MessageStatus::Queued,
        delivery: Some(PromptDelivery::Queue),
    };
    let admitted = |message_id: Uuid, text: &str| SessionEventKind::Message {
        message_id,
        actor: EventActor::User,
        text: text.to_string(),
        attachments: Vec::new(),
        status: MessageStatus::Complete,
        delivery: Some(PromptDelivery::Queue),
    };

    let mut queue = Vec::new();
    update_queued_prompts(&mut queue, &queued(first, "first"));
    update_queued_prompts(&mut queue, &queued(second, "second"));
    update_queued_prompts(&mut queue, &admitted(first, "first"));
    assert_eq!(
        queue,
        vec![PendingPromptProjection {
            message_id: second,
            text: "second".to_string(),
            delivery: PromptDelivery::Queue,
        }]
    );

    update_queued_prompts(&mut queue, &queued(first, "stale first"));
    update_queued_prompts(&mut queue, &admitted(first, "stale first"));
    assert!(queue.is_empty());

    update_queued_prompts(&mut queue, &queued(first, "bypassed"));
    update_queued_prompts(&mut queue, &admitted(second, "later prompt"));
    assert!(queue.is_empty());
}

#[test]
fn one_queued_prompt_allocates_a_content_row_below_its_border() {
    let prompts = [PendingPromptProjection {
        message_id: Uuid::new_v4(),
        text: "visible follow-up".to_string(),
        delivery: PromptDelivery::Queue,
    }];
    let six = (0..6).map(|_| prompts[0].clone()).collect::<Vec<_>>();
    let seven = (0..7).map(|_| prompts[0].clone()).collect::<Vec<_>>();
    assert_eq!(queued_prompt_panel_height(&[], 60), 0);
    assert_eq!(queued_prompt_panel_height(&prompts, 60), 3);
    assert_eq!(queued_prompt_panel_height(&six, 60), 8);
    assert_eq!(queued_prompt_panel_height(&seven, 60), 9);

    let area = Rect::new(0, 0, 60, queued_prompt_panel_height(&prompts, 60));
    let mut buffer = ratatui::buffer::Buffer::empty(area);
    let widget = Paragraph::new(queued_prompt_lines(&prompts, area.width, None)).block(
        Block::default()
            .borders(Borders::TOP | Borders::LEFT)
            .border_style(Style::default().fg(Color::DarkGray))
            .title(" Pending Input · 1 "),
    );
    ratatui::widgets::Widget::render(widget, area, &mut buffer);
    let content = (0..area.width)
        .map(|x| buffer[(x, 1)].symbol())
        .collect::<String>();
    let hint = (0..area.width)
        .map(|x| buffer[(x, 2)].symbol())
        .collect::<String>();
    assert!(content.contains("Next"));
    assert!(content.contains("visible follow-up"));
    assert!(hint.contains("↑ edit / recall pending"));
}

#[test]
fn pending_input_wraps_the_entire_prompt_instead_of_compacting_it() {
    let text = "a very long pending prompt that continues beyond the panel width";
    let prompts = [PendingPromptProjection {
        message_id: Uuid::new_v4(),
        text: text.to_string(),
        delivery: PromptDelivery::Queue,
    }];
    let lines = queued_prompt_lines(&prompts, 44, None);
    let rendered = lines
        .iter()
        .map(Line::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        rendered.contains("beyond") && rendered.contains("panel") && rendered.contains("width")
    );
    assert!(!rendered.contains('…'));
}

#[test]
fn pending_steer_ui_uses_the_shared_next_label_and_interrupt_action() {
    let prompts = [PendingPromptProjection {
        message_id: Uuid::new_v4(),
        text: "focus on the failing test".to_string(),
        delivery: PromptDelivery::Steer,
    }];
    let rendered = queued_prompt_lines(&prompts, 80, None)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");

    assert!(rendered.contains("Next"));
    assert!(!rendered.contains("NEXT TOOL"));
    assert!(!rendered.contains("NEXT TURN"));
    assert!(rendered.contains("focus on the failing test"));
    assert!(rendered.contains("esc interrupt + send now"));
    // ↑ asks the session to recall the steer; it decides whether the provider
    // has acknowledged it yet.
    assert!(rendered.contains("recall pending"));
}

#[test]
fn recovered_idle_session_stops_orphaned_tool_spinner() {
    let session_id = Uuid::new_v4();
    let mut transcript = Transcript::default();
    transcript.apply(&SessionEvent::new(
        session_id,
        1,
        SessionEventKind::ToolStarted {
            tool_call_id: "orphaned".to_string(),
            name: "read_file".to_string(),
            input: serde_json::json!({"path": "src/lib.rs"}),
            input_ref: None,
        },
    ));
    assert!(transcript.tool_spinner_cache_tick().is_some());

    transcript.reconcile_session_status(&SessionState {
        status: Some(SessionStatus::Ready),
        activity_at: Some(Utc::now()),
        ..SessionState::default()
    });

    assert!(transcript.tool_spinner_cache_tick().is_none());
    assert!(matches!(
        transcript.order.first(),
        Some(TranscriptEntry::Tool { complete: true, .. })
    ));
}

#[test]
fn up_recall_targets_all_queued_prompts_only_for_an_empty_composer() {
    let queued = PendingPromptProjection {
        message_id: Uuid::new_v4(),
        text: "edit me".to_string(),
        delivery: PromptDelivery::Queue,
    };
    let steer = PendingPromptProjection {
        message_id: Uuid::new_v4(),
        text: "already submitted".to_string(),
        delivery: PromptDelivery::Steer,
    };

    assert!(has_recallable_queued_prompts(
        "",
        std::slice::from_ref(&queued)
    ));
    assert!(has_recallable_queued_prompts(
        "",
        &[queued.clone(), steer.clone()]
    ));
    assert!(!has_recallable_queued_prompts("", &[steer]));
    assert!(!has_recallable_queued_prompts(
        "draft in progress",
        &[PendingPromptProjection {
            message_id: Uuid::new_v4(),
            text: "queued".to_string(),
            delivery: PromptDelivery::Queue,
        }]
    ));

    let newer = PendingPromptProjection {
        message_id: Uuid::new_v4(),
        text: "newer".to_string(),
        delivery: PromptDelivery::Queue,
    };
    assert!(has_recallable_queued_prompts("", &[queued, newer]));
}

#[test]
fn up_does_not_fake_recall_an_already_submitted_steer() {
    let steer = PendingPromptProjection {
        message_id: Uuid::new_v4(),
        text: "already submitted".to_string(),
        delivery: PromptDelivery::Steer,
    };

    assert!(!has_recallable_queued_prompts(
        "",
        std::slice::from_ref(&steer)
    ));
    assert!(pending_steer_blocks_history_recall(
        "",
        std::slice::from_ref(&steer)
    ));
    assert!(!pending_steer_blocks_history_recall(
        "draft in progress",
        &[steer]
    ));
}

#[test]
fn copied_terminal_regions_drop_screen_padding_and_right_gutters() {
    let pasted = "message                                      ▊\n\
                         20:03  ✓ Edit  file.rs                      ▊\n\
                             10 │ + let value = true;                ▊\n\
                                                                  ▊";

    assert_eq!(
        normalize_terminal_capture_paste(pasted),
        "message\n20:03  ✓ Edit  file.rs\n10 │ + let value = true;\n"
    );
    assert_eq!(
        normalize_terminal_capture_paste("    fn preserved_code() {\n        work();\n    }"),
        "    fn preserved_code() {\n        work();\n    }"
    );
}

#[test]
fn context_percentage_matches_codex_compaction_headroom() {
    assert_eq!(context_remaining_percent(12_000, 258_400), 100);
    assert_eq!(context_remaining_percent(135_200, 258_400), 50);
    assert_eq!(context_remaining_percent(258_400, 258_400), 0);
}

#[test]
fn active_terminal_title_identifies_borg_and_the_first_prompt() {
    let title = terminal_title(
        SessionStatus::Running,
        Some("  polish   the terminal\ninteraction  "),
    );

    assert!(title.contains("Borg CLI - polish the terminal interaction..."));
    assert!(
        title
            .chars()
            .next()
            .is_some_and(|glyph| "⠋⠙⠹⠸⠼⠴⠦⠧".contains(glyph))
    );
    assert_eq!(
        terminal_title(SessionStatus::Ready, None),
        "Borg CLI".to_string()
    );
}

#[test]
fn borging_roll_selects_exactly_one_percent_of_uniform_run_ids() {
    assert_eq!(
        (0..100)
            .filter(|value| borging_for_run(Uuid::from_u128(*value)))
            .count(),
        1
    );
}

#[test]
fn splash_logo_randomizes_glitches_and_then_settles() {
    assert_eq!(splash_version(), format!("v{}", env!("CARGO_PKG_VERSION")));
    assert_eq!(splash_alpha_line().to_string(), "αlphα");
    assert_eq!(
        splash_logo_line(Duration::from_millis(1_320), 7).to_string(),
        "B O R G"
    );
    let glitch = splash_logo_line(Duration::ZERO, 7).to_string();
    assert_ne!(glitch, "B O R G");
    assert_eq!(
        UnicodeWidthStr::width(glitch.as_str()),
        UnicodeWidthStr::width("B O R G")
    );
    assert_ne!(
        splash_logo_line(Duration::ZERO, 7).to_string(),
        splash_logo_line(Duration::ZERO, 8).to_string()
    );
}

#[test]
fn provider_compaction_events_become_status_cards() {
    let session_id = Uuid::new_v4();
    let mut transcript = Transcript::default();
    transcript.apply(&SessionEvent::new(
        session_id,
        1,
        SessionEventKind::ProviderEvent {
            provider: CodingProvider::Codex,
            kind: "context_compaction".to_string(),
            payload: serde_json::json!({
                "summary": "Earlier conversation was compacted"
            }),
        },
    ));

    assert!(matches!(
        transcript.order.last(),
        Some(TranscriptEntry::Compaction { summary, .. })
            if summary == "Earlier conversation was compacted"
    ));
    let rendered = transcript
        .lines(100)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(rendered.contains("Earlier conversation was compacted"));
    assert!(!rendered.contains("Starting context compaction"));
    assert!(!rendered.contains("being condensed so Borg can keep working"));
}

#[test]
fn automatic_compaction_event_reports_work_in_progress() {
    let session_id = Uuid::new_v4();
    let mut transcript = Transcript::default();
    transcript.apply(&SessionEvent::new(
        session_id,
        1,
        SessionEventKind::ProviderEvent {
            provider: CodingProvider::Codex,
            kind: "context_compaction".to_string(),
            payload: serde_json::json!({}),
        },
    ));

    assert!(matches!(
        transcript.order.last(),
        Some(TranscriptEntry::Compaction { summary, .. })
            if summary == "Compacting context…"
    ));
}

#[test]
fn adjacent_provider_notifications_render_one_compaction_card() {
    let session_id = Uuid::new_v4();
    let mut transcript = Transcript::default();
    for sequence in 1..=3 {
        transcript.apply(&SessionEvent::new(
            session_id,
            sequence,
            SessionEventKind::ProviderEvent {
                provider: CodingProvider::Codex,
                kind: "context_compaction".to_string(),
                payload: serde_json::json!({
                    "summary": "Conversation context condensed"
                }),
            },
        ));
    }

    assert_eq!(
        transcript
            .order
            .iter()
            .filter(|entry| matches!(entry, TranscriptEntry::Compaction { .. }))
            .count(),
        1
    );
}

#[test]
fn low_context_status_announces_imminent_compaction() {
    let transcript = Transcript {
        context_remaining_percent: 20,
        ..Default::default()
    };

    let (status, imminent) = transcript.context_status();

    assert_eq!(status, "compaction imminent (20% left)");
    assert!(imminent);
}

#[test]
fn stale_session_state_cannot_reseed_newer_root_projection_fields() {
    let stale_ready = SessionState {
        latest_sequence: 4,
        status: Some(SessionStatus::Ready),
        pending_approval_id: Some("stale-approval".to_string()),
        ..SessionState::default()
    };

    assert!(session_state_snapshot_is_stale(5, &stale_ready));
    assert!(!session_state_snapshot_is_stale(4, &stale_ready));
    assert!(!session_state_snapshot_is_stale(3, &stale_ready));
}

#[test]
fn projected_session_state_restores_status_config_outside_the_history_tail() {
    let separator = std::path::MAIN_SEPARATOR;
    let mut transcript = Transcript::default();
    transcript.seed_session_state(&SessionState {
        configuration: Some(borg_remote::SessionConfiguration {
            cwd: PathBuf::from("/workspace/borg"),
            provider: CodingProvider::Codex,
            model: Some("gpt-5.6-sol".to_string()),
            effort: Some("medium".to_string()),
            fast: false,
            response_language: ResponseLanguage::Auto,
            permission_mode: PermissionMode::FullAccess,
        }),
        usage: borg_remote::SessionUsage {
            context_tokens: Some(69_768),
            context_window_tokens: Some(258_400),
            ..Default::default()
        },
        ..Default::default()
    });

    assert_eq!(
        transcript.config_statuses(),
        (
            Some("gpt-5.6-sol".to_string()),
            Some("medium".to_string()),
            None,
            Some("full access".to_string()),
            format!("{separator}w{separator}borg")
        )
    );
    assert_eq!(transcript.context_remaining_percent, 77);
}

#[test]
fn fast_mode_gets_its_own_status_segment_only_when_enabled() {
    let mut transcript = Transcript::default();
    transcript.seed_session_state(&SessionState {
        configuration: Some(borg_remote::SessionConfiguration {
            cwd: PathBuf::from("/workspace/borg"),
            provider: CodingProvider::Codex,
            model: Some("gpt-5.6-sol".to_string()),
            effort: Some("high".to_string()),
            fast: true,
            response_language: ResponseLanguage::Auto,
            permission_mode: PermissionMode::FullAccess,
        }),
        ..Default::default()
    });

    let (model, effort, fast, _, _) = transcript.config_statuses();
    assert_eq!(model.as_deref(), Some("gpt-5.6-sol"));
    assert_eq!(effort.as_deref(), Some("high"));
    assert_eq!(fast.as_deref(), Some("fast"));
}

#[test]
fn transcript_separates_labeled_groups_from_header_and_tool_activity() {
    let mut transcript = Transcript::default();
    transcript.order.push(TranscriptEntry::Message {
        actor: EventActor::User,
        text: "request".to_string(),
        attachments: Vec::new(),
        model: None,
        effort: None,
        time: "12:00".to_string(),
        status: MessageStatus::Complete,
        complete: true,
    });
    transcript.order.push(TranscriptEntry::Tool {
        source_name: "command_execution".to_string(),
        name: "command_execution".to_string(),
        detail: "done".to_string(),
        code_view: None,
        output_view: None,
        payload_refs: Vec::new(),
        time: "12:01".to_string(),
        started_at: Utc::now(),
        completed_at: Some(Utc::now()),
        complete: true,
        error: false,
        user_interrupted: false,
        backgrounded: false,
        expanded: false,
    });
    transcript.order.push(TranscriptEntry::Message {
        actor: EventActor::Assistant,
        text: "answer".to_string(),
        attachments: Vec::new(),
        model: Some("gpt-5.6-sol".to_string()),
        effort: Some("xhigh".to_string()),
        time: "12:02".to_string(),
        status: MessageStatus::Complete,
        complete: true,
    });
    transcript.order.push(TranscriptEntry::Tool {
        source_name: "command_execution".to_string(),
        name: "command_execution".to_string(),
        detail: "done".to_string(),
        code_view: None,
        output_view: None,
        payload_refs: Vec::new(),
        time: "12:03".to_string(),
        started_at: Utc::now(),
        completed_at: Some(Utc::now()),
        complete: true,
        error: false,
        user_interrupted: false,
        backgrounded: false,
        expanded: false,
    });
    transcript.order.push(TranscriptEntry::Plan {
        items: vec![PlanItem {
            id: Uuid::new_v4(),
            content: "Verify the result".to_string(),
            status: PlanItemStatus::InProgress,
        }],
        time: "12:04".to_string(),
        expanded: false,
    });
    transcript.user_label = "shulgin".to_string();
    transcript.assistant_label = "borg".to_string();

    let lines = transcript.lines(80);
    assert!(lines.first().is_some_and(|line| line.spans.is_empty()));
    let user_label = lines
        .iter()
        .flat_map(|line| line.spans.iter())
        .find(|span| span.content.contains("shulgin"))
        .expect("user label");
    assert_eq!(user_label.style.fg, Some(USER_LABEL_BLUE));
    let user_header = lines
        .iter()
        .position(|line| {
            line.spans
                .iter()
                .any(|span| span.content.contains("shulgin"))
        })
        .expect("user header");
    assert_eq!(
        lines[user_header - 1]
            .spans
            .last()
            .and_then(|span| span.style.bg),
        Some(MESSAGE_BG)
    );
    let user_message = lines
        .iter()
        .flat_map(|line| line.spans.iter())
        .find(|span| span.content.contains("request"))
        .expect("user message");
    assert_eq!(user_message.style.fg, Some(USER_TEXT));
    let assistant_header = lines
        .iter()
        .position(|line| line.spans.iter().any(|span| span.content.contains("borg")))
        .expect("assistant header");
    let assistant_header_spans = &lines[assistant_header].spans;
    assert_eq!(assistant_header_spans[0].content, "  ▌ borg");
    assert_eq!(assistant_header_spans[1].content, "  gpt-5.6-sol xhigh");
    assert_eq!(assistant_header_spans[1].style.fg, Some(Color::DarkGray));
    assert_eq!(assistant_header_spans[2].content, "  12:02");
    assert!(lines[assistant_header - 1].to_string().trim().is_empty());
    assert_eq!(
        lines[assistant_header - 1]
            .spans
            .last()
            .and_then(|span| span.style.bg),
        Some(MESSAGE_BG)
    );
    let plan_header = lines
        .iter()
        .position(|line| line.spans.iter().any(|span| span.content.contains("Plan")))
        .expect("plan header");
    assert!(lines[plan_header - 1].spans.is_empty());
}

#[test]
fn timestamps_add_the_date_only_when_it_is_not_today() {
    let today = NaiveDate::from_ymd_opt(2026, 7, 26).unwrap();

    assert_eq!(display_local_time("2026-07-26 12:02", today), "12:02");
    assert_eq!(
        display_local_time("2026-07-25 23:58", today),
        "2026-07-25 23:58"
    );
}

#[test]
fn active_session_status_uses_vibrant_peach() {
    assert_eq!(
        session_status_color(SessionStatus::Running),
        RUNNING_STATUS_PEACH
    );
    assert_eq!(
        session_status_color(SessionStatus::Starting),
        RUNNING_STATUS_PEACH
    );
    assert_eq!(session_status_color(SessionStatus::Failed), Color::LightRed);
}

#[test]
fn status_hover_underlines_the_label_but_not_the_activity_glyph() {
    let spans = status_control_spans("⠋", "running", RUNNING_STATUS_PEACH, true, Some("2m"));

    assert_eq!(spans[0].content.as_ref(), " ⠋ ");
    assert!(!spans[0].style.add_modifier.contains(Modifier::UNDERLINED));
    assert_eq!(spans[1].content.as_ref(), "running");
    assert!(spans[1].style.add_modifier.contains(Modifier::UNDERLINED));
    assert_eq!(spans[2].content.as_ref(), " 2m");
    assert!(!spans[2].style.add_modifier.contains(Modifier::UNDERLINED));
}

#[test]
fn ready_status_does_not_register_an_actionable_hitbox() {
    let footer = Rect::new(4, 20, 80, 1);

    assert_eq!(
        status_control_hit_area(SessionStatus::Ready, footer, 3, 12),
        None
    );
    assert_eq!(
        status_control_hit_area(SessionStatus::Running, footer, 3, 12),
        Some(Rect::new(7, 20, 12, 1))
    );
}

#[test]
fn open_overlays_suppress_background_hover_hit_testing() {
    assert!(!overlay_suppresses_background_hover(false, false, false));
    assert!(overlay_suppresses_background_hover(true, false, false));
    assert!(overlay_suppresses_background_hover(false, true, false));
    assert!(overlay_suppresses_background_hover(false, false, true));
}

#[test]
fn wheel_distance_advances_in_bounded_frames_and_stops_at_boundaries() {
    let mut motion = ScrollMotion::default();
    motion.push(MAX_PENDING_WHEEL_SCROLL_LINES);
    assert_eq!(
        motion.advance(0, 500),
        MAX_WHEEL_SCROLL_LINES_PER_FRAME as usize
    );
    assert_eq!(
        motion.remaining_lines,
        MAX_PENDING_WHEEL_SCROLL_LINES - MAX_WHEEL_SCROLL_LINES_PER_FRAME
    );

    motion.remaining_lines = -24;
    assert_eq!(motion.advance(40, 500), 37);
    assert_eq!(motion.remaining_lines, -21);
    motion.remaining_lines = 40;
    assert_eq!(motion.advance(496, 500), 500);
    assert!(!motion.is_active());
    motion.remaining_lines = -40;
    assert_eq!(motion.advance(3, 500), 0);
    assert!(!motion.is_active());

    let mut scroll = 0;
    let mut motion = ScrollMotion::default();
    let event_lines = wheel_scroll_lines(30);
    motion.push(event_lines);
    let mut frames = 0;
    while motion.is_active() {
        scroll = motion.advance(scroll, 500);
        frames += 1;
    }
    assert_eq!(scroll, event_lines as usize);
    assert_eq!(frames, event_lines as usize);
}

#[test]
fn nested_wheel_motion_applies_a_coalesced_gesture_in_one_render_frame() {
    let mut scroll = 0;
    let mut motion = ScrollMotion::default();
    let event_lines = wheel_scroll_lines(30);
    motion.push(event_lines);
    let mut frames = 0;
    while motion.is_active() {
        scroll = motion.advance_immediately(scroll, 500);
        frames += 1;
    }

    assert_eq!(scroll, event_lines as usize);
    assert_eq!(frames, 1);
}

#[test]
fn wheel_distance_scales_with_the_target_viewport_height() {
    assert_eq!(wheel_scroll_lines(1), 1);
    assert_eq!(wheel_scroll_lines(6), 1);
    assert_eq!(wheel_scroll_lines(12), 2);
    assert_eq!(wheel_scroll_lines(18), 3);
    assert_eq!(wheel_scroll_lines(30), 5);
    assert_eq!(wheel_scroll_lines(48), 8);
    assert_eq!(wheel_scroll_lines(72), 12);
    assert_eq!(wheel_scroll_lines(120), 12);
}

#[test]
fn coalesced_wheel_bursts_preserve_viewport_scaled_distance() {
    let repetitions = 3;

    assert_eq!(wheel_scroll_distance(6, repetitions), 3);
    assert_eq!(wheel_scroll_distance(12, repetitions), 6);
    assert_eq!(wheel_scroll_distance(18, repetitions), 9);
}

#[test]
fn nested_wheel_distance_eases_in_quadratically_with_terminal_height() {
    assert_eq!(nested_wheel_scroll_lines(1), 1);
    assert_eq!(nested_wheel_scroll_lines(36), 1);
    assert_eq!(nested_wheel_scroll_lines(48), 2);
    assert_eq!(nested_wheel_scroll_lines(54), 4);
    assert_eq!(nested_wheel_scroll_lines(60), 6);
    assert_eq!(nested_wheel_scroll_lines(66), 9);
    assert_eq!(nested_wheel_scroll_lines(72), 12);
    assert_eq!(nested_wheel_scroll_lines(120), 12);
}

#[test]
fn coalesced_nested_wheel_bursts_preserve_height_scaled_distance() {
    let repetitions = 3;

    assert_eq!(nested_wheel_scroll_distance(36, repetitions), 3);
    assert_eq!(nested_wheel_scroll_distance(54, repetitions), 12);
    assert_eq!(nested_wheel_scroll_distance(72, repetitions), 36);
}

#[test]
fn long_tool_runs_show_eight_lines_and_scroll_independently() {
    let mut transcript = Transcript::default();
    for index in 0..20 {
        transcript.order.push(TranscriptEntry::Tool {
            source_name: "Run".to_string(),
            name: "Run".to_string(),
            detail: format!("call-{index}"),
            code_view: None,
            output_view: None,
            payload_refs: Vec::new(),
            time: "12:00".to_string(),
            started_at: Utc::now(),
            completed_at: Some(Utc::now()),
            complete: true,
            error: false,
            user_interrupted: false,
            backgrounded: false,
            expanded: false,
        });
    }

    let rendered = transcript
        .lines(100)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(!rendered.contains("call-11"));
    assert!(rendered.contains("call-12"));
    assert!(rendered.contains("call-19"));
    assert!(rendered.contains("actions · 20 · click to expand · ↑ scroll"));
    assert!(!rendered.contains("scroll for older/newer"));

    transcript.scroll_tool_run(0, 12, -3);
    let rendered = transcript
        .lines(100)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(rendered.contains("call-9"));
    assert!(rendered.contains("call-16"));
    assert!(!rendered.contains("call-17"));
}

#[test]
fn expanded_tool_run_shows_every_action_and_collapses_again() {
    let mut transcript = Transcript::default();
    for index in 0..20 {
        transcript.order.push(TranscriptEntry::Tool {
            source_name: "Run".to_string(),
            name: "Run".to_string(),
            detail: format!("call-{index}"),
            code_view: None,
            output_view: None,
            payload_refs: Vec::new(),
            time: "12:00".to_string(),
            started_at: Utc::now(),
            completed_at: Some(Utc::now()),
            complete: true,
            error: false,
            user_interrupted: false,
            backgrounded: false,
            expanded: false,
        });
    }
    let render = |transcript: &Transcript| {
        transcript
            .lines(100)
            .into_iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
            .join("\n")
    };

    assert!(transcript.toggle_tool_run_expansion(0));
    let expanded = render(&transcript);
    assert!(expanded.contains("call-0"), "{expanded}");
    assert!(expanded.contains("call-19"), "{expanded}");
    assert!(expanded.contains("actions · 20"), "{expanded}");
    assert!(!expanded.contains("↑ more"), "{expanded}");
    assert!(!expanded.contains("↓ more"), "{expanded}");

    assert!(!transcript.toggle_tool_run_expansion(0));
    let collapsed = render(&transcript);
    assert!(!collapsed.contains("call-11"), "{collapsed}");
    assert!(collapsed.contains("call-12"), "{collapsed}");
    assert!(
        collapsed.contains("actions · 20 · click to expand · ↑ scroll"),
        "{collapsed}"
    );
}

#[test]
fn sticky_tool_run_header_row_covers_only_overflowing_boxes() {
    let rows = vec![(0, 2, 10, 0), (12, 14, 30, 4)];

    assert_eq!(sticky_tool_run_header_row(&rows, 0), None);
    assert_eq!(sticky_tool_run_header_row(&rows, 2), None);
    assert_eq!(sticky_tool_run_header_row(&rows, 3), Some((0, 2)));
    assert_eq!(sticky_tool_run_header_row(&rows, 9), Some((0, 2)));
    assert_eq!(sticky_tool_run_header_row(&rows, 10), None);
    assert_eq!(sticky_tool_run_header_row(&rows, 20), Some((12, 14)));
    assert_eq!(sticky_tool_run_header_row(&rows, 30), None);
}

#[test]
fn agent_lifecycle_rows_keep_one_continuous_actions_accordion() {
    let mut transcript = Transcript::default();
    let tool = |index| TranscriptEntry::Tool {
        source_name: "Run".to_string(),
        name: "Run".to_string(),
        detail: format!("call-{index}"),
        code_view: None,
        output_view: None,
        payload_refs: Vec::new(),
        time: "19:38".to_string(),
        started_at: Utc::now(),
        completed_at: Some(Utc::now()),
        complete: true,
        error: false,
        user_interrupted: false,
        backgrounded: false,
        expanded: false,
    };
    for index in 0..4 {
        transcript.order.push(tool(index));
    }
    transcript.order.push(TranscriptEntry::Activity {
        text: "agent · /root/v391_scaling_audit · started".to_string(),
        time: "19:38".to_string(),
    });
    for index in 4..10 {
        transcript.order.push(tool(index));
    }

    let windows = transcript.tool_run_windows();
    assert!(windows.iter().all(Option::is_some));
    assert_eq!(windows[0].unwrap().total, 11);
    assert_eq!(windows[10].unwrap().start, 0);

    let rendered = transcript
        .lines(100)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert_eq!(rendered.matches("┌─ actions").count(), 1);
    assert!(rendered.contains("actions · 11"));
    assert!(
        rendered.contains("│ 19:38  agent · /root/v391_scaling_audit · started"),
        "{rendered}"
    );
}

#[test]
fn tool_run_scroll_only_consumes_wheel_events_while_it_can_move() {
    let mut transcript = Transcript::default();
    for index in 0..20 {
        transcript.order.push(TranscriptEntry::Tool {
            source_name: "Run".to_string(),
            name: "Run".to_string(),
            detail: format!("call-{index}"),
            code_view: None,
            output_view: None,
            payload_refs: Vec::new(),
            time: "12:00".to_string(),
            started_at: Utc::now(),
            completed_at: Some(Utc::now()),
            complete: true,
            error: false,
            user_interrupted: false,
            backgrounded: false,
            expanded: false,
        });
    }

    assert!(!transcript.scroll_tool_run(0, 12, 3));
    assert!(transcript.scroll_tool_run(0, 12, -3));
    assert!(transcript.scroll_tool_run(0, 12, 3));
    assert!(!transcript.scroll_tool_run(0, 12, 3));
}

#[test]
fn action_viewport_uses_up_to_one_third_of_the_terminal() {
    assert_eq!(tool_run_viewport_height(66) + TOOL_RUN_CHROME_HEIGHT, 22);
    assert_eq!(tool_run_viewport_height(20) + TOOL_RUN_CHROME_HEIGHT, 8);
    assert_eq!(tool_run_viewport_height(200) + TOOL_RUN_CHROME_HEIGHT, 32);
}

#[test]
fn nested_tool_scroll_keeps_momentum_out_of_the_transcript() {
    let started_at = Instant::now();
    let mut capture = None;

    assert!(nested_scroll_consumed(
        &mut capture,
        4,
        -1,
        true,
        started_at,
    ));
    assert!(nested_scroll_consumed(
        &mut capture,
        4,
        -1,
        false,
        started_at + Duration::from_millis(50),
    ));
    assert!(!nested_scroll_consumed(
        &mut capture,
        4,
        -1,
        false,
        started_at
            + Duration::from_millis(50)
            + NESTED_SCROLL_GESTURE_GAP
            + Duration::from_millis(1),
    ));
}

#[test]
fn transcript_scroll_anchor_tracks_content_growth_and_collapse() {
    assert_eq!(preserve_scroll_anchor(0, 20, 24), 4);
    assert_eq!(preserve_scroll_anchor(7, 20, 24), 11);
    assert_eq!(preserve_scroll_anchor(11, 24, 20), 7);
    assert_eq!(preserve_scroll_anchor(2, 24, 20), 0);
}

fn tall_expanded_diff_transcript() -> Transcript {
    let mut transcript = Transcript::default();
    for index in 0..20 {
        transcript.order.push(TranscriptEntry::Activity {
            text: format!("before tool {index}"),
            time: "12:00".to_string(),
        });
    }
    transcript.order.push(TranscriptEntry::Tool {
        source_name: "Edit".to_string(),
        name: "Edit".to_string(),
        detail: "very-tall.rs".to_string(),
        code_view: Some((
            "diff:rs".to_string(),
            (0..240)
                .map(|line| format!("+changed line {line}"))
                .collect::<Vec<_>>()
                .join("\n"),
        )),
        output_view: None,
        payload_refs: Vec::new(),
        time: "12:00".to_string(),
        started_at: Utc::now(),
        completed_at: Some(Utc::now()),
        complete: true,
        error: false,
        user_interrupted: false,
        backgrounded: false,
        expanded: true,
    });
    for index in 0..20 {
        transcript.order.push(TranscriptEntry::Activity {
            text: format!("after tool {index}"),
            time: "12:00".to_string(),
        });
    }
    transcript
}

#[test]
fn mouse_collapse_of_tall_diff_keeps_the_tool_header_at_the_anchor_row() {
    let mut transcript = tall_expanded_diff_transcript();
    let before = transcript.render(100, None, None, None);
    let viewport_height = 12;
    let scroll_max = before.0.len() - viewport_height;
    let scroll_from_bottom = scroll_max - 40;
    let anchor = transcript_viewport_anchor(
        &before.1,
        &before.4,
        scroll_max,
        scroll_from_bottom,
        viewport_height,
        true,
    )
    .expect("anchor inside the tall diff");
    let tool_index = transcript
        .order
        .iter()
        .position(|entry| matches!(entry, TranscriptEntry::Tool { .. }))
        .unwrap();
    assert_eq!(anchor.collapsed_tool_header, Some(tool_index));

    transcript.toggle_tool(tool_index);
    let after = transcript.render(100, None, None, None);
    let restored = restore_transcript_viewport_anchor(
        anchor,
        &after.1,
        &after.4,
        after.0.len(),
        viewport_height,
        scroll_from_bottom,
    );
    let restored_start = after.0.len().saturating_sub(viewport_height + restored);
    assert_eq!(
        restored_start.saturating_add(anchor.viewport_row),
        after.1[0].1
    );
}

#[test]
fn keyboard_collapse_of_tall_output_uses_the_same_reflow_anchor() {
    let mut transcript = tall_expanded_diff_transcript();
    if let Some(TranscriptEntry::Tool {
        code_view,
        output_view,
        ..
    }) = transcript
        .order
        .iter_mut()
        .find(|entry| matches!(entry, TranscriptEntry::Tool { .. }))
    {
        *code_view = Some(("text".to_string(), "command input".to_string()));
        *output_view = Some((
            "text".to_string(),
            (0..240)
                .map(|line| format!("output line {line}"))
                .collect::<Vec<_>>()
                .join("\n"),
        ));
    }
    let before = transcript.render(100, None, None, None);
    let viewport_height = 10;
    let scroll_max = before.0.len() - viewport_height;
    let scroll_from_bottom = scroll_max - 30;
    let anchor = transcript_viewport_anchor(
        &before.1,
        &before.4,
        scroll_max,
        scroll_from_bottom,
        viewport_height,
        true,
    )
    .expect("anchor inside the tall output");

    transcript.set_auto_expand_tools(false);
    let after = transcript.render(100, None, None, None);
    let restored = restore_transcript_viewport_anchor(
        anchor,
        &after.1,
        &after.4,
        after.0.len(),
        viewport_height,
        scroll_from_bottom,
    );
    let restored_start = after.0.len().saturating_sub(viewport_height + restored);
    assert_eq!(
        restored_start.saturating_add(anchor.viewport_row),
        after.1[0].1
    );
}

#[test]
fn line_scrolling_preserves_expanded_actions() {
    let mut transcript = Transcript::default();
    for index in 0..9 {
        transcript.order.push(TranscriptEntry::Tool {
            source_name: "Edit".to_string(),
            name: "Edit".to_string(),
            detail: format!("file-{index}.rs"),
            code_view: Some((
                "diff:rs".to_string(),
                (0..12)
                    .map(|line| format!("+changed-{line}"))
                    .collect::<Vec<_>>()
                    .join("\n"),
            )),
            output_view: None,
            payload_refs: Vec::new(),
            time: "12:00".to_string(),
            started_at: Utc::now(),
            completed_at: Some(Utc::now()),
            complete: true,
            error: false,
            user_interrupted: false,
            backgrounded: false,
            expanded: index == 8,
        });
    }

    let render = transcript.render(100, None, None, None);
    let max_offset = render.2[0].3;
    assert!(max_offset > DEFAULT_TOOL_RUN_VIEWPORT_HEIGHT);
    assert!(matches!(
        &transcript.order[8],
        TranscriptEntry::Tool { expanded: true, .. }
    ));
    assert!(render.0.iter().any(|line| line.to_string() == "└─"));

    assert!(transcript.scroll_tool_run(0, max_offset, -3));
    assert!(matches!(
        &transcript.order[8],
        TranscriptEntry::Tool { expanded: true, .. }
    ));
    let rendered = transcript
        .lines(100)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(rendered.contains("└─ ↓ more"));
}

#[test]
fn scrolled_action_viewport_pins_the_current_tool_header() {
    let mut transcript = Transcript::default();
    for index in 0..9 {
        transcript.order.push(TranscriptEntry::Tool {
            source_name: "Edit".to_string(),
            name: "Edit".to_string(),
            detail: format!("file-{index}.rs"),
            code_view: Some((
                "diff:rs".to_string(),
                (0..20)
                    .map(|line| format!("+changed-{line}"))
                    .collect::<Vec<_>>()
                    .join("\n"),
            )),
            output_view: None,
            payload_refs: Vec::new(),
            time: "12:00".to_string(),
            started_at: Utc::now(),
            completed_at: Some(Utc::now()),
            complete: true,
            error: false,
            user_interrupted: false,
            backgrounded: false,
            expanded: index == 8,
        });
    }

    let rendered = transcript.render_with_tool_run_viewport(100, 8, Some(8), None, None);
    let pinned = &rendered.0[1];

    assert!(pinned.to_string().contains("Edit"));
    assert!(
        pinned
            .spans
            .iter()
            .any(|span| span.style.bg == Some(MESSAGE_HOVER_BG))
    );
}

#[test]
fn expanding_an_action_preserves_the_current_line_anchor() {
    let mut transcript = Transcript::default();
    for index in 0..9 {
        transcript.order.push(TranscriptEntry::Tool {
            source_name: "Edit".to_string(),
            name: "Edit".to_string(),
            detail: format!("file-{index}.rs"),
            code_view: Some(("diff:rs".to_string(), "+first\n+second\n+third".to_string())),
            output_view: None,
            payload_refs: Vec::new(),
            time: "12:00".to_string(),
            started_at: Utc::now(),
            completed_at: Some(Utc::now()),
            complete: true,
            error: false,
            user_interrupted: false,
            backgrounded: false,
            expanded: false,
        });
    }

    let max_offset = transcript.render(100, None, None, None).2[0].3;
    transcript.anchor_tool_run(0, max_offset);
    transcript.toggle_tool(8);

    assert_eq!(transcript.tool_run_offsets.get(&0), Some(&max_offset));
    assert!(transcript.render(100, None, None, None).2[0].3 > max_offset);
}

#[test]
fn reasoning_is_one_live_muted_disclosure_that_collapses_at_a_tool_boundary() {
    let session_id = Uuid::new_v4();
    let mut transcript = Transcript::default();
    transcript.apply(&SessionEvent::new(
        session_id,
        1,
        SessionEventKind::ReasoningDelta {
            text: "Checking".to_string(),
        },
    ));
    transcript.apply(&SessionEvent::new(
        session_id,
        2,
        SessionEventKind::ReasoningDelta {
            text: " the source".to_string(),
        },
    ));

    assert_eq!(transcript.order.len(), 1);
    assert!(matches!(
        &transcript.order[0],
        TranscriptEntry::Tool {
            name,
            code_view: Some((language, source)),
            complete: false,
            expanded: true,
            ..
        } if name == "Thinking"
            && language == "reasoning"
            && source == "Checking the source"
    ));
    assert!(transcript.tool_spinner_cache_tick().is_some());
    let rendered = transcript
        .lines(100)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(rendered.chars().any(|glyph| "⠋⠙⠹⠸⠼⠴⠦⠧".contains(glyph)));

    transcript.apply(&SessionEvent::new(
        session_id,
        3,
        SessionEventKind::ToolStarted {
            tool_call_id: "read-1".to_string(),
            name: "read_file".to_string(),
            input: serde_json::json!({"path": "src/lib.rs"}),
            input_ref: None,
        },
    ));
    assert!(matches!(
        &transcript.order[0],
        TranscriptEntry::Tool {
            complete: true,
            expanded: false,
            ..
        }
    ));
    transcript.toggle_tool(0);
    assert!(matches!(
        &transcript.order[0],
        TranscriptEntry::Tool { expanded: true, .. }
    ));
}

#[test]
fn reasoning_completion_freezes_thinking_duration_before_a_delayed_tool() {
    let session_id = Uuid::new_v4();
    let started_at = Utc::now();
    let completed_at = started_at + chrono::Duration::seconds(2);
    let tool_started_at = started_at + chrono::Duration::seconds(9);
    let mut transcript = Transcript::default();
    let mut reasoning = SessionEvent::new(
        session_id,
        1,
        SessionEventKind::ReasoningDelta {
            text: "Checking the source".to_string(),
        },
    );
    reasoning.created_at = started_at;
    transcript.apply(&reasoning);
    let mut completed = SessionEvent::new(session_id, 2, SessionEventKind::ReasoningCompleted);
    completed.created_at = completed_at;
    transcript.apply(&completed);
    let mut tool = SessionEvent::new(
        session_id,
        3,
        SessionEventKind::ToolStarted {
            tool_call_id: "read-1".to_string(),
            name: "read_file".to_string(),
            input: serde_json::json!({"path": "src/lib.rs"}),
            input_ref: None,
        },
    );
    tool.created_at = tool_started_at;
    transcript.apply(&tool);

    let TranscriptEntry::Tool {
        started_at: stored_started_at,
        completed_at: Some(stored_completed_at),
        ..
    } = &transcript.order[0]
    else {
        panic!("expected completed thinking card");
    };
    assert_eq!(*stored_started_at, started_at);
    assert_eq!(*stored_completed_at, completed_at);
    let rendered = transcript
        .lines(100)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(rendered.contains("2.0s"));
    assert!(!rendered.contains("9.0s"));
}

#[test]
fn rich_plan_orders_active_work_first_and_mutes_completed_work() {
    let mut transcript = Transcript::default();
    transcript.order.push(TranscriptEntry::Plan {
        items: vec![
            PlanItem {
                id: Uuid::new_v4(),
                content: "Already done".to_string(),
                status: PlanItemStatus::Completed,
            },
            PlanItem {
                id: Uuid::new_v4(),
                content: "Still to do".to_string(),
                status: PlanItemStatus::Pending,
            },
            PlanItem {
                id: Uuid::new_v4(),
                content: "Working now".to_string(),
                status: PlanItemStatus::InProgress,
            },
        ],
        time: "12:00".to_string(),
        expanded: false,
    });

    let lines = transcript.lines(80);
    let find = |text: &str| {
        lines
            .iter()
            .position(|line| line.spans.iter().any(|span| span.content.contains(text)))
            .expect("plan item is rendered")
    };
    let in_progress = find("Working now");
    let pending = find("Still to do");
    let completed = find("Already done");
    assert!(in_progress < pending && pending < completed);
    let in_progress_marker = lines[in_progress]
        .spans
        .iter()
        .find(|span| span.content.contains('●'))
        .expect("in-progress plan marker");
    assert!(!in_progress_marker.content.contains('◌'));
    let pending_marker = lines[pending]
        .spans
        .iter()
        .find(|span| span.content.contains('○'))
        .expect("pending plan marker");
    assert!(!pending_marker.content.contains('●'));
    assert!(
        lines
            .iter()
            .flat_map(|line| line.spans.iter())
            .any(|span| span.content.contains("1/3 completed"))
    );
    let completed_marker = lines[completed]
        .spans
        .iter()
        .find(|span| span.content.contains('✓'))
        .expect("completed plan marker");
    assert!(
        !completed_marker
            .style
            .add_modifier
            .contains(Modifier::CROSSED_OUT)
    );
    let completed_span = lines[completed]
        .spans
        .iter()
        .find(|span| span.content.contains("Already done"))
        .expect("completed plan text");
    assert_eq!(completed_span.style.fg, Some(Color::DarkGray));
    assert!(
        completed_span
            .style
            .add_modifier
            .contains(Modifier::CROSSED_OUT)
    );
}

#[test]
fn plan_cards_copy_the_complete_readable_todo_list() {
    let entry = TranscriptEntry::Plan {
        items: vec![
            PlanItem {
                id: Uuid::new_v4(),
                content: "Inspect scrolling".to_string(),
                status: PlanItemStatus::Completed,
            },
            PlanItem {
                id: Uuid::new_v4(),
                content: "Polish interactions".to_string(),
                status: PlanItemStatus::InProgress,
            },
        ],
        time: "12:00".to_string(),
        expanded: false,
    };

    assert_eq!(
        entry.copy_text_owned().as_deref(),
        Some("✓ Inspect scrolling\n● Polish interactions")
    );
}

#[test]
fn long_plans_clip_with_a_hint_and_expand_on_toggle() {
    let mut transcript = Transcript::default();
    transcript.order.push(TranscriptEntry::Plan {
        items: (0..8)
            .map(|index| PlanItem {
                id: Uuid::new_v4(),
                content: format!("Step {index}"),
                status: PlanItemStatus::Pending,
            })
            .collect(),
        time: "12:00".to_string(),
        expanded: false,
    });
    let render = |transcript: &Transcript| {
        transcript
            .lines(80)
            .into_iter()
            .map(|line| line.to_string())
            .collect::<Vec<_>>()
            .join("\n")
    };

    assert!(transcript.plan_is_clippable(0));
    let clipped = render(&transcript);
    assert!(clipped.contains("Step 0"), "{clipped}");
    assert!(clipped.contains("Step 4"), "{clipped}");
    assert!(!clipped.contains("Step 5"), "{clipped}");
    assert!(clipped.contains("+ 3 more · click to expand"), "{clipped}");

    transcript.toggle_plan_expansion(0);
    let expanded = render(&transcript);
    assert!(expanded.contains("Step 7"), "{expanded}");
    assert!(expanded.contains("− show less"), "{expanded}");
    assert!(!expanded.contains("+ 3 more"), "{expanded}");

    transcript.toggle_plan_expansion(0);
    let reclipped = render(&transcript);
    assert!(!reclipped.contains("Step 5"), "{reclipped}");
}

#[test]
fn upserting_a_plan_preserves_its_expansion_state() {
    let mut transcript = Transcript::default();
    let items = |count: usize| {
        (0..count)
            .map(|index| PlanItem {
                id: Uuid::new_v4(),
                content: format!("Step {index}"),
                status: PlanItemStatus::Pending,
            })
            .collect::<Vec<_>>()
    };
    transcript.upsert_plan(items(8), "12:00".to_string());
    transcript.toggle_plan_expansion(transcript.order.len() - 1);

    transcript.upsert_plan(items(9), "12:01".to_string());

    assert!(matches!(
        transcript.order.last(),
        Some(TranscriptEntry::Plan {
            expanded: true,
            items,
            ..
        }) if items.len() == 9
    ));
}

#[test]
fn interrupted_tools_update_in_place_with_explicit_user_cause() {
    let mut transcript = Transcript::default();
    transcript.order.push(TranscriptEntry::Tool {
        source_name: "Run".to_string(),
        name: "Run".to_string(),
        detail: "cargo check".to_string(),
        code_view: Some(("bash".to_string(), "cargo check".to_string())),
        output_view: None,
        payload_refs: Vec::new(),
        time: "12:00".to_string(),
        started_at: Utc::now(),
        completed_at: None,
        complete: false,
        error: false,
        user_interrupted: false,
        backgrounded: false,
        expanded: false,
    });

    transcript.mark_running_tools_user_interrupted(Utc::now());

    assert!(matches!(
        transcript.order.first(),
        Some(TranscriptEntry::Tool {
            complete: true,
            user_interrupted: true,
            ..
        })
    ));
    assert!(
        transcript
            .lines(100)
            .iter()
            .any(|line| line.to_string().contains("user interrupted"))
    );
}

#[test]
fn turn_completion_settles_unresolved_foreground_tools() {
    let session_id = Uuid::new_v4();
    let mut transcript = Transcript::default();
    transcript.apply(&SessionEvent::new(
        session_id,
        1,
        SessionEventKind::ToolStarted {
            tool_call_id: "run-1".to_string(),
            name: "exec".to_string(),
            input: serde_json::json!({"cmd": "just cli"}),
            input_ref: None,
        },
    ));
    transcript.apply(&SessionEvent::new(
        session_id,
        2,
        SessionEventKind::TurnCompleted {
            message_id: Uuid::new_v4(),
            provider_session_id: None,
            final_text: String::new(),
            error: None,
        },
    ));

    assert!(matches!(
        transcript.order.first(),
        Some(TranscriptEntry::Tool {
            complete: true,
            error: false,
            user_interrupted: false,
            ..
        })
    ));
    let rendered = transcript
        .lines(100)
        .into_iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(rendered.contains('✓'));
    assert!(!rendered.contains("completed"));
}

#[test]
fn completed_web_search_updates_the_started_card_with_the_late_query() {
    let session_id = Uuid::new_v4();
    let mut transcript = Transcript::default();
    transcript.apply(&SessionEvent::new(
        session_id,
        1,
        SessionEventKind::ToolStarted {
            tool_call_id: "search-1".to_string(),
            name: "web_search".to_string(),
            input: serde_json::Value::Null,
            input_ref: None,
        },
    ));
    transcript.apply(&SessionEvent::new(
        session_id,
        2,
        SessionEventKind::ToolCompleted {
            tool_call_id: "search-1".to_string(),
            output: String::new(),
            output_ref: None,
            is_error: false,
            input: Some(serde_json::json!({"query": "Borg CLI queue"})),
            input_ref: None,
        },
    ));

    assert!(matches!(
        transcript.order.first(),
        Some(TranscriptEntry::Tool { name, detail, complete: true, .. })
            if name == "Search web" && detail == "“Borg CLI queue”"
    ));
}

#[test]
fn empty_assistant_updates_are_not_rendered() {
    let mut transcript = Transcript::default();
    transcript.apply(&SessionEvent::new(
        Uuid::new_v4(),
        1,
        SessionEventKind::Message {
            message_id: Uuid::new_v4(),
            actor: EventActor::Assistant,
            text: "  ".to_string(),
            attachments: Vec::new(),
            status: MessageStatus::Complete,
            delivery: None,
        },
    ));

    assert!(transcript.order.is_empty());
}

#[test]
fn transcript_text_selection_uses_stable_document_rows() {
    let lines = vec![
        Line::from("zero"),
        Line::from("one two"),
        Line::from("three four"),
        Line::from("five"),
    ];
    let start = TranscriptPoint { row: 1, column: 4 };
    let end = TranscriptPoint { row: 3, column: 2 };

    assert_eq!(
        selected_transcript_text(&lines, start, end).as_deref(),
        Some("two\nthree four\nfi")
    );
}

#[test]
fn selection_highlight_survives_a_changed_viewport_offset() {
    let start = TranscriptPoint { row: 5, column: 1 };
    let end = TranscriptPoint { row: 6, column: 2 };
    let mut first_view = vec![Line::from("abcd"), Line::from("efgh")];
    apply_text_selection(&mut first_view, 5, start, end);
    assert!(
        first_view[0]
            .spans
            .iter()
            .any(|span| span.style.bg.is_some())
    );

    let mut scrolled_view = vec![Line::from("xxxx"), Line::from("abcd")];
    apply_text_selection(&mut scrolled_view, 4, start, end);
    assert!(
        scrolled_view[0]
            .spans
            .iter()
            .all(|span| span.style.bg.is_none())
    );
    assert!(
        scrolled_view[1]
            .spans
            .iter()
            .any(|span| span.style.bg.is_some())
    );
}

#[test]
fn incoming_transcript_lines_move_selection_with_live_content() {
    let previous_height = 20;
    let next_height = 21;
    let viewport_height = 5;
    let previous_scroll_from_bottom = 0;
    let previous_scroll_start = previous_height - viewport_height;
    let next_scroll_from_bottom =
        if should_preserve_transcript_viewport(previous_scroll_from_bottom, true) {
            preserve_scroll_anchor(previous_scroll_from_bottom, previous_height, next_height)
        } else {
            previous_scroll_from_bottom
        };
    let next_scroll_start = (next_height - viewport_height) - next_scroll_from_bottom;

    assert_eq!(next_scroll_start, previous_scroll_start + 1);

    let start = TranscriptPoint {
        row: previous_scroll_start + 2,
        column: 1,
    };
    let end = TranscriptPoint {
        row: previous_scroll_start + 2,
        column: 2,
    };
    let mut viewport = vec![
        Line::from("line 16"),
        Line::from("selected"),
        Line::from("line 18"),
    ];
    apply_text_selection(&mut viewport, next_scroll_start, start, end);
    assert!(viewport[1].spans.iter().any(|span| span.style.bg.is_some()));
    assert!(viewport[2].spans.iter().all(|span| span.style.bg.is_none()));
    assert!(should_preserve_transcript_viewport(3, false));
    assert!(!should_preserve_transcript_viewport(3, true));
}

#[test]
fn selection_points_resolve_through_accordion_body_offsets() {
    // Entry 1 is a boxed tool whose visible slice starts at body row 3.
    let ranges = vec![(0, 2, 10, 0), (1, 10, 18, 3)];

    // Absolute row 12 inside the slice anchors to body row 5 of entry 1.
    let anchor = selection_point_for_row(&ranges, 12, 4);
    assert_eq!(
        anchor,
        SelectionPoint {
            entry: 1,
            row_in_entry: 5,
            column: 4,
        }
    );

    // The window shifts down by one: the slice now starts at body row 4.
    let shifted = vec![(0, 2, 10, 0), (1, 10, 18, 4)];
    let resolved = resolve_selection_point(anchor, &shifted);
    assert_eq!(resolved, Some(TranscriptPoint { row: 11, column: 4 }));

    // Body rows that scroll out of the slice clamp to its nearest edge.
    let clamped = resolve_selection_point(
        SelectionPoint {
            entry: 1,
            row_in_entry: 0,
            column: 4,
        },
        &shifted,
    );
    assert_eq!(clamped, Some(TranscriptPoint { row: 10, column: 4 }));
}

#[test]
fn selection_anchors_follow_content_when_the_actions_window_shifts() {
    let tool = |index: usize| TranscriptEntry::Tool {
        source_name: "Run".to_string(),
        name: "Run".to_string(),
        detail: format!("call-{index}"),
        code_view: None,
        output_view: None,
        payload_refs: Vec::new(),
        time: "12:00".to_string(),
        started_at: Utc::now(),
        completed_at: Some(Utc::now()),
        complete: true,
        error: false,
        user_interrupted: false,
        backgrounded: false,
        expanded: false,
    };
    let mut transcript = Transcript::default();
    for index in 0..20 {
        transcript.order.push(tool(index));
    }

    let before = transcript.render(100, None, None, None);
    let target_row = before
        .0
        .iter()
        .position(|line| line.to_string().contains("call-15"))
        .expect("call-15 is visible before the window shifts");
    let anchor = selection_point_for_row(&before.6, target_row, 3);

    // A new tool line pushes the accordion window up under the selection.
    transcript.order.push(tool(20));

    let after = transcript.render(100, None, None, None);
    let resolved = resolve_selection_point(anchor, &after.6).expect("anchor resolves");
    let rendered = after
        .0
        .iter()
        .map(|line| line.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        after.0[resolved.row].to_string().contains("call-15"),
        "anchor must track call-15 to row {}:\n{rendered}",
        resolved.row,
    );
    assert_eq!(resolved.row, target_row - 1);
}
