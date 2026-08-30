use std::io;
use std::time::Duration;

use crossterm::event::{
    Event, EventStream, KeyCode, KeyEventKind, KeyModifiers, MouseEvent, MouseEventKind,
};
use futures_util::{Stream, StreamExt};

const PRIORITY_EVENT_CHANNEL_CAPACITY: usize = 64;
const VISUAL_EVENT_CHANNEL_CAPACITY: usize = 32;
const WHEEL_FLUSH_INTERVAL: Duration = Duration::from_millis(4);
const POINTER_MOVE_FLUSH_INTERVAL: Duration = Duration::from_millis(8);
const DRAG_FLUSH_INTERVAL: Duration = Duration::from_millis(8);
const RESIZE_FLUSH_INTERVAL: Duration = Duration::from_millis(33);
const MAX_PENDING_WHEEL_EVENTS: isize = 32;

pub struct TerminalInputEvent {
    pub(super) event: Event,
    pub(super) scroll_repetitions: usize,
}

impl TerminalInputEvent {
    fn single(event: Event) -> Self {
        Self {
            event,
            scroll_repetitions: 1,
        }
    }

    pub fn is_up(&self) -> bool {
        matches!(
            &self.event,
            Event::Key(key) if key.kind != KeyEventKind::Release && key.code == KeyCode::Up
        )
    }

    pub fn is_keyboard_input(&self) -> bool {
        matches!(&self.event, Event::Paste(_))
            || matches!(&self.event, Event::Key(key) if key.kind != KeyEventKind::Release)
    }

    pub fn is_interaction_input(&self) -> bool {
        self.is_keyboard_input() || matches!(&self.event, Event::Mouse(_))
    }

    pub fn may_change_transcript_view(&self) -> bool {
        match &self.event {
            Event::Mouse(mouse) => !matches!(mouse.kind, MouseEventKind::Moved),
            Event::Key(key) if key.kind != KeyEventKind::Release => {
                key.modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
                    || matches!(
                        key.code,
                        KeyCode::Enter
                            | KeyCode::Esc
                            | KeyCode::Tab
                            | KeyCode::BackTab
                            | KeyCode::PageUp
                            | KeyCode::PageDown
                    )
            }
            _ => false,
        }
    }
}

pub(super) struct TerminalInput {
    priority_events: tokio::sync::mpsc::Receiver<io::Result<Event>>,
    visual_events: tokio::sync::mpsc::Receiver<io::Result<Event>>,
    wheel_events: tokio::sync::mpsc::Receiver<WheelBurst>,
    deferred_visual: Option<io::Result<Event>>,
    priority_closed: bool,
    visual_closed: bool,
    wheel_closed: bool,
    pump: Option<tokio::task::JoinHandle<()>>,
}

impl TerminalInput {
    pub(super) fn spawn() -> Self {
        let (priority_tx, priority_events) =
            tokio::sync::mpsc::channel(PRIORITY_EVENT_CHANNEL_CAPACITY);
        let (visual_tx, visual_events) = tokio::sync::mpsc::channel(VISUAL_EVENT_CHANNEL_CAPACITY);
        let (wheel_tx, wheel_events) = tokio::sync::mpsc::channel(1);
        let pump = tokio::spawn(async move {
            pump_terminal_events(EventStream::new(), priority_tx, visual_tx, wheel_tx).await;
        });
        Self {
            priority_events,
            visual_events,
            wheel_events,
            deferred_visual: None,
            priority_closed: false,
            visual_closed: false,
            wheel_closed: false,
            pump: Some(pump),
        }
    }

    pub(super) async fn next_event(&mut self) -> Option<io::Result<TerminalInputEvent>> {
        loop {
            if !self.priority_closed {
                match self.priority_events.try_recv() {
                    Ok(event) => return Some(event.map(TerminalInputEvent::single)),
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                        self.priority_closed = true;
                    }
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
                }
            }
            if let Some(event) = self.deferred_visual.take() {
                return Some(
                    self.coalesce_ready_resize(event)
                        .map(TerminalInputEvent::single),
                );
            }
            if !self.visual_closed {
                match self.visual_events.try_recv() {
                    Ok(event) => {
                        return Some(
                            self.coalesce_ready_resize(event)
                                .map(TerminalInputEvent::single),
                        );
                    }
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                        self.visual_closed = true;
                    }
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
                }
            }
            if !self.wheel_closed {
                match self.wheel_events.try_recv() {
                    Ok(wheel) => return Some(Ok(wheel.into())),
                    Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                        self.wheel_closed = true;
                    }
                    Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
                }
            }
            if self.priority_closed && self.visual_closed && self.wheel_closed {
                return None;
            }

            tokio::select! {
                biased;
                event = self.priority_events.recv(), if !self.priority_closed => {
                    match event {
                        Some(event) => return Some(event.map(TerminalInputEvent::single)),
                        None => self.priority_closed = true,
                    }
                }
                event = self.visual_events.recv(), if !self.visual_closed => {
                    match event {
                        Some(event) => {
                            return Some(self.coalesce_ready_resize(event).map(TerminalInputEvent::single));
                        }
                        None => self.visual_closed = true,
                    }
                }
                wheel = self.wheel_events.recv(), if !self.wheel_closed => {
                    match wheel {
                        Some(wheel) => return Some(Ok(wheel.into())),
                        None => self.wheel_closed = true,
                    }
                }
            }
        }
    }

    fn coalesce_ready_resize(&mut self, event: io::Result<Event>) -> io::Result<Event> {
        if !matches!(&event, Ok(Event::Resize(_, _))) {
            return event;
        }
        let mut latest = event;
        loop {
            match self.visual_events.try_recv() {
                Ok(event @ Ok(Event::Resize(_, _))) => latest = event,
                Ok(event) => {
                    self.deferred_visual = Some(event);
                    break;
                }
                Err(
                    tokio::sync::mpsc::error::TryRecvError::Empty
                    | tokio::sync::mpsc::error::TryRecvError::Disconnected,
                ) => break,
            }
        }
        latest
    }

    pub(super) fn abort(&mut self) {
        if let Some(pump) = &self.pump {
            pump.abort();
        }
    }

    pub(super) async fn shutdown(&mut self) {
        let Some(pump) = self.pump.take() else {
            return;
        };
        pump.abort();
        let _ = pump.await;
    }
}

impl Drop for TerminalInput {
    fn drop(&mut self) {
        self.abort();
    }
}

#[derive(Clone, Copy)]
struct WheelBurst {
    mouse: MouseEvent,
    repetitions: isize,
}

impl From<WheelBurst> for TerminalInputEvent {
    fn from(mut burst: WheelBurst) -> Self {
        burst.mouse.kind = if burst.repetitions > 0 {
            MouseEventKind::ScrollUp
        } else {
            MouseEventKind::ScrollDown
        };
        Self {
            event: Event::Mouse(burst.mouse),
            scroll_repetitions: burst.repetitions.unsigned_abs(),
        }
    }
}

fn push_wheel(pending: &mut Option<WheelBurst>, mouse: MouseEvent) {
    let direction = match mouse.kind {
        MouseEventKind::ScrollUp => 1,
        MouseEventKind::ScrollDown => -1,
        _ => return,
    };
    match pending {
        Some(burst) if burst.repetitions.signum() == direction => {
            burst.mouse = mouse;
            burst.repetitions = burst
                .repetitions
                .saturating_add(direction)
                .clamp(-MAX_PENDING_WHEEL_EVENTS, MAX_PENDING_WHEEL_EVENTS);
        }
        slot => {
            *slot = Some(WheelBurst {
                mouse,
                repetitions: direction,
            });
        }
    }
}

fn flush_wheel(
    pending: &mut Option<WheelBurst>,
    wheel_tx: &tokio::sync::mpsc::Sender<WheelBurst>,
) -> bool {
    let Some(burst) = pending.take() else {
        return true;
    };
    match wheel_tx.try_send(burst) {
        Ok(()) => true,
        Err(tokio::sync::mpsc::error::TrySendError::Full(burst)) => {
            *pending = Some(burst);
            true
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => false,
    }
}

fn flush_resize(
    pending: &mut Option<Event>,
    visual_tx: &tokio::sync::mpsc::Sender<io::Result<Event>>,
) -> bool {
    let Some(event) = pending.take() else {
        return true;
    };
    match visual_tx.try_send(Ok(event)) {
        Ok(()) => true,
        Err(tokio::sync::mpsc::error::TrySendError::Full(Ok(event))) => {
            *pending = Some(event);
            true
        }
        Err(tokio::sync::mpsc::error::TrySendError::Closed(_))
        | Err(tokio::sync::mpsc::error::TrySendError::Full(Err(_))) => false,
    }
}

fn flush_drag(
    pending: &mut Option<Event>,
    visual_tx: &tokio::sync::mpsc::Sender<io::Result<Event>>,
) -> bool {
    flush_resize(pending, visual_tx)
}

fn flush_pending_visual(
    pending: &mut Option<Event>,
    visual_tx: &tokio::sync::mpsc::Sender<io::Result<Event>>,
) -> bool {
    flush_resize(pending, visual_tx)
}

fn flush_pending_visuals(
    pending_pointer_move: &mut Option<Event>,
    pending_drag: &mut Option<Event>,
    pending_resize: &mut Option<Event>,
    visual_tx: &tokio::sync::mpsc::Sender<io::Result<Event>>,
) -> bool {
    flush_pending_visual(pending_pointer_move, visual_tx)
        && flush_pending_visual(pending_drag, visual_tx)
        && flush_pending_visual(pending_resize, visual_tx)
}

async fn pump_terminal_events<S>(
    mut events: S,
    priority_tx: tokio::sync::mpsc::Sender<io::Result<Event>>,
    visual_tx: tokio::sync::mpsc::Sender<io::Result<Event>>,
    wheel_tx: tokio::sync::mpsc::Sender<WheelBurst>,
) where
    S: Stream<Item = io::Result<Event>> + Unpin,
{
    let mut pending_wheel = None;
    let mut pending_pointer_move = None;
    let mut pending_drag = None;
    let mut pending_resize = None;
    let mut wheel_flush_tick = tokio::time::interval(WHEEL_FLUSH_INTERVAL);
    wheel_flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut pointer_move_flush_tick = tokio::time::interval_at(
        tokio::time::Instant::now() + POINTER_MOVE_FLUSH_INTERVAL,
        POINTER_MOVE_FLUSH_INTERVAL,
    );
    pointer_move_flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut drag_flush_tick = tokio::time::interval_at(
        tokio::time::Instant::now() + DRAG_FLUSH_INTERVAL,
        DRAG_FLUSH_INTERVAL,
    );
    drag_flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut resize_flush_tick = tokio::time::interval_at(
        tokio::time::Instant::now() + RESIZE_FLUSH_INTERVAL,
        RESIZE_FLUSH_INTERVAL,
    );
    resize_flush_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            event = events.next() => {
                let Some(event) = event else {
                    let _ = flush_wheel(&mut pending_wheel, &wheel_tx);
                    let _ = flush_pending_visuals(
                        &mut pending_pointer_move,
                        &mut pending_drag,
                        &mut pending_resize,
                        &visual_tx,
                    );
                    return;
                };
                if let Ok(Event::Resize(_, _)) = &event {
                    pending_resize = event.ok();
                } else if let Ok(Event::Mouse(mouse)) = &event
                    && matches!(
                        mouse.kind,
                        MouseEventKind::ScrollUp | MouseEventKind::ScrollDown
                    )
                {
                    push_wheel(&mut pending_wheel, *mouse);
                } else if let Ok(Event::Mouse(mouse)) = &event
                    && matches!(mouse.kind, MouseEventKind::Moved)
                {
                    if !flush_pending_visual(&mut pending_drag, &visual_tx) {
                        return;
                    }
                    pending_pointer_move = event.ok();
                } else if let Ok(Event::Mouse(mouse)) = &event
                    && matches!(mouse.kind, MouseEventKind::Drag(_))
                {
                    if !flush_pending_visual(&mut pending_pointer_move, &visual_tx) {
                        return;
                    }
                    pending_drag = event.ok();
                } else {
                    // Visual events are best-effort and coalesced. Never let
                    // their bounded queue delay a keyboard, paste, focus, or
                    // mouse-button event that can change session state.
                    if !flush_pending_visuals(
                        &mut pending_pointer_move,
                        &mut pending_drag,
                        &mut pending_resize,
                        &visual_tx,
                    ) {
                        return;
                    }
                    if priority_tx.send(event).await.is_err() {
                        return;
                    }
                }
            }
            _ = wheel_flush_tick.tick() => {
                if !flush_wheel(&mut pending_wheel, &wheel_tx) {
                    return;
                }
            }
            _ = pointer_move_flush_tick.tick() => {
                if !flush_drag(&mut pending_pointer_move, &visual_tx) {
                    return;
                }
            }
            _ = drag_flush_tick.tick() => {
                if !flush_drag(&mut pending_drag, &visual_tx) {
                    return;
                }
            }
            _ = resize_flush_tick.tick() => {
                if !flush_resize(&mut pending_resize, &visual_tx) {
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use crossterm::event::{
        Event, KeyCode, KeyEvent, KeyEventKind, KeyModifiers, MouseEvent, MouseEventKind,
    };

    use super::*;

    fn wheel_mouse(kind: MouseEventKind) -> MouseEvent {
        MouseEvent {
            kind,
            column: 10,
            row: 10,
            modifiers: KeyModifiers::NONE,
        }
    }

    #[test]
    fn up_recall_barrier_only_matches_non_release_up_keys() {
        let press =
            TerminalInputEvent::single(Event::Key(KeyEvent::new(KeyCode::Up, KeyModifiers::NONE)));
        assert!(press.is_up());

        let mut release = KeyEvent::new(KeyCode::Up, KeyModifiers::NONE);
        release.kind = KeyEventKind::Release;
        assert!(!TerminalInputEvent::single(Event::Key(release)).is_up());
        assert!(
            !TerminalInputEvent::single(Event::Key(KeyEvent::new(
                KeyCode::Down,
                KeyModifiers::NONE,
            )))
            .is_up()
        );
    }

    #[test]
    fn keyboard_input_excludes_release_and_visual_events() {
        assert!(
            TerminalInputEvent::single(Event::Key(KeyEvent::new(
                KeyCode::Char('x'),
                KeyModifiers::NONE,
            )))
            .is_keyboard_input()
        );
        assert!(TerminalInputEvent::single(Event::Paste("x".to_string())).is_keyboard_input());

        let mut release = KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE);
        release.kind = KeyEventKind::Release;
        assert!(!TerminalInputEvent::single(Event::Key(release)).is_keyboard_input());
        assert!(!TerminalInputEvent::single(Event::Resize(80, 24)).is_keyboard_input());
    }

    #[test]
    fn interaction_input_includes_hover_click_drag_and_wheel() {
        for kind in [
            MouseEventKind::Moved,
            MouseEventKind::Down(crossterm::event::MouseButton::Left),
            MouseEventKind::Drag(crossterm::event::MouseButton::Left),
            MouseEventKind::ScrollDown,
        ] {
            assert!(
                TerminalInputEvent::single(Event::Mouse(wheel_mouse(kind))).is_interaction_input()
            );
        }
        assert!(
            TerminalInputEvent::single(Event::Key(KeyEvent::new(
                KeyCode::Char('x'),
                KeyModifiers::NONE,
            )))
            .is_interaction_input()
        );
        assert!(!TerminalInputEvent::single(Event::Resize(80, 24)).is_interaction_input());
    }

    #[test]
    fn passive_hover_and_text_input_do_not_request_transcript_reflow() {
        assert!(
            !TerminalInputEvent::single(Event::Mouse(wheel_mouse(MouseEventKind::Moved)))
                .may_change_transcript_view()
        );
        assert!(
            !TerminalInputEvent::single(Event::Key(KeyEvent::new(
                KeyCode::Char('x'),
                KeyModifiers::NONE,
            )))
            .may_change_transcript_view()
        );
        for kind in [
            MouseEventKind::Down(crossterm::event::MouseButton::Left),
            MouseEventKind::Drag(crossterm::event::MouseButton::Left),
            MouseEventKind::ScrollDown,
        ] {
            assert!(
                TerminalInputEvent::single(Event::Mouse(wheel_mouse(kind)))
                    .may_change_transcript_view()
            );
        }
        assert!(
            TerminalInputEvent::single(Event::Key(
                KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE,)
            ))
            .may_change_transcript_view()
        );
    }

    #[tokio::test]
    async fn wheel_flood_stays_out_of_the_discrete_input_queue() {
        let (priority_tx, mut priority_events) =
            tokio::sync::mpsc::channel(PRIORITY_EVENT_CHANNEL_CAPACITY);
        let (visual_tx, _visual_events) = tokio::sync::mpsc::channel(VISUAL_EVENT_CHANNEL_CAPACITY);
        let (wheel_tx, mut wheel_events) = tokio::sync::mpsc::channel(1);
        let events = (0..100_000)
            .map(|_| Ok(Event::Mouse(wheel_mouse(MouseEventKind::ScrollDown))))
            .chain(std::iter::once(Ok(Event::Key(KeyEvent::new(
                KeyCode::Char('x'),
                KeyModifiers::NONE,
            )))));

        pump_terminal_events(
            futures_util::stream::iter(events),
            priority_tx,
            visual_tx,
            wheel_tx,
        )
        .await;

        assert!(matches!(
            priority_events.recv().await,
            Some(Ok(Event::Key(KeyEvent {
                code: KeyCode::Char('x'),
                ..
            })))
        ));
        assert!(priority_events.recv().await.is_none());
        let input: TerminalInputEvent = wheel_events.recv().await.unwrap().into();
        assert_eq!(input.scroll_repetitions, MAX_PENDING_WHEEL_EVENTS as usize);
        assert!(matches!(
            input.event,
            Event::Mouse(MouseEvent {
                kind: MouseEventKind::ScrollDown,
                ..
            })
        ));
        assert!(wheel_events.recv().await.is_none());
    }

    #[tokio::test]
    async fn keyboard_events_bypass_a_full_visual_queue() {
        let (priority_tx, mut priority_events) =
            tokio::sync::mpsc::channel(PRIORITY_EVENT_CHANNEL_CAPACITY);
        let (visual_tx, _visual_events) = tokio::sync::mpsc::channel(1);
        visual_tx.send(Ok(Event::Resize(80, 24))).await.unwrap();
        let (wheel_tx, _wheel_events) = tokio::sync::mpsc::channel(1);
        let events = futures_util::stream::iter([
            Ok(Event::Resize(120, 40)),
            Ok(Event::Key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE))),
        ]);

        tokio::time::timeout(
            Duration::from_millis(100),
            pump_terminal_events(events, priority_tx, visual_tx, wheel_tx),
        )
        .await
        .expect("keyboard delivery must not wait for visual backpressure");
        assert!(matches!(
            priority_events.recv().await,
            Some(Ok(Event::Key(KeyEvent {
                code: KeyCode::Esc,
                ..
            })))
        ));
    }

    #[tokio::test]
    async fn pointer_motion_cannot_queue_a_wheel_gesture_behind_stale_coordinates() {
        let (priority_tx, _priority_events) =
            tokio::sync::mpsc::channel(PRIORITY_EVENT_CHANNEL_CAPACITY);
        let (visual_tx, mut visual_events) =
            tokio::sync::mpsc::channel(VISUAL_EVENT_CHANNEL_CAPACITY);
        let (wheel_tx, mut wheel_events) = tokio::sync::mpsc::channel(1);
        let events = (1..=10_000)
            .map(|row| {
                Ok(Event::Mouse(MouseEvent {
                    kind: MouseEventKind::Moved,
                    column: 10,
                    row,
                    modifiers: KeyModifiers::NONE,
                }))
            })
            .chain(std::iter::once(Ok(Event::Mouse(wheel_mouse(
                MouseEventKind::ScrollDown,
            )))));

        pump_terminal_events(
            futures_util::stream::iter(events),
            priority_tx,
            visual_tx,
            wheel_tx,
        )
        .await;

        assert!(matches!(
            visual_events.recv().await,
            Some(Ok(Event::Mouse(MouseEvent {
                kind: MouseEventKind::Moved,
                row: 10_000,
                ..
            })))
        ));
        assert!(visual_events.recv().await.is_none());
        let input: TerminalInputEvent = wheel_events.recv().await.unwrap().into();
        assert!(matches!(
            input.event,
            Event::Mouse(MouseEvent {
                kind: MouseEventKind::ScrollDown,
                ..
            })
        ));
    }

    #[tokio::test]
    async fn resize_burst_keeps_only_the_latest_dimensions() {
        let (priority_tx, mut priority_events) =
            tokio::sync::mpsc::channel(PRIORITY_EVENT_CHANNEL_CAPACITY);
        let (visual_tx, mut visual_events) =
            tokio::sync::mpsc::channel(VISUAL_EVENT_CHANNEL_CAPACITY);
        let (wheel_tx, _wheel_events) = tokio::sync::mpsc::channel(1);
        let events = (1..=10_000)
            .map(|width| Ok(Event::Resize(width, 40)))
            .chain(std::iter::once(Ok(Event::Key(KeyEvent::new(
                KeyCode::Char('x'),
                KeyModifiers::NONE,
            )))));

        pump_terminal_events(
            futures_util::stream::iter(events),
            priority_tx,
            visual_tx,
            wheel_tx,
        )
        .await;

        assert!(matches!(
            visual_events.recv().await,
            Some(Ok(Event::Resize(10_000, 40)))
        ));
        assert!(matches!(
            priority_events.recv().await,
            Some(Ok(Event::Key(KeyEvent {
                code: KeyCode::Char('x'),
                ..
            })))
        ));
        assert!(priority_events.recv().await.is_none());
    }

    #[tokio::test]
    async fn drag_burst_keeps_the_latest_pointer_position_before_release() {
        let (priority_tx, mut priority_events) =
            tokio::sync::mpsc::channel(PRIORITY_EVENT_CHANNEL_CAPACITY);
        let (visual_tx, mut visual_events) =
            tokio::sync::mpsc::channel(VISUAL_EVENT_CHANNEL_CAPACITY);
        let (wheel_tx, _wheel_events) = tokio::sync::mpsc::channel(1);
        let events = (1..=10_000)
            .map(|row| {
                Ok(Event::Mouse(MouseEvent {
                    kind: MouseEventKind::Drag(crossterm::event::MouseButton::Left),
                    column: 10,
                    row,
                    modifiers: KeyModifiers::NONE,
                }))
            })
            .chain(std::iter::once(Ok(Event::Mouse(MouseEvent {
                kind: MouseEventKind::Up(crossterm::event::MouseButton::Left),
                column: 10,
                row: 10_000,
                modifiers: KeyModifiers::NONE,
            }))));

        pump_terminal_events(
            futures_util::stream::iter(events),
            priority_tx,
            visual_tx,
            wheel_tx,
        )
        .await;

        let visual = std::iter::from_fn(|| visual_events.try_recv().ok()).collect::<Vec<_>>();
        let priority = std::iter::from_fn(|| priority_events.try_recv().ok()).collect::<Vec<_>>();
        assert!(visual.len() <= 1);
        assert_eq!(priority.len(), 1);
        assert!(matches!(
            visual.as_slice(),
            [Ok(Event::Mouse(MouseEvent {
                kind: MouseEventKind::Drag(_),
                row: 10_000,
                ..
            }))]
        ));
        assert!(matches!(
            priority.as_slice(),
            [Ok(Event::Mouse(MouseEvent {
                kind: MouseEventKind::Up(_),
                row: 10_000,
                ..
            }))]
        ));
    }

    #[tokio::test]
    async fn queued_resize_frames_collapse_before_the_renderer_sees_them() {
        let (priority_tx, priority_events) =
            tokio::sync::mpsc::channel(PRIORITY_EVENT_CHANNEL_CAPACITY);
        let (visual_tx, visual_events) = tokio::sync::mpsc::channel(VISUAL_EVENT_CHANNEL_CAPACITY);
        let (_wheel_tx, wheel_events) = tokio::sync::mpsc::channel(1);
        for width in 1..=20 {
            visual_tx.send(Ok(Event::Resize(width, 40))).await.unwrap();
        }
        priority_tx
            .send(Ok(Event::Key(KeyEvent::new(
                KeyCode::Char('x'),
                KeyModifiers::NONE,
            ))))
            .await
            .unwrap();
        drop(priority_tx);
        drop(visual_tx);
        let pump = tokio::spawn(std::future::pending());
        let mut input = TerminalInput {
            priority_events,
            visual_events,
            wheel_events,
            deferred_visual: None,
            priority_closed: false,
            visual_closed: false,
            wheel_closed: false,
            pump: Some(pump),
        };

        assert!(matches!(
            input.next_event().await,
            Some(Ok(TerminalInputEvent {
                event: Event::Key(KeyEvent {
                    code: KeyCode::Char('x'),
                    ..
                }),
                ..
            }))
        ));
        assert!(matches!(
            input.next_event().await,
            Some(Ok(TerminalInputEvent {
                event: Event::Resize(20, 40),
                ..
            }))
        ));
    }

    #[tokio::test]
    async fn discrete_input_takes_priority_over_pending_wheel_motion() {
        let (priority_tx, priority_events) =
            tokio::sync::mpsc::channel(PRIORITY_EVENT_CHANNEL_CAPACITY);
        let (_visual_tx, visual_events) = tokio::sync::mpsc::channel(VISUAL_EVENT_CHANNEL_CAPACITY);
        let (wheel_tx, wheel_events) = tokio::sync::mpsc::channel(1);
        priority_tx
            .send(Ok(Event::Key(KeyEvent::new(
                KeyCode::Char('x'),
                KeyModifiers::NONE,
            ))))
            .await
            .unwrap();
        wheel_tx
            .send(WheelBurst {
                mouse: wheel_mouse(MouseEventKind::ScrollDown),
                repetitions: -1,
            })
            .await
            .unwrap();
        let pump = tokio::spawn(std::future::pending());
        let mut input = TerminalInput {
            priority_events,
            visual_events,
            wheel_events,
            deferred_visual: None,
            priority_closed: false,
            visual_closed: false,
            wheel_closed: false,
            pump: Some(pump),
        };

        assert!(matches!(
            input.next_event().await,
            Some(Ok(TerminalInputEvent {
                event: Event::Key(KeyEvent {
                    code: KeyCode::Char('x'),
                    ..
                }),
                ..
            }))
        ));
    }

    #[tokio::test]
    async fn explicit_shutdown_waits_for_the_event_pump_to_release_resources() {
        struct DropFlag(Arc<AtomicBool>);

        impl Drop for DropFlag {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Release);
            }
        }

        let released = Arc::new(AtomicBool::new(false));
        let guard = DropFlag(Arc::clone(&released));
        let pump = tokio::spawn(async move {
            let _guard = guard;
            std::future::pending::<()>().await;
        });
        tokio::task::yield_now().await;

        let (_priority_tx, priority_events) =
            tokio::sync::mpsc::channel(PRIORITY_EVENT_CHANNEL_CAPACITY);
        let (_visual_tx, visual_events) = tokio::sync::mpsc::channel(VISUAL_EVENT_CHANNEL_CAPACITY);
        let (_wheel_tx, wheel_events) = tokio::sync::mpsc::channel(1);
        let mut input = TerminalInput {
            priority_events,
            visual_events,
            wheel_events,
            deferred_visual: None,
            priority_closed: false,
            visual_closed: false,
            wheel_closed: false,
            pump: Some(pump),
        };

        input.shutdown().await;

        assert!(released.load(Ordering::Acquire));
        assert!(input.pump.is_none());
    }

    #[test]
    fn wheel_direction_change_discards_stale_momentum() {
        let mut pending = None;
        for _ in 0..MAX_PENDING_WHEEL_EVENTS {
            push_wheel(&mut pending, wheel_mouse(MouseEventKind::ScrollDown));
        }
        push_wheel(&mut pending, wheel_mouse(MouseEventKind::ScrollUp));

        let input: TerminalInputEvent = pending.expect("latest wheel direction").into();
        assert_eq!(input.scroll_repetitions, 1);
        assert!(matches!(
            input.event,
            Event::Mouse(MouseEvent {
                kind: MouseEventKind::ScrollUp,
                ..
            })
        ));
    }
}
