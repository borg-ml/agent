use std::io;
use std::time::Duration;

use crossterm::event::{Event, EventStream, MouseEvent, MouseEventKind};
use futures_util::{Stream, StreamExt};

const DISCRETE_EVENT_CHANNEL_CAPACITY: usize = 32;
const WHEEL_FLUSH_INTERVAL: Duration = Duration::from_millis(4);
const POINTER_MOVE_FLUSH_INTERVAL: Duration = Duration::from_millis(8);
const DRAG_FLUSH_INTERVAL: Duration = Duration::from_millis(8);
const RESIZE_FLUSH_INTERVAL: Duration = Duration::from_millis(33);
const MAX_PENDING_WHEEL_EVENTS: isize = 32;

pub(crate) struct TerminalInputEvent {
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
}

pub(super) struct TerminalInput {
    discrete_events: tokio::sync::mpsc::Receiver<io::Result<Event>>,
    wheel_events: tokio::sync::mpsc::Receiver<WheelBurst>,
    deferred_discrete: Option<io::Result<Event>>,
    pump: Option<tokio::task::JoinHandle<()>>,
}

impl TerminalInput {
    pub(super) fn spawn() -> Self {
        let (discrete_tx, discrete_events) =
            tokio::sync::mpsc::channel(DISCRETE_EVENT_CHANNEL_CAPACITY);
        let (wheel_tx, wheel_events) = tokio::sync::mpsc::channel(1);
        let pump = tokio::spawn(async move {
            pump_terminal_events(EventStream::new(), discrete_tx, wheel_tx).await;
        });
        Self {
            discrete_events,
            wheel_events,
            deferred_discrete: None,
            pump: Some(pump),
        }
    }

    pub(super) async fn next_event(&mut self) -> Option<io::Result<TerminalInputEvent>> {
        if let Some(event) = self.deferred_discrete.take() {
            return Some(
                self.coalesce_ready_resize(event)
                    .map(TerminalInputEvent::single),
            );
        }
        match self.discrete_events.try_recv() {
            Ok(event) => {
                return Some(
                    self.coalesce_ready_resize(event)
                        .map(TerminalInputEvent::single),
                );
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                return self.wheel_events.recv().await.map(|wheel| Ok(wheel.into()));
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
        }
        match self.wheel_events.try_recv() {
            Ok(wheel) => return Some(Ok(wheel.into())),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                let event = self.discrete_events.recv().await?;
                return Some(
                    self.coalesce_ready_resize(event)
                        .map(TerminalInputEvent::single),
                );
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
        }

        tokio::select! {
            biased;
            event = self.discrete_events.recv() => {
                match event {
                    Some(event) => {
                        Some(self.coalesce_ready_resize(event).map(TerminalInputEvent::single))
                    }
                    None => self.wheel_events.recv().await.map(|wheel| Ok(wheel.into())),
                }
            }
            wheel = self.wheel_events.recv() => {
                match wheel {
                    Some(wheel) => Some(Ok(wheel.into())),
                    None => {
                        let event = self.discrete_events.recv().await?;
                        Some(self.coalesce_ready_resize(event).map(TerminalInputEvent::single))
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
            match self.discrete_events.try_recv() {
                Ok(event @ Ok(Event::Resize(_, _))) => latest = event,
                Ok(event) => {
                    self.deferred_discrete = Some(event);
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
    discrete_tx: &tokio::sync::mpsc::Sender<io::Result<Event>>,
) -> bool {
    let Some(event) = pending.take() else {
        return true;
    };
    match discrete_tx.try_send(Ok(event)) {
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
    discrete_tx: &tokio::sync::mpsc::Sender<io::Result<Event>>,
) -> bool {
    flush_resize(pending, discrete_tx)
}

async fn pump_terminal_events<S>(
    mut events: S,
    discrete_tx: tokio::sync::mpsc::Sender<io::Result<Event>>,
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
                    if let Some(event) = pending_pointer_move.take() {
                        let _ = discrete_tx.send(Ok(event)).await;
                    }
                    if let Some(event) = pending_drag.take() {
                        let _ = discrete_tx.send(Ok(event)).await;
                    }
                    if let Some(event) = pending_resize.take() {
                        let _ = discrete_tx.send(Ok(event)).await;
                    }
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
                    if let Some(drag) = pending_drag.take()
                        && discrete_tx.send(Ok(drag)).await.is_err()
                    {
                        return;
                    }
                    pending_pointer_move = event.ok();
                } else if let Ok(Event::Mouse(mouse)) = &event
                    && matches!(mouse.kind, MouseEventKind::Drag(_))
                {
                    if let Some(pointer_move) = pending_pointer_move.take()
                        && discrete_tx.send(Ok(pointer_move)).await.is_err()
                    {
                        return;
                    }
                    pending_drag = event.ok();
                } else {
                    if let Some(pointer_move) = pending_pointer_move.take()
                        && discrete_tx.send(Ok(pointer_move)).await.is_err()
                    {
                        return;
                    }
                    if let Some(drag) = pending_drag.take()
                        && discrete_tx.send(Ok(drag)).await.is_err()
                    {
                        return;
                    }
                    if let Some(resize) = pending_resize.take()
                        && discrete_tx.send(Ok(resize)).await.is_err()
                    {
                        return;
                    }
                    if discrete_tx.send(event).await.is_err() {
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
                if !flush_drag(&mut pending_pointer_move, &discrete_tx) {
                    return;
                }
            }
            _ = drag_flush_tick.tick() => {
                if !flush_drag(&mut pending_drag, &discrete_tx) {
                    return;
                }
            }
            _ = resize_flush_tick.tick() => {
                if !flush_resize(&mut pending_resize, &discrete_tx) {
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

    use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers, MouseEvent, MouseEventKind};

    use super::*;

    fn wheel_mouse(kind: MouseEventKind) -> MouseEvent {
        MouseEvent {
            kind,
            column: 10,
            row: 10,
            modifiers: KeyModifiers::NONE,
        }
    }

    #[tokio::test]
    async fn wheel_flood_stays_out_of_the_discrete_input_queue() {
        let (discrete_tx, mut discrete_events) =
            tokio::sync::mpsc::channel(DISCRETE_EVENT_CHANNEL_CAPACITY);
        let (wheel_tx, mut wheel_events) = tokio::sync::mpsc::channel(1);
        let events = (0..100_000)
            .map(|_| Ok(Event::Mouse(wheel_mouse(MouseEventKind::ScrollDown))))
            .chain(std::iter::once(Ok(Event::Key(KeyEvent::new(
                KeyCode::Char('x'),
                KeyModifiers::NONE,
            )))));

        pump_terminal_events(futures_util::stream::iter(events), discrete_tx, wheel_tx).await;

        assert!(matches!(
            discrete_events.recv().await,
            Some(Ok(Event::Key(KeyEvent {
                code: KeyCode::Char('x'),
                ..
            })))
        ));
        assert!(discrete_events.recv().await.is_none());
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
    async fn pointer_motion_cannot_queue_a_wheel_gesture_behind_stale_coordinates() {
        let (discrete_tx, mut discrete_events) =
            tokio::sync::mpsc::channel(DISCRETE_EVENT_CHANNEL_CAPACITY);
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

        pump_terminal_events(futures_util::stream::iter(events), discrete_tx, wheel_tx).await;

        assert!(matches!(
            discrete_events.recv().await,
            Some(Ok(Event::Mouse(MouseEvent {
                kind: MouseEventKind::Moved,
                row: 10_000,
                ..
            })))
        ));
        assert!(discrete_events.recv().await.is_none());
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
        let (discrete_tx, mut discrete_events) =
            tokio::sync::mpsc::channel(DISCRETE_EVENT_CHANNEL_CAPACITY);
        let (wheel_tx, _wheel_events) = tokio::sync::mpsc::channel(1);
        let events = (1..=10_000)
            .map(|width| Ok(Event::Resize(width, 40)))
            .chain(std::iter::once(Ok(Event::Key(KeyEvent::new(
                KeyCode::Char('x'),
                KeyModifiers::NONE,
            )))));

        pump_terminal_events(futures_util::stream::iter(events), discrete_tx, wheel_tx).await;

        assert!(matches!(
            discrete_events.recv().await,
            Some(Ok(Event::Resize(10_000, 40)))
        ));
        assert!(matches!(
            discrete_events.recv().await,
            Some(Ok(Event::Key(KeyEvent {
                code: KeyCode::Char('x'),
                ..
            })))
        ));
        assert!(discrete_events.recv().await.is_none());
    }

    #[tokio::test]
    async fn drag_burst_keeps_the_latest_pointer_position_before_release() {
        let (discrete_tx, mut discrete_events) =
            tokio::sync::mpsc::channel(DISCRETE_EVENT_CHANNEL_CAPACITY);
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

        pump_terminal_events(futures_util::stream::iter(events), discrete_tx, wheel_tx).await;

        let events = std::iter::from_fn(|| discrete_events.try_recv().ok()).collect::<Vec<_>>();
        assert!(events.len() <= 2);
        assert!(matches!(
            events.as_slice(),
            [
                Ok(Event::Mouse(MouseEvent {
                    kind: MouseEventKind::Drag(_),
                    row: 10_000,
                    ..
                })),
                Ok(Event::Mouse(MouseEvent {
                    kind: MouseEventKind::Up(_),
                    row: 10_000,
                    ..
                }))
            ]
        ));
    }

    #[tokio::test]
    async fn queued_resize_frames_collapse_before_the_renderer_sees_them() {
        let (discrete_tx, discrete_events) =
            tokio::sync::mpsc::channel(DISCRETE_EVENT_CHANNEL_CAPACITY);
        let (_wheel_tx, wheel_events) = tokio::sync::mpsc::channel(1);
        for width in 1..=20 {
            discrete_tx
                .send(Ok(Event::Resize(width, 40)))
                .await
                .unwrap();
        }
        discrete_tx
            .send(Ok(Event::Key(KeyEvent::new(
                KeyCode::Char('x'),
                KeyModifiers::NONE,
            ))))
            .await
            .unwrap();
        drop(discrete_tx);
        let pump = tokio::spawn(std::future::pending());
        let mut input = TerminalInput {
            discrete_events,
            wheel_events,
            deferred_discrete: None,
            pump: Some(pump),
        };

        assert!(matches!(
            input.next_event().await,
            Some(Ok(TerminalInputEvent {
                event: Event::Resize(20, 40),
                ..
            }))
        ));
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
    async fn discrete_input_takes_priority_over_pending_wheel_motion() {
        let (discrete_tx, discrete_events) =
            tokio::sync::mpsc::channel(DISCRETE_EVENT_CHANNEL_CAPACITY);
        let (wheel_tx, wheel_events) = tokio::sync::mpsc::channel(1);
        discrete_tx
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
            discrete_events,
            wheel_events,
            deferred_discrete: None,
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

        let (_discrete_tx, discrete_events) =
            tokio::sync::mpsc::channel(DISCRETE_EVENT_CHANNEL_CAPACITY);
        let (_wheel_tx, wheel_events) = tokio::sync::mpsc::channel(1);
        let mut input = TerminalInput {
            discrete_events,
            wheel_events,
            deferred_discrete: None,
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
