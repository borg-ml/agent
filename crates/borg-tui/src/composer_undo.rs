//! Undo/redo history for the composer.
//!
//! The history is checkpoint based rather than operation based: the composer
//! has many mutation paths (typing, word and line deletion, selection edits,
//! pastes resolved asynchronously, dictation, prompts returned after a failed
//! send, recalled history), and instrumenting each one would rot as soon as a
//! new path is added. Instead the frontend calls [`UndoHistory::checkpoint`]
//! at input boundaries with the kind of edit that just happened. Any
//! difference from the last committed state becomes one undo step, and
//! consecutive typing or deleting is coalesced so Ctrl+Z removes a word or a
//! deletion run, not a single character.

use std::time::{Duration, Instant};

/// Most undo steps retained; older steps are dropped first.
const MAX_UNDO_STEPS: usize = 200;
/// Typing separated by a longer pause starts a new undo step.
const COALESCE_WINDOW: Duration = Duration::from_secs(2);

/// What produced the change being checkpointed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EditKind {
    /// A printable non-whitespace character.
    Typing,
    /// Typed whitespace. It joins the word before it, so the next word
    /// starts a new step.
    WordBreak,
    /// Plain Backspace or Delete.
    Deleting,
    /// Anything else: pastes, word or line deletion, restores, clears.
    Discrete,
}

impl EditKind {
    fn continues(self, next: Self) -> bool {
        matches!(
            (self, next),
            (Self::Typing, Self::Typing | Self::WordBreak)
                | (Self::WordBreak, Self::WordBreak)
                | (Self::Deleting, Self::Deleting)
        )
    }
}

#[derive(Debug)]
pub(crate) struct UndoHistory<S> {
    /// The last committed state: what the current state is diffed against.
    baseline: S,
    undo: Vec<S>,
    redo: Vec<S>,
    last: Option<(EditKind, Instant)>,
}

impl<S: Default> Default for UndoHistory<S> {
    fn default() -> Self {
        Self {
            baseline: S::default(),
            undo: Vec::new(),
            redo: Vec::new(),
            last: None,
        }
    }
}

impl<S: Clone + PartialEq> UndoHistory<S> {
    pub(crate) fn baseline(&self) -> &S {
        &self.baseline
    }

    /// Commit `current` if it differs from the last committed state.
    pub(crate) fn checkpoint(&mut self, current: &S, kind: EditKind, now: Instant) {
        if *current == self.baseline {
            return;
        }
        let coalesce = !self.undo.is_empty()
            && self.last.is_some_and(|(last, at)| {
                last.continues(kind) && now.saturating_duration_since(at) <= COALESCE_WINDOW
            });
        let previous = std::mem::replace(&mut self.baseline, current.clone());
        if !coalesce {
            self.undo.push(previous);
            if self.undo.len() > MAX_UNDO_STEPS {
                self.undo.remove(0);
            }
        }
        self.redo.clear();
        self.last = Some((kind, now));
    }

    /// Return the state before the latest step, if any.
    pub(crate) fn undo(&mut self, current: &S, now: Instant) -> Option<S> {
        self.checkpoint(current, EditKind::Discrete, now);
        let previous = self.undo.pop()?;
        self.redo
            .push(std::mem::replace(&mut self.baseline, previous.clone()));
        self.last = None;
        Some(previous)
    }

    /// Reapply the latest undone step, if any.
    pub(crate) fn redo(&mut self, current: &S, now: Instant) -> Option<S> {
        if *current != self.baseline {
            // A new edit since the undo invalidates the redo branch.
            self.checkpoint(current, EditKind::Discrete, now);
            return None;
        }
        let next = self.redo.pop()?;
        self.undo
            .push(std::mem::replace(&mut self.baseline, next.clone()));
        self.last = None;
        Some(next)
    }

    /// Forget all steps, e.g. after the composer was submitted.
    pub(crate) fn reset(&mut self, state: S) {
        self.baseline = state;
        self.undo.clear();
        self.redo.clear();
        self.last = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn typed(history: &mut UndoHistory<String>, text: &mut String, value: &str, at: Instant) {
        for character in value.chars() {
            text.push(character);
            let kind = if character.is_whitespace() {
                EditKind::WordBreak
            } else {
                EditKind::Typing
            };
            history.checkpoint(text, kind, at);
        }
    }

    #[test]
    fn typing_coalesces_by_word_and_redo_reapplies() {
        let now = Instant::now();
        let mut history = UndoHistory::<String>::default();
        let mut text = String::new();
        typed(&mut history, &mut text, "hello world", now);
        assert_eq!(history.undo(&text, now).as_deref(), Some("hello "));
        assert_eq!(history.undo(&"hello ".into(), now).as_deref(), Some(""));
        assert_eq!(history.undo(&String::new(), now), None);
        assert_eq!(history.redo(&String::new(), now).as_deref(), Some("hello "));
        assert_eq!(
            history.redo(&"hello ".into(), now).as_deref(),
            Some("hello world")
        );
        assert_eq!(history.redo(&"hello world".into(), now), None);
    }

    #[test]
    fn pause_discrete_edits_and_new_edits_after_undo() {
        let now = Instant::now();
        let mut history = UndoHistory::<String>::default();
        let mut text = String::new();
        typed(&mut history, &mut text, "ab", now);
        typed(&mut history, &mut text, "cd", now + Duration::from_secs(5));
        history.checkpoint(&"abcd[Image 1]".into(), EditKind::Discrete, now);
        history.checkpoint(&String::new(), EditKind::Discrete, now);
        assert_eq!(
            history.undo(&String::new(), now).as_deref(),
            Some("abcd[Image 1]")
        );
        assert_eq!(
            history.undo(&"abcd[Image 1]".into(), now).as_deref(),
            Some("abcd")
        );
        assert_eq!(history.undo(&"abcd".into(), now).as_deref(), Some("ab"));
        // Typing after an undo discards the redo branch.
        assert_eq!(history.redo(&"abX".into(), now), None);
        assert_eq!(history.redo(&"abX".into(), now), None);
        assert_eq!(history.undo(&"abX".into(), now).as_deref(), Some("ab"));
    }

    #[test]
    fn uncommitted_changes_are_undone_first_and_reset_forgets() {
        let now = Instant::now();
        let mut history = UndoHistory::<String>::default();
        history.checkpoint(&"one".into(), EditKind::Discrete, now);
        assert_eq!(history.undo(&"one two".into(), now).as_deref(), Some("one"));
        history.reset(String::new());
        assert_eq!(history.undo(&String::new(), now), None);
        assert_eq!(history.baseline(), "");
    }
}
