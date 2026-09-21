//! Child agents as a watcher subject kind.
//!
//! A watch is one concept with two subject kinds: a command, which the process
//! manager reports on, and a child agent, whose lifecycle the runtime already
//! records durably. This module holds the agent kind's vocabulary and its
//! aggregation rule; `watch::Watches` owns the entries, the notification path
//! and the journaling both kinds share.

use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;
use uuid::Uuid;

/// What the session last knew about one child, derived from its recorded
/// lifecycle rather than from an edge that may have been missed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SubjectLife {
    /// Starting, running, or working a tool call.
    Live,
    /// Alive but not working for the parent: idle after finishing one
    /// assignment, or blocked on an approval only the parent can give.
    Parked,
    /// Stopped or failed. Terminal, whatever the provider did.
    Exited,
}

impl SubjectLife {
    /// How a notification names this life.
    pub fn describe(self) -> &'static str {
        match self {
            Self::Live => "running",
            Self::Parked => "waiting on the parent",
            Self::Exited => "exited",
        }
    }
}

/// The life a subject has to reach to settle an agent watch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AgentSignal {
    /// The child is gone. Terminal and provider-independent: a child that fails
    /// exits, and a child whose status means nothing to this session still
    /// exits.
    Exit,
    /// The child is alive and waiting on the parent: the one case exit can
    /// never report, and the reason a watch would otherwise wait forever.
    Attention,
}

impl AgentSignal {
    fn matches(self, life: SubjectLife) -> bool {
        match self {
            Self::Exit => life == SubjectLife::Exited,
            Self::Attention => life == SubjectLife::Parked,
        }
    }
}

/// How many subjects have to signal before the watch speaks.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Aggregate {
    /// Every subject signalled. The default: spawn N children, integrate once
    /// all of them are done.
    #[default]
    All,
    /// The first subject to signal.
    Any,
}

/// One subject's last known life, with the name a notification calls it.
#[derive(Clone, Debug)]
pub(crate) struct SubjectRecord {
    pub life: SubjectLife,
    pub name: String,
}

/// The subjects of one agent watch, and the rule that settles it.
pub(crate) struct AgentSubjects {
    subjects: Vec<Uuid>,
    signal: AgentSignal,
    aggregate: Aggregate,
}

impl AgentSubjects {
    pub fn new(subjects: Vec<Uuid>, signal: AgentSignal, aggregate: Aggregate) -> Self {
        // Named twice is still one child: `all` has to mean every distinct
        // subject, and the notification names each of them once.
        let mut seen = BTreeSet::new();
        let subjects = subjects.into_iter().filter(|id| seen.insert(*id)).collect();
        Self {
            subjects,
            signal,
            aggregate,
        }
    }

    pub fn subjects(&self) -> &[Uuid] {
        &self.subjects
    }

    /// The subjects that settle the watch, in the order they were named, once
    /// the aggregate asks for them. `None` means it keeps waiting: a subject the
    /// session has never been told about is unknown rather than exited, and
    /// guessing would wake the session before the child was ever terminal.
    ///
    /// Every subject being gone settles the watch whatever its signal asks for.
    /// Nothing can make a dead child signal, so such a watch can never fire
    /// again, and a watch left running holds a yielded session on a wake that
    /// cannot arrive.
    pub fn settle(
        &self,
        states: &BTreeMap<Uuid, SubjectRecord>,
    ) -> Option<Vec<(Uuid, SubjectLife)>> {
        let known: Vec<(Uuid, SubjectLife)> = self
            .subjects
            .iter()
            .filter_map(|id| states.get(id).map(|record| (*id, record.life)))
            .collect();
        let signalled: Vec<(Uuid, SubjectLife)> = known
            .iter()
            .copied()
            .filter(|(_, life)| self.signal.matches(*life))
            .collect();
        let settled = match self.aggregate {
            Aggregate::All => signalled.len() == self.subjects.len(),
            Aggregate::Any => !signalled.is_empty(),
        };
        if settled {
            return Some(signalled);
        }
        let gone = known.len() == self.subjects.len()
            && known.iter().all(|(_, life)| *life == SubjectLife::Exited);
        gone.then_some(known)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn states(entries: &[(Uuid, SubjectLife)]) -> BTreeMap<Uuid, SubjectRecord> {
        entries
            .iter()
            .map(|(id, life)| {
                (
                    *id,
                    SubjectRecord {
                        life: *life,
                        name: id.to_string(),
                    },
                )
            })
            .collect()
    }

    #[test]
    fn all_waits_for_every_subject_and_any_settles_on_the_first() {
        let (first, second) = (Uuid::new_v4(), Uuid::new_v4());
        let all = AgentSubjects::new(vec![first, second], AgentSignal::Exit, Aggregate::All);
        assert!(
            all.settle(&states(&[(first, SubjectLife::Exited)]))
                .is_none()
        );
        assert_eq!(
            all.settle(&states(&[
                (first, SubjectLife::Exited),
                (second, SubjectLife::Exited)
            ]))
            .expect("every subject exited")
            .len(),
            2
        );

        let any = AgentSubjects::new(vec![first, second], AgentSignal::Exit, Aggregate::Any);
        assert_eq!(
            any.settle(&states(&[(second, SubjectLife::Exited)])),
            Some(vec![(second, SubjectLife::Exited)])
        );
    }

    #[test]
    fn attention_settles_on_a_parked_child_that_exit_would_not() {
        let child = Uuid::new_v4();
        let parked = states(&[(child, SubjectLife::Parked)]);
        assert!(
            AgentSubjects::new(vec![child], AgentSignal::Exit, Aggregate::All)
                .settle(&parked)
                .is_none(),
            "a child waiting on the parent has not exited"
        );
        assert!(
            AgentSubjects::new(vec![child], AgentSignal::Attention, Aggregate::All)
                .settle(&parked)
                .is_some()
        );
        assert!(
            AgentSubjects::new(vec![child], AgentSignal::Attention, Aggregate::All)
                .settle(&states(&[(child, SubjectLife::Live)]))
                .is_none(),
            "a child still working needs nothing from the parent"
        );
    }

    /// A watch whose subjects are all gone settles even when it asked for
    /// attention, because a dead child can never signal again; a subject that is
    /// still working keeps it waiting, so a live batch is not woken early.
    #[test]
    fn a_watch_whose_subjects_are_all_gone_settles_even_for_attention() {
        let (first, second) = (Uuid::new_v4(), Uuid::new_v4());
        let alone = AgentSubjects::new(vec![first], AgentSignal::Attention, Aggregate::All);
        assert_eq!(
            alone.settle(&states(&[(first, SubjectLife::Exited)])),
            Some(vec![(first, SubjectLife::Exited)])
        );

        let pair = AgentSubjects::new(vec![first, second], AgentSignal::Attention, Aggregate::All);
        assert!(
            pair.settle(&states(&[
                (first, SubjectLife::Exited),
                (second, SubjectLife::Live)
            ]))
            .is_none(),
            "a child still working can still signal"
        );
    }

    #[test]
    fn an_unknown_subject_does_not_settle_and_a_duplicate_is_one_subject() {
        let child = Uuid::new_v4();
        let watch = AgentSubjects::new(vec![child], AgentSignal::Exit, Aggregate::All);
        assert!(watch.settle(&BTreeMap::new()).is_none());

        let twice = AgentSubjects::new(vec![child, child], AgentSignal::Exit, Aggregate::All);
        assert_eq!(twice.subjects(), [child]);
        assert_eq!(
            twice
                .settle(&states(&[(child, SubjectLife::Exited)]))
                .expect("the one distinct child exited")
                .len(),
            1
        );
    }
}
