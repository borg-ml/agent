export type TurnMessage = {
  type: string;
  subtype?: string;
  result?: unknown;
  user_message_uuid?: unknown;
};

export type TurnResultDisposition =
  | "not_result"
  | "current"
  | "stale_correlated";

export type TurnResultContext = {
  inputIds: ReadonlySet<string>;
};

export type TurnMessageAction = "forward" | "suppress" | "terminal";

/**
 * Decide whether an SDK result belongs to the Borg turn currently being read.
 *
 * A resumed Claude process can drain an internally queued notification before
 * the caller's newly pushed user message. The terminal result for that work is
 * not the terminal result for the new Borg turn.
 */
export function classifyTurnResult(
  message: TurnMessage,
  context: TurnResultContext,
): TurnResultDisposition {
  if (message.type !== "result") return "not_result";

  const inputId =
    typeof message.user_message_uuid === "string"
      ? message.user_message_uuid
      : undefined;
  if (inputId) {
    return context.inputIds.has(inputId) ? "current" : "stale_correlated";
  }

  // The correlation field is optional in the SDK contract. Forward an
  // uncorrelated result: suppressing it could wait forever for a second
  // terminal message that the SDK never promised to send.
  return "current";
}

/**
 * Keep one Borg turn open while Claude still owns live background work.
 *
 * Claude emits a foreground `result` after backgrounding a task, then later
 * injects the task notification back into the same query and emits another
 * result after handling it. Forwarding the first result makes Borg report
 * Ready while that work continues unseen in the pooled process. The SDK's
 * `background_tasks_changed` message is a level signal, so replacing the set
 * on every message also avoids wedging the turn if an edge notification was
 * missed.
 */
export class TurnMessageBoundary {
  private readonly liveBackgroundTasks = new Set<string>();
  private observedBackgroundWork = false;
  private awaitingPostBackgroundResult = false;

  classify(
    message: TurnMessage & { tasks?: unknown },
    context: TurnResultContext,
  ): TurnMessageAction {
    if (
      message.type === "system" &&
      message.subtype === "background_tasks_changed"
    ) {
      this.liveBackgroundTasks.clear();
      if (Array.isArray(message.tasks)) {
        for (const task of message.tasks) {
          if (
            typeof task === "object" &&
            task !== null &&
            "task_id" in task &&
            typeof task.task_id === "string"
          ) {
            this.liveBackgroundTasks.add(task.task_id);
          }
        }
      }
      if (this.liveBackgroundTasks.size > 0) {
        this.observedBackgroundWork = true;
      }
      return "forward";
    }

    const result = classifyTurnResult(message, context);
    if (
      result !== "not_result" &&
      this.awaitingPostBackgroundResult &&
      this.liveBackgroundTasks.size === 0
    ) {
      // Correlation is optional. Once this turn has deliberately withheld a
      // foreground result and the live-task level is empty, the next result is
      // the post-notification terminal regardless of whether the SDK labels
      // it with the internal notification UUID or omits that field.
      this.awaitingPostBackgroundResult = false;
      return "terminal";
    }
    if (result === "stale_correlated") {
      // Claude's follow-up after a background completion is driven by an
      // internal task-notification user message, so its result is correlated
      // to that notification rather than the original Borg prompt. Admit it
      // only after this turn itself observed and withheld a result while work
      // was live; unrelated resume housekeeping remains suppressed.
      return "suppress";
    }
    if (result !== "current") return "forward";

    if (this.observedBackgroundWork) {
      this.awaitingPostBackgroundResult = true;
      return "suppress";
    }

    // If an earlier result arrived while work was live, this is the result
    // produced after Claude consumed the completion notification.
    this.awaitingPostBackgroundResult = false;
    return "terminal";
  }

  backgroundTaskIds(): string[] {
    return [...this.liveBackgroundTasks];
  }
}
