import assert from "node:assert/strict";
import test from "node:test";

import {
  classifyTurnResult,
  TurnMessageBoundary,
  type TurnResultContext,
} from "./turn_messages.js";

function context(
  overrides: Partial<TurnResultContext> = {},
): TurnResultContext {
  return {
    inputIds: new Set(["prompt-id", "steer-id"]),
    ...overrides,
  };
}

test("rejects a result correlated to SDK housekeeping on resume", () => {
  assert.equal(
    classifyTurnResult(
      {
        type: "result",
        subtype: "success",
        result: "No response requested.",
        user_message_uuid: "task-notification-id",
      },
      context(),
    ),
    "stale_correlated",
  );
});

test("accepts results for the initial prompt and a steer", () => {
  for (const inputId of ["prompt-id", "steer-id"]) {
    assert.equal(
      classifyTurnResult(
        {
          type: "result",
          subtype: "success",
          result: "done",
          user_message_uuid: inputId,
        },
        context(),
      ),
      "current",
    );
  }
});

test("accepts an empty result when the SDK omits correlation", () => {
  assert.equal(
    classifyTurnResult(
      { type: "result", subtype: "success", result: "" },
      context(),
    ),
    "current",
  );
});

test("does not hide legacy errors or non-empty results", () => {
  assert.equal(
    classifyTurnResult(
      { type: "result", subtype: "error_during_execution" },
      context(),
    ),
    "current",
  );
  assert.equal(
    classifyTurnResult(
      { type: "result", subtype: "success", result: "limit reached" },
      context(),
    ),
    "current",
  );
});

test("keeps a turn open until live background work produces its follow-up result", () => {
  const boundary = new TurnMessageBoundary();
  const turn = context();

  assert.equal(
    boundary.classify(
      {
        type: "system",
        subtype: "background_tasks_changed",
        tasks: [{ task_id: "task-1" }],
      },
      turn,
    ),
    "forward",
  );
  assert.deepEqual(boundary.backgroundTaskIds(), ["task-1"]);
  assert.equal(
    boundary.classify(
      {
        type: "result",
        subtype: "success",
        result: "foreground done",
        user_message_uuid: "prompt-id",
      },
      turn,
    ),
    "suppress",
  );
  assert.equal(
    boundary.classify(
      {
        type: "system",
        subtype: "background_tasks_changed",
        tasks: [],
      },
      turn,
    ),
    "forward",
  );
  assert.equal(
    boundary.classify(
      {
        type: "result",
        subtype: "success",
        result: "background follow-up done",
        user_message_uuid: "task-notification-id",
      },
      turn,
    ),
    "terminal",
  );
});

test("ordinary results remain immediate", () => {
  assert.equal(
    new TurnMessageBoundary().classify(
      {
        type: "result",
        subtype: "success",
        result: "done",
        user_message_uuid: "prompt-id",
      },
      context(),
    ),
    "terminal",
  );
});

test("background task levels use replacement semantics", () => {
  const boundary = new TurnMessageBoundary();
  const turn = context();
  boundary.classify(
    {
      type: "system",
      subtype: "background_tasks_changed",
      tasks: [{ task_id: "old" }, { task_id: "keep" }],
    },
    turn,
  );
  boundary.classify(
    {
      type: "system",
      subtype: "background_tasks_changed",
      tasks: [{ task_id: "keep" }],
    },
    turn,
  );
  assert.deepEqual(boundary.backgroundTaskIds(), ["keep"]);
});

test("a fast background completion cannot race the foreground result", () => {
  const boundary = new TurnMessageBoundary();
  const turn = context();
  boundary.classify(
    {
      type: "system",
      subtype: "background_tasks_changed",
      tasks: [{ task_id: "quick" }],
    },
    turn,
  );
  boundary.classify(
    {
      type: "system",
      subtype: "background_tasks_changed",
      tasks: [],
    },
    turn,
  );
  assert.equal(
    boundary.classify(
      {
        type: "result",
        subtype: "success",
        user_message_uuid: "prompt-id",
      },
      turn,
    ),
    "suppress",
  );
  assert.equal(
    boundary.classify(
      {
        type: "result",
        subtype: "success",
        user_message_uuid: "task-notification-id",
      },
      turn,
    ),
    "terminal",
  );
});

test("accepts an uncorrelated post-background result", () => {
  const boundary = new TurnMessageBoundary();
  const turn = context();
  boundary.classify(
    {
      type: "system",
      subtype: "background_tasks_changed",
      tasks: [{ task_id: "task" }],
    },
    turn,
  );
  boundary.classify(
    {
      type: "result",
      subtype: "success",
      user_message_uuid: "prompt-id",
    },
    turn,
  );
  boundary.classify(
    {
      type: "system",
      subtype: "background_tasks_changed",
      tasks: [],
    },
    turn,
  );
  assert.equal(
    boundary.classify(
      { type: "result", subtype: "success", result: "done" },
      turn,
    ),
    "terminal",
  );
});
