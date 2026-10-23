# GPT steering latency comparison

Local comparison on 2026-10-23: Borg 0.19.1 versus Codex CLI 0.163.0,
`gpt-6.1-sol`, low reasoning effort, standard service tier, the same ChatGPT
subscription account, and an otherwise empty workspace. No API-key fallback.

The Borg runner used a PTY for interactive stdin and plain streaming stdout;
`/steer` reaches the same session/harness steering path as the rich terminal.
The Codex runner used its app-server `turn/steer` API, the route used by its
interactive client. These measurements exclude keyboard handling and painting.

Each pair ran concurrently. A monotonic clock measured from submitting the
steer until the complete redirected `STEERING_ACK` text arrived, not from
process startup or the start timestamp of a reasoning snapshot. Different
harness instructions and tool catalogs remain; these are product-path tests,
not identical provider request bodies. Network and model timings vary.

## Before the fix

| Scenario | Borg | Codex CLI |
| --- | ---: | ---: |
| Streaming prose, trial 1 | 1.79 s | 1.62 s |
| Streaming prose, trial 2 | 1.55 s | 1.55 s |
| Streaming prose, trial 3 | 1.52 s | 1.50 s |
| Running 12-second shell command | 13.70 s | 1.93 s |
| Streaming prose with larger reference input | 4.03 s | 2.94 s |

The large difference reproduced during a running blocking shell action. Borg
captured the steer but deliberately waited for an effectful action to finish
before folding the message into the next model request. This prevented lost
effects but also made reply latency include the whole remaining command wait.

The fix uses the existing tracked background-action mechanism: human or
preemptive steering yields the foreground wait without killing the execution.
The tool result identifies the still-running job; its final result remains
available through the existing watcher tools. Interrupt still cancels.
Recalled and non-preemptive background messages do not force this yield.

## Matched active-command confirmation

A second set of three pairs waited 500 ms after observing command start before
submitting the steer, ensuring that execution was already in progress.

| Build | Borg trials | Codex CLI trials |
| --- | --- | --- |
| Before | 13.04 s, 13.88 s, 11.80 s | 1.62 s, 2.47 s, 1.47 s |
| Fixed | 2.01 s, 1.74 s, 1.99 s | 1.68 s, 1.75 s, 2.10 s |

Borg's median fell from 13.04 s to 1.99 s in these trials. The preserved-command
and final-result contract is additionally checked by the runtime regression
`steering_backgrounds_effectful_actions_without_cancelling_them`; live trial
processes were stopped after acknowledgement rather than used to measure job
completion. These are local samples, not latency guarantees.

## Reproduction

Start fresh sessions at the settings above, without unrelated project work.
For the streaming case, request 500 numbered lines about trees beginning
with `ROW` and send the steer after the first numbered line arrives.

For the active-tool case, use this initial prompt:

> Read-only steering latency test. Your only task is to run a shell command
> that waits twelve seconds and prints ORIGINAL_DONE, then reply
> ORIGINAL_COMPLETE. Execute python3 -c "import time; time.sleep(12);
> print(12345)" now. Do not inspect or modify files.

Send this as soon as command execution starts, without interrupting first:

> Stop the previous task immediately. Do not use tools. Reply with exactly
> STEERING_ACK.

For the larger-input case, prefix the streaming prompt with 1,400 distinct
numbered rows of inert tree reference data. That trial is not a reproduction
of an arbitrary long-running session or its full context/cache state.
