# Multiline leading-cd persistence fix

Reproduced in the live Abundance session on 2026-09-29: `cd DIR` followed by a
newline and commands changes the child process directory, but the next exec
runs in the workspace root. In contrast, `cd DIR && COMMAND` updates the
remembered shell directory as intended.

Root cause: tool_presentation::split_leading_cd recognizes && and semicolon,
but consumes newline as whitespace and requires a subsequent separator/end.
AgentToolDispatcher::in_shell_directory therefore never records the target.
Fix: treat LF/CRLF as separators; use horizontal whitespace between cd and its
argument and before the separator so a directory on another line is not
mistaken for an argument. Same helper also renders command cwd in the UI.

Only crates/borg-agent-runtime/src/tool_presentation.rs modified. Existing
parser regression extended for the actual multiline failure and CRLF/whitespace
variants, plus no-argument cd followed by a new command. Rust LSP: 0 errors,
format/diff check pass. Targeted cargo regression: 1 passed; full shell-presentation suite: 40 passed, 0 failed. See multiline-cd-test.log and multiline-cd-presentation-tests.log.
Live process is not upgraded/restarted; use existing && tracking until a normal
rebuild/relaunch takes the source fix. No credentials or provider changes.
