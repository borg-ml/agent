# Scout subagents

`scout(task_name, message)` is a first-class Borg tool for bounded exploration.
It starts a fresh child and returns its identity immediately. Use `wait_agent`
and messaging to collect findings, or `inspect_agent` for an explicit bounded
inspection. The scout owns its context window: its transcript and tool results
are not dumped into the calling model's context. It is instructed to report
concise findings with paths and evidence, not edit files or delegate further;
this is a task brief, not a read-only security sandbox.

Configure the next scout call in the user `agent.toml` (also exposed through
`get_agent_settings` / `update_agent_settings`):

```toml
[scout]
model = "claude-haiku-5-5@xhigh"
fallback = ["gpt-6-luna@xhigh"]
allow_api_billing = false
```

Model routes use the same `model@effort` or `provider/model@effort` syntax as
[model fallback](model-fallback.md). Change either effort suffix to change its
reasoning effort, reorder/replace the fallback list, or set it to `[]` to disable
fallback. Settings apply on the next call; the main session's configuration is
unchanged. Defaults are shown above. Scout selects the first admissible route
and carries the configured chain into the child for usage-limit recovery.
No route spends API credit unless `allow_api_billing` is explicitly enabled.
Normal subagent permission, stop, concurrency and admission rules still apply.
