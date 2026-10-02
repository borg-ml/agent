# Per-child fast mode

The director can request `configure_agent({"target":"/root/worker","fast":true})`.
Use `false` to disable it; omitting `fast` retains that child's mode. Provider,
model, effort, session identity, conversation, goals and queued tools are retained
unless explicitly changed. Combined settings are admitted atomically: an
unsupported or unconfirmed fast request leaves the previous configuration intact.

Fast requires the selected model's subscription speed metadata. Codex sends
`service_tier: "priority"`, never switches models or reduces effort, and refuses
fast requests on API-key billing. Priority may consume additional subscription
quota; model support alone does not establish its quota multiplier or price.
Check the account's terms and obtain approval before enabling it on live workers.

A native worker finishes the model request and tool batch already in flight,
then reads the durable speed setting at the next model boundary. Other settings
apply next turn. Configuration never wakes a paused/stopped worker: resume it
explicitly first. New children inherit live speed only on the same provider/model
route and revalidate fast support before reservation. Idle-worker reuse respects
fast mode; resumed children retain their own saved mode.

The TUI subagent menu has a compact `Mode` column: ordinary mode is blank,
`fast` is shown for fast mode, and `ultrafast` takes precedence when enabled.
The column hides first on narrow terminals, preserving the existing roster columns.
These labels reflect requested configuration, not proof the server accepted
priority or that latency improved. Inspect actual provider request/response
and usage evidence separately. A source update does not hot-replace a running
host's compiled schemas; existing processes need a planned upgrade. Do not restart
an active director or workers merely to expose this flag.
