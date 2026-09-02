# 提供商与协作

Borg 支持 Codex、Claude、OpenCode、Kimi、OpenRouter 和已配置的
OpenAI-compatible 提供商。实际可用状态取决于本机安装与认证；运行
`borg capabilities` 查看不会泄露密钥的状态摘要。

子代理和持久 peer 属于同一持久会话。团队菜单显示每个代理的产品模型名称
（例如 `Opus 5`）、推理强度、状态以及累计 token 用量。用量列已经表示 token，
因此不会在每行重复显示 `ctx`。上下文窗口剩余量是另一项独立指标。

关闭一个界面不会关闭提供商应用服务器或子代理；它们由会话宿主管理。有关
连接、恢复和空闲退出，请阅读[会话生命周期](session-lifecycle.md)。英文规范文档：

- [提供商对等性](../provider-parity.md)
- [Claude/Codex 对等性](../claude-codex-provider-parity.md)
- [多人工作区](../multiplayer-workspaces.md)
- [协作可靠性](../collaboration-acp-reliability.md)

[返回中文指南](README.md)
