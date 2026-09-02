# 自定义与语言

Borg 的用户配置通常位于 `~/.config/borg`：

- `editor.toml` 管理界面语言、终端显示和交互偏好；
- `agent.toml` 管理提供商、能力、MCP、命令别名、按键和扩展权限；
- Blu 软件包可以增加技能、工具、工作流和扩展命令。

在交互界面中运行 `/settings` 可打开设置选择器。界面语言可用
`/ui-language` 设置，支持 `auto`、`en`、`zh-Hans`、`es` 和 `ru`：

```toml
[presentation]
ui_language = "zh-Hans"
refresh_rate_fps = 60
```

`auto` 依次读取 `LC_ALL`、`LC_MESSAGES` 和 `LANG`，无法识别时使用英语。
界面语言是本机偏好，不写入会话事件；模型回复语言由 `/language` 单独设置。

其他常用设置包括 `/followups`、`/sleep`、`/notifications`、`/sound`、
`/refresh`、`/expand-edits`、`/expand-tools`、`/icons`、`/colors` 和
`/user-label`。详细字段和扩展权限规则请以
[英文自定义文档](../customization.md)为准。

[返回中文指南](README.md)
