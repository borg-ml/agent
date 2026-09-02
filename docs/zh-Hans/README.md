# Borg Agent 简体中文指南

Borg Agent 是一个用 Rust 编写的智能代理运行环境和编排器，提供终端界面、
原生 GUI、持久会话、工具、多个模型提供商以及子代理协作。

## 安装与启动

Linux 和 macOS：

```sh
curl -fsSL https://borg.ml/install | sh
```

Windows PowerShell：

```powershell
irm https://borg.ml/install.ps1 | iex
```

常用命令：

```sh
borg                 # 启动交互会话
borg resume          # 恢复已有会话
borg gui             # 启动原生 GUI
borg capabilities    # 查看运行能力
borg extensions list # 查看扩展
```

## 语言设置

运行 `/ui-language` 选择界面语言，或直接输入 `/ui-language zh-Hans`。
设置会保存到 `~/.config/borg/editor.toml`（设置了 `XDG_CONFIG_HOME` 时位于
该目录下）。`/ui-language` 只改变 Borg 的界面标签；`/language` 独立控制模型
回复和起草所用的语言。命令名、模型 ID、路径、工具原始输出和用户内容不会被
翻译。

终端输入按字素簇处理中文、组合字符和 emoji；原生 GUI 支持 CJK 输入法的
UTF-16 组合文本范围。

## 会话不会随窗口退出

每个本地交互会话由一个独立的后台宿主进程拥有。关闭某个 TUI 或 GUI 只会
断开该界面，不会终止正在运行的模型调用、应用服务器或子代理。另一个界面
可以继续连接同一持久会话。

当会话处于就绪状态、没有待处理提示且没有任何界面连接时，宿主等待五分钟
后退出；之后恢复会话会从日志重新启动宿主。详见
[会话生命周期](session-lifecycle.md)。

## 配置

```sh
cp configs/agent.example.toml ~/.config/borg/agent.toml
cp configs/editor.example.toml ~/.config/borg/editor.toml
```

- `agent.toml`：提供商、能力、MCP、别名、按键和团队设置。
- `editor.toml`：界面语言、终端显示和交互偏好。

## 最小化的使用计数

发行版每天最多发送一次不含内容的活跃安装心跳。随机标识每 31 天更换，不能
跨周期追踪；不会发送版本、系统、模型、会话、提示、路径或设备信息。可在
`agent.toml` 中设置 `usage_count.enabled = false`，或设置环境变量
`BORG_DISABLE_USAGE_COUNT=1` 完全关闭。详细英文契约见
[`docs/usage-count.md`](../usage-count.md)。

完整英文参考文档位于 [`docs/`](../)。中文入口优先覆盖安装、语言、会话生命
周期和常用配置；协议与扩展 ABI 仍以英文原文为规范。

## 进一步阅读

- [会话生命周期](session-lifecycle.md)
- [自定义与语言](customization.md)
- [提供商与协作](providers-and-collaboration.md)
- [英文完整 README](../../README.md)
