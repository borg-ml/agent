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

## 快速开始

在项目目录中运行 `borg` 并输入请求，其余操作都在会话内完成：`/help` 列出
所有控制项，**设置 › 提供商**（或 `/login`）连接提供商，`/model` 切换模型，
`/resume` 恢复之前的会话。Borg 的 Claude 登录独立于 Claude Code。完整的提供商
列表见[英文 README](../../README.md#providers)。

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
断开该界面，不会终止正在运行的模型调用或子代理。另一个界面
可以继续连接同一持久会话。

当会话处于就绪状态、没有待处理提示且没有任何界面连接时，宿主等待五分钟
后退出；之后恢复会话会从日志重新启动宿主。详见
[会话生命周期](session-lifecycle.md)。

## 配置

在会话中打开 **设置** 或 `/customize` 修改配置。配置文件是
`~/.config/borg/` 下的 TOML，带注释的示例位于仓库的
[`configs/`](../../configs/) 目录。

- `agent.toml`：提供商、能力、MCP、别名、按键和团队设置。
- `editor.toml`：界面语言、终端显示和交互偏好。

## 隐私

Borg 没有任何遥测：没有分析、使用计数、心跳或崩溃上报。它绝不会把你的提示、
代码、路径或会话数据发送到你未选择的地方。Borg 只连接你配置的模型提供商和
搜索服务、你启用的 Borg Remote、用于检查更新的 GitHub，以及公开的模型元数据。

完整英文参考文档位于 [`docs/`](../)。中文入口优先覆盖安装、语言、会话生命
周期和常用配置；协议与扩展 ABI 仍以英文原文为规范。

## 进一步阅读

- [会话生命周期](session-lifecycle.md)
- [自定义与语言](customization.md)
- [提供商与协作](providers-and-collaboration.md)
- [英文完整 README](../../README.md)
