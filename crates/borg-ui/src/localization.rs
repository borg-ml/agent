use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum UiLanguage {
    #[default]
    Auto,
    English,
    SimplifiedChinese,
    Spanish,
}

impl UiLanguage {
    pub const ALL: [Self; 4] = [
        Self::Auto,
        Self::English,
        Self::SimplifiedChinese,
        Self::Spanish,
    ];

    pub const fn code(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::English => "en",
            Self::SimplifiedChinese => "zh-Hans",
            Self::Spanish => "es",
        }
    }

    pub const fn name(self) -> &'static str {
        match self {
            Self::Auto => "Automatic",
            Self::English => "English",
            Self::SimplifiedChinese => "简体中文",
            Self::Spanish => "Español",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "auto" | "system" => Some(Self::Auto),
            "en" | "english" => Some(Self::English),
            "zh" | "zh-cn" | "zh-hans" | "chinese" | "简体中文" => {
                Some(Self::SimplifiedChinese)
            }
            "es" | "spanish" | "español" => Some(Self::Spanish),
            _ => None,
        }
    }

    pub fn resolved(self) -> Self {
        if self != Self::Auto {
            return self;
        }
        let locale = ["LC_ALL", "LC_MESSAGES", "LANG"]
            .into_iter()
            .find_map(|name| std::env::var(name).ok())
            .unwrap_or_default()
            .to_ascii_lowercase();
        if locale.starts_with("zh") {
            Self::SimplifiedChinese
        } else if locale.starts_with("es") {
            Self::Spanish
        } else {
            Self::English
        }
    }
}

/// Translate presentation copy only. Command names, picker values, protocol
/// fields, and persisted strings stay language-neutral and stable.
pub fn text<'a>(language: UiLanguage, english: &'a str) -> &'a str {
    match (language.resolved(), english) {
        (UiLanguage::SimplifiedChinese, value) => chinese(value).unwrap_or(value),
        (UiLanguage::Spanish, value) => spanish(value).unwrap_or(value),
        _ => english,
    }
}

fn chinese(value: &str) -> Option<&'static str> {
    Some(match value {
        "Automatic" => "跟随系统",
        "English" => "英语",
        "Spanish" => "西班牙语",
        "What are we working on?" => "我们要做什么？",
        "Describe a task…" => "描述任务…",
        "Type a follow-up to redirect the current turn now…" => "输入后续消息以调整当前任务…",
        "Type a follow-up to send after the current turn…" => "输入将在当前任务后发送的消息…",
        "send" => "发送",
        "commands" => "命令",
        "palette menu" => "命令面板",
        "Jump to bottom" => "跳到底部",
        "Pending Input" => "待处理输入",
        "ready" => "就绪",
        "starting" => "正在启动",
        "running" => "运行中",
        "awaiting approval" => "等待批准",
        "stopped" => "已停止",
        "failed" => "失败",
        "main thread" => "主线程",
        "model pending" => "等待模型",
        "default" => "默认",
        "full access" => "完全访问",
        "manual" => "手动",
        "unknown" => "未知",
        "offline" => "离线",
        "standard" => "标准",
        "fast" => "快速",
        "model" => "模型",
        "effort" => "推理强度",
        "access" => "权限",
        "login" => "登录",
        "dismiss" => "关闭",
        "Response language" => "回复语言",
        "Response and drafting language" => "回复与起草语言",
        "UI language" => "界面语言",
        "Interface language" => "界面语言",
        "Agent" => "代理",
        "Agents" => "代理",
        "Tasks" => "任务",
        "Usage" => "用量",
        "AGENT" => "代理",
        "MODEL" => "模型",
        "EFFORT" => "强度",
        "STATE" => "状态",
        "USAGE" => "用量",
        "No local Borg session" => "没有本地 Borg 会话",
        "unconfigured" => "未配置",
        "Context" => "上下文",
        "Input tokens" => "输入词元",
        "Output tokens" => "输出词元",
        "Total tokens" => "总词元",
        "Provider time" => "提供商耗时",
        "Working directory" => "工作目录",
        "Permission" => "权限",
        "Provider" => "提供商",
        "Fast mode" => "快速模式",
        "Help" => "帮助",
        "Settings" => "设置",
        "New session" => "新建会话",
        "Open session" => "打开会话",
        "Back to director" => "返回主代理",
        "No subagents are running" => "没有正在运行的子代理",
        _ => return None,
    })
}

fn spanish(value: &str) -> Option<&'static str> {
    Some(match value {
        "Automatic" => "Automático",
        "English" => "Inglés",
        "Spanish" => "Español",
        "What are we working on?" => "¿En qué estamos trabajando?",
        "Describe a task…" => "Describe una tarea…",
        "Type a follow-up to redirect the current turn now…" => "Escribe un mensaje para redirigir el turno actual…",
        "Type a follow-up to send after the current turn…" => "Escribe un mensaje para enviar después del turno actual…",
        "send" => "enviar",
        "commands" => "comandos",
        "palette menu" => "menú de comandos",
        "Jump to bottom" => "Ir al final",
        "Pending Input" => "Entrada pendiente",
        "ready" => "listo",
        "starting" => "iniciando",
        "running" => "en curso",
        "awaiting approval" => "esperando aprobación",
        "stopped" => "detenido",
        "failed" => "fallido",
        "main thread" => "hilo principal",
        "model pending" => "modelo pendiente",
        "default" => "predeterminado",
        "full access" => "acceso total",
        "manual" => "manual",
        "unknown" => "desconocido",
        "offline" => "sin conexión",
        "standard" => "estándar",
        "fast" => "rápido",
        "model" => "modelo",
        "effort" => "esfuerzo",
        "access" => "acceso",
        "login" => "iniciar sesión",
        "dismiss" => "cerrar",
        "Response language" => "Idioma de respuesta",
        "Response and drafting language" => "Idioma de respuesta y redacción",
        "UI language" => "Idioma de la interfaz",
        "Interface language" => "Idioma de la interfaz",
        "Agent" => "Agente",
        "Agents" => "Agentes",
        "Tasks" => "Tareas",
        "Usage" => "Uso",
        "AGENT" => "AGENTE",
        "MODEL" => "MODELO",
        "EFFORT" => "ESFUERZO",
        "STATE" => "ESTADO",
        "USAGE" => "USO",
        "No local Borg session" => "No hay una sesión local de Borg",
        "unconfigured" => "sin configurar",
        "Context" => "Contexto",
        "Input tokens" => "Tokens de entrada",
        "Output tokens" => "Tokens de salida",
        "Total tokens" => "Tokens totales",
        "Provider time" => "Tiempo del proveedor",
        "Working directory" => "Directorio de trabajo",
        "Permission" => "Permiso",
        "Provider" => "Proveedor",
        "Fast mode" => "Modo rápido",
        "Help" => "Ayuda",
        "Settings" => "Ajustes",
        "New session" => "Nueva sesión",
        "Open session" => "Abrir sesión",
        "Back to director" => "Volver al director",
        "No subagents are running" => "No hay subagentes en ejecución",
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn language_codes_are_stable_and_translations_fall_back_to_english() {
        assert_eq!(UiLanguage::parse("zh-CN"), Some(UiLanguage::SimplifiedChinese));
        assert_eq!(UiLanguage::parse("español"), Some(UiLanguage::Spanish));
        assert_eq!(text(UiLanguage::SimplifiedChinese, "Settings"), "设置");
        assert_eq!(text(UiLanguage::SimplifiedChinese, "protocol-id"), "protocol-id");
    }
}
