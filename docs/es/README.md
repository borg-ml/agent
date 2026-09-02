# Guía de Borg Agent en español

Borg Agent es un entorno de ejecución y orquestador de agentes escrito en Rust.
Incluye una interfaz de terminal, una GUI nativa, sesiones persistentes,
herramientas, varios proveedores de modelos y colaboración con subagentes.

## Instalación e inicio

Linux y macOS:

```sh
curl -fsSL https://borg.ml/install | sh
```

Windows PowerShell:

```powershell
irm https://borg.ml/install.ps1 | iex
```

Los comandos principales son `borg`, `borg resume`, `borg gui`,
`borg capabilities` y `borg extensions list`.

## Idioma de la interfaz

Abre el selector con `/ui-language` o elige español directamente:

```text
/ui-language es
```

La preferencia se guarda en `editor.toml`. El idioma de la interfaz es
independiente del idioma de respuesta: `/ui-language` traduce los elementos de
Borg, mientras que `/language` controla el idioma de las respuestas y
borradores del modelo. Los nombres de comandos, identificadores de modelos,
rutas, salidas originales de herramientas y contenido del usuario no se
traducen.

## Ciclo de vida de las sesiones

Cada sesión interactiva pertenece a un proceso anfitrión independiente. Cerrar
una TUI o GUI solo desconecta esa vista; no detiene una solicitud activa, el
servidor del proveedor ni los subagentes. Otra interfaz puede conectarse a la
misma sesión persistente.

Un anfitrión preparado, sin mensajes pendientes ni interfaces conectadas, se
cierra después de cinco minutos de inactividad. Al reanudar la sesión, Borg
vuelve a iniciarlo desde el diario persistente.

La documentación normativa completa está disponible en inglés:

- [ciclo de vida de sesiones](../session-lifecycle.md)
- [personalización del agente](../customization.md)
- [recuento privado de instalaciones activas](../usage-count.md)
- [README completo](../../README.md)

## Recuento mínimo de uso

Las versiones publicadas envían como máximo una señal de instalación activa al
día. El identificador aleatorio cambia cada 31 días y no permite seguir una
instalación entre períodos. No se envían versiones, sistema operativo, modelos,
sesiones, mensajes, rutas ni datos del dispositivo.

Se puede desactivar con `usage_count.enabled = false` en `agent.toml` o con
`BORG_DISABLE_USAGE_COUNT=1`.
