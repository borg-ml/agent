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

## Primeros pasos

Ejecuta `borg` en el directorio de un proyecto y escribe una solicitud. Todo lo
demás ocurre dentro de la sesión: `/help` muestra los controles, **Ajustes ›
Proveedores** (o `/login`) conecta proveedores, `/model` cambia de modelo y
`/resume` retoma una sesión anterior. El acceso de Claude es propio de Borg,
separado del de Claude Code. La lista completa de proveedores está en el
[README en inglés](../../README.md#providers).

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
una TUI o GUI solo desconecta esa vista; no detiene una solicitud activa ni los
subagentes. Otra interfaz puede conectarse a la
misma sesión persistente.

Un anfitrión preparado, sin mensajes pendientes ni interfaces conectadas, se
cierra después de cinco minutos de inactividad. Al reanudar la sesión, Borg
vuelve a iniciarlo desde el diario persistente.

La documentación normativa completa está disponible en inglés:

- [ciclo de vida de sesiones](../session-lifecycle.md)
- [personalización del agente](../customization.md)
- [README completo](../../README.md)

## Privacidad

Borg no tiene telemetría: ni analíticas, ni recuentos de uso, ni señales de
actividad, ni informes de fallos. Nunca envía tus mensajes, código, rutas ni
datos de sesión a ningún sitio que no hayas elegido. Solo se conecta a los
proveedores y buscadores que configuras, a Borg Remote si lo activas, a GitHub
para buscar actualizaciones y a metadatos públicos de modelos.
