//! Robust context ownership and a sticky, pre-render reset gate.
use anyhow::{Context, Result, bail, ensure};
use smithay::backend::egl::{EGLContext, EGLDisplay, ffi::egl};
use smithay::backend::renderer::gles::GlesRenderer;

pub(crate) const LOST: &str =
    "GPU reset: renderer lost; client GPU buffers may also be lost; stop_display and relaunch apps";

// EGLContext::from_raw is externally managed. Keep the display alive and destroy
// the raw context only after the renderer has been dropped (State field order).
pub(crate) struct ContextOwner {
    display: EGLDisplay,
    raw: egl::types::EGLContext,
}
impl Drop for ContextOwner {
    fn drop(&mut self) {
        unsafe {
            egl::DestroyContext(**self.display.get_display_handle(), self.raw);
        }
    }
}

pub(crate) fn renderer(display: EGLDisplay) -> Result<(GlesRenderer, Option<ContextOwner>)> {
    if !display
        .extensions()
        .iter()
        .any(|e| e == "EGL_EXT_create_context_robustness")
    {
        // Retain compatibility, but expose that reset detection is unavailable.
        let context = EGLContext::new(&display)?;
        return Ok((unsafe { GlesRenderer::new(context)? }, None));
    }
    // EGLDevice's Smithay surface type is STREAM_BIT_KHR. Mesa headless
    // devices need a config with no window/stream surface requirement instead.
    let config_attributes = [
        egl::SURFACE_TYPE as i32,
        0,
        egl::RENDERABLE_TYPE as i32,
        egl::OPENGL_ES2_BIT as i32,
        egl::NONE as i32,
    ];
    let mut config = std::ptr::null();
    let mut count = 0;
    ensure!(
        unsafe {
            egl::ChooseConfig(
                **display.get_display_handle(),
                config_attributes.as_ptr(),
                &mut config,
                1,
                &mut count,
            )
        } == egl::TRUE
            && count > 0,
        "choosing robust headless EGL config failed"
    );
    let attributes = [
        egl::CONTEXT_CLIENT_VERSION as i32,
        2,
        egl::CONTEXT_OPENGL_ROBUST_ACCESS_EXT as i32,
        egl::TRUE as i32,
        egl::CONTEXT_OPENGL_RESET_NOTIFICATION_STRATEGY_EXT as i32,
        egl::LOSE_CONTEXT_ON_RESET_EXT as i32,
        egl::NONE as i32,
    ];
    let raw = unsafe {
        egl::CreateContext(
            **display.get_display_handle(),
            config,
            egl::NO_CONTEXT,
            attributes.as_ptr(),
        )
    };
    ensure!(
        raw != egl::NO_CONTEXT,
        "creating robust EGL context failed: {:#x}",
        unsafe { egl::GetError() }
    );
    let owner = ContextOwner { display, raw };
    let context =
        unsafe { EGLContext::from_raw(**owner.display.get_display_handle(), config, raw) }
            .context("wrapping robust EGL context")?;
    Ok((unsafe { GlesRenderer::new(context)? }, Some(owner)))
}

#[derive(Default)]
pub(crate) struct ResetGate {
    pub(crate) lost: bool,
}
impl ResetGate {
    pub(crate) fn check(&mut self, probe: impl FnOnce() -> Result<u32>) -> Result<()> {
        if self.lost {
            bail!(LOST);
        }
        let status = probe()?;
        if status != 0 {
            self.lost = true;
            eprintln!("borg-display: {LOST} (reset status {status:#x})");
            bail!(LOST);
        }
        Ok(())
    }
}

pub(crate) type ResetStatus = unsafe extern "system" fn() -> u32;

pub(crate) fn reset_probe(renderer: &mut GlesRenderer) -> Result<Option<ResetStatus>> {
    // Smithay's generated GL bindings contain the ES 3.2 core entry point,
    // but not its KHR/EXT aliases; resolve the extension on this current context.
    renderer
        .with_context(|gl| unsafe {
            let extensions = gl.GetString(smithay::backend::renderer::gles::ffi::EXTENSIONS);
            if extensions.is_null() {
                return None;
            }
            let extensions = std::ffi::CStr::from_ptr(extensions.cast()).to_string_lossy();
            let symbol = if extensions
                .split_whitespace()
                .any(|e| e == "GL_KHR_robustness")
            {
                "glGetGraphicsResetStatusKHR"
            } else if extensions
                .split_whitespace()
                .any(|e| e == "GL_EXT_robustness")
            {
                "glGetGraphicsResetStatusEXT"
            } else {
                return None;
            };
            let address = smithay::backend::egl::get_proc_address(symbol);
            if address.is_null() {
                return None;
            }
            Some(std::mem::transmute::<*const std::ffi::c_void, ResetStatus>(
                address,
            ))
        })
        .context("loading renderer reset status")
}

pub(crate) fn reset_status(renderer: &mut GlesRenderer, probe: Option<ResetStatus>) -> Result<u32> {
    renderer
        .with_context(|_| probe.map(|status| unsafe { status() }).unwrap_or(0))
        .or_else(|error| {
            // EGL_CONTEXT_LOST can prevent making the context current at all.
            use std::error::Error;
            let mut source: Option<&dyn Error> = Some(&error);
            while let Some(error) = source {
                if matches!(
                    error.downcast_ref::<smithay::backend::egl::EGLError>(),
                    Some(smithay::backend::egl::EGLError::ContextLost)
                ) {
                    return Ok(0x8255); // GL_UNKNOWN_CONTEXT_RESET
                }
                source = error.source();
            }
            Err(error)
        })
        .context("checking renderer reset status")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reset_blocks_rendering_and_remains_latched() {
        for status in [0x8253, 0x8254, 0x8255] {
            let mut gate = ResetGate::default();
            gate.check(|| Ok(0)).unwrap();
            let mut rendered = false;
            let result = gate.check(|| Ok(status)).map(|()| rendered = true);
            assert!(result.is_err());
            assert!(!rendered);
            assert!(
                gate.check(|| panic!("must not touch lost GL context"))
                    .is_err()
            );
        }
    }
}
