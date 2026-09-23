//! Owner control socket: newline-delimited JSON requests and responses.
//!
//! This is the only way input reaches the private seat, so injected events
//! can never land on the user's physical seat or focused window.
use std::collections::HashMap;
use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use smithay::backend::allocator::Fourcc;
use smithay::backend::input::{Axis, AxisSource, ButtonState, KeyState};
use smithay::backend::renderer::damage::OutputDamageTracker;
use smithay::backend::renderer::element::AsRenderElements;
use smithay::backend::renderer::element::RenderElement;
use smithay::backend::renderer::element::surface::WaylandSurfaceRenderElement;
use smithay::backend::renderer::gles::{GlesRenderer, GlesTexture};
use smithay::backend::renderer::{Bind, ExportMem, Offscreen};
use smithay::desktop::Window;
use smithay::desktop::space::space_render_elements;
use smithay::input::keyboard::{FilterResult, Keycode, xkb};
use smithay::input::pointer::{AxisFrame, ButtonEvent, MotionEvent, RelativeMotionEvent};
use smithay::reexports::calloop::generic::Generic;
use smithay::reexports::calloop::{Interest, LoopHandle, Mode, PostAction};
use smithay::reexports::wayland_server::Resource;
use smithay::utils::{Point, Rectangle, SERIAL_COUNTER, Scale, Size, Transform};
use smithay::wayland::compositor::with_states;
use smithay::wayland::shell::xdg::XdgToplevelSurfaceData;

use crate::compositor::State;

const MAX_REQUEST: usize = 256 * 1024;
const KEY_LEFTSHIFT: u32 = 42;
const KEY_ENTER: u32 = 28;
const KEY_TAB: u32 = 15;

/// The first connection owns the display: when it closes, the display exits.
static OWNER_CLAIMED: AtomicBool = AtomicBool::new(false);

pub(crate) fn listen(handle: &LoopHandle<'static, State>, listener: UnixListener) -> Result<()> {
    listener.set_nonblocking(true)?;
    // The loop handle comes from State, not a captured clone: a captured
    // handle would keep the loop (and the Wayland socket file) alive forever.
    handle
        .insert_source(
            Generic::new(listener, Interest::READ, Mode::Level),
            move |_, listener, state| {
                while let Ok((stream, _)) = listener.accept() {
                    let owner = !OWNER_CLAIMED.swap(true, Ordering::SeqCst);
                    if let Err(error) = serve(&state.loop_handle, stream, owner) {
                        eprintln!("borg-display: control connection failed: {error:#}");
                    }
                }
                Ok(PostAction::Continue)
            },
        )
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    Ok(())
}

fn serve(handle: &LoopHandle<'static, State>, stream: UnixStream, owner: bool) -> Result<()> {
    stream.set_nonblocking(true)?;
    let mut buffer = Vec::new();
    handle
        .insert_source(
            Generic::new(stream, Interest::READ, Mode::Level),
            move |_, stream, state| {
                let mut chunk = [0u8; 16 * 1024];
                // Safety: the stream is only borrowed for this callback.
                let stream = unsafe { stream.get_mut() };
                let read = match stream.read(&mut chunk) {
                    Ok(read) => read,
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        return Ok(PostAction::Continue);
                    }
                    Err(_) => 0,
                };
                if read == 0 || buffer.len() + read > MAX_REQUEST {
                    if owner {
                        state.loop_signal.stop();
                    }
                    return Ok(PostAction::Remove);
                }
                buffer.extend_from_slice(&chunk[..read]);
                while let Some(end) = buffer.iter().position(|byte| *byte == b'\n') {
                    let line: Vec<u8> = buffer.drain(..=end).collect();
                    let response = match serde_json::from_slice::<Value>(&line) {
                        Ok(request) => match handle_request(state, &request) {
                            Ok(result) => json!({"ok": true, "result": result}),
                            Err(error) => json!({"ok": false, "error": format!("{error:#}")}),
                        },
                        Err(error) => {
                            json!({"ok": false, "error": format!("invalid JSON: {error}")})
                        }
                    };
                    let mut bytes = response.to_string().into_bytes();
                    bytes.push(b'\n');
                    if write_all(stream, &bytes).is_err() {
                        if owner {
                            state.loop_signal.stop();
                        }
                        return Ok(PostAction::Remove);
                    }
                }
                let _ = state.display_handle.flush_clients();
                Ok(PostAction::Continue)
            },
        )
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    Ok(())
}

fn write_all(stream: &mut UnixStream, mut bytes: &[u8]) -> std::io::Result<()> {
    while !bytes.is_empty() {
        match stream.write(bytes) {
            Ok(0) => return Err(std::io::ErrorKind::WriteZero.into()),
            Ok(written) => bytes = &bytes[written..],
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

fn number(request: &Value, key: &str) -> Result<f64> {
    request
        .get(key)
        .and_then(Value::as_f64)
        .filter(|value| value.is_finite())
        .with_context(|| format!("{key} must be a finite number"))
}

fn window_arg(state: &State, request: &Value) -> Result<Window> {
    let id = request
        .get("window")
        .and_then(Value::as_u64)
        .context("window must be a window id from windows")?;
    state
        .find_window(id)
        .context("unknown or closed window; list windows again")
}

fn handle_request(state: &mut State, request: &Value) -> Result<Value> {
    let op = request
        .get("op")
        .and_then(Value::as_str)
        .context("op is required")?;
    match op {
        "info" => Ok(json!({
            "width": state.size.0,
            "height": state.size.1,
            "gl_renderer": state.renderer_info.gl_renderer,
            "render_node": state.renderer_info.render_node,
            "hardware": state.renderer_info.hardware,
            "dmabuf": state.renderer_info.dmabuf,
            "pointer": {"x": state.pointer_location.x, "y": state.pointer_location.y},
        })),
        "windows" => Ok(json!({"windows": windows(state)})),
        "focus" => {
            let window = window_arg(state, request)?;
            state.focus_window(&window, SERIAL_COUNTER.next_serial());
            Ok(json!({"focused": State::window_id(&window)}))
        }
        "close" => {
            let window = window_arg(state, request)?;
            if let Some(toplevel) = window.toplevel() {
                toplevel.send_close();
            }
            Ok(json!({"closing": State::window_id(&window)}))
        }
        "pointer_move" => {
            if request.get("dx").is_some() || request.get("dy").is_some() {
                let dx = request.get("dx").and_then(Value::as_f64).unwrap_or(0.0);
                let dy = request.get("dy").and_then(Value::as_f64).unwrap_or(0.0);
                ensure!(dx.is_finite() && dy.is_finite(), "dx and dy must be finite");
                relative_motion(state, dx, dy);
            } else {
                let x = number(request, "x")?;
                let y = number(request, "y")?;
                move_pointer(state, x, y)?;
            }
            Ok(json!({"pointer": {"x": state.pointer_location.x, "y": state.pointer_location.y}}))
        }
        "button" => {
            let code = request
                .get("code")
                .and_then(Value::as_u64)
                .context("code must be an evdev button code")? as u32;
            let pressed = request
                .get("pressed")
                .and_then(Value::as_bool)
                .context("pressed must be a boolean")?;
            button(state, code, pressed);
            Ok(json!({}))
        }
        "axis" => {
            let dx = request.get("dx").and_then(Value::as_i64).unwrap_or(0);
            let dy = request.get("dy").and_then(Value::as_i64).unwrap_or(0);
            ensure!(dx.abs() <= 1000 && dy.abs() <= 1000, "at most 1000 notches");
            axis(state, dx as i32, dy as i32);
            Ok(json!({}))
        }
        "key" => {
            let code = request
                .get("code")
                .and_then(Value::as_u64)
                .context("code must be an evdev key code")? as u32;
            let pressed = request
                .get("pressed")
                .and_then(Value::as_bool)
                .context("pressed must be a boolean")?;
            key(state, code, pressed);
            Ok(json!({}))
        }
        "type" => {
            let text = request
                .get("text")
                .and_then(Value::as_str)
                .context("text must be a string")?;
            type_text(state, text)?;
            Ok(json!({"typed": text.chars().count()}))
        }
        "screenshot" => {
            let path = request
                .get("path")
                .and_then(Value::as_str)
                .context("path is required")?;
            let window = match request.get("window") {
                Some(_) => Some(window_arg(state, request)?),
                None => None,
            };
            screenshot(state, window.as_ref(), path)
        }
        _ => bail!("unsupported op {op}"),
    }
}

fn windows(state: &State) -> Vec<Value> {
    let focused = state
        .focused_window()
        .map(|window| State::window_id(&window));
    state
        .space
        .elements()
        .filter_map(|window| {
            let toplevel = window.toplevel()?;
            let surface = toplevel.wl_surface();
            let (title, app_id) = with_states(surface, |states| {
                states
                    .data_map
                    .get::<XdgToplevelSurfaceData>()
                    .map(|data| {
                        let data = data.lock().unwrap();
                        (data.title.clone(), data.app_id.clone())
                    })
                    .unwrap_or_default()
            });
            let pid = state
                .display_handle
                .get_client(surface.id())
                .ok()
                .and_then(|client| client.get_credentials(&state.display_handle).ok())
                .map(|credentials| credentials.pid);
            let bounds = state.space.element_geometry(window)?;
            let id = State::window_id(window);
            Some(json!({
                "id": id,
                "title": title.unwrap_or_default(),
                "app_id": app_id.unwrap_or_default(),
                "pid": pid,
                "bounds": {"x": bounds.loc.x, "y": bounds.loc.y,
                           "width": bounds.size.w, "height": bounds.size.h},
                "focused": focused == Some(id),
            }))
        })
        .collect()
}

fn move_pointer(state: &mut State, x: f64, y: f64) -> Result<()> {
    let (width, height) = state.size;
    ensure!(
        (0.0..width as f64).contains(&x) && (0.0..height as f64).contains(&y),
        "point ({x}, {y}) is outside the {width}x{height} private display"
    );
    let location = Point::from((x, y));
    state.pointer_location = location;
    let under = state.surface_under(location);
    let Some(pointer) = state.seat.get_pointer() else {
        bail!("the private seat has no pointer");
    };
    let event = MotionEvent {
        location,
        serial: SERIAL_COUNTER.next_serial(),
        time: state.now_ms(),
    };
    pointer.motion(state, under, &event);
    pointer.frame(state);
    Ok(())
}

/// Relative motion for pointer-locked apps (games, editor viewports): moves
/// the pointer and emits relative-pointer events.
fn relative_motion(state: &mut State, dx: f64, dy: f64) {
    let (width, height) = state.size;
    let location = Point::from((
        (state.pointer_location.x + dx).clamp(0.0, width as f64 - 1.0),
        (state.pointer_location.y + dy).clamp(0.0, height as f64 - 1.0),
    ));
    state.pointer_location = location;
    let under = state.surface_under(location);
    let Some(pointer) = state.seat.get_pointer() else {
        return;
    };
    let time = state.now_ms();
    pointer.motion(
        state,
        under.clone(),
        &MotionEvent {
            location,
            serial: SERIAL_COUNTER.next_serial(),
            time,
        },
    );
    pointer.relative_motion(
        state,
        under,
        &RelativeMotionEvent {
            delta: (dx, dy).into(),
            delta_unaccel: (dx, dy).into(),
            utime: state.start.elapsed().as_micros() as u64,
        },
    );
    pointer.frame(state);
}

fn button(state: &mut State, code: u32, pressed: bool) {
    let serial = SERIAL_COUNTER.next_serial();
    if pressed {
        let target = state
            .space
            .element_under(state.pointer_location)
            .map(|(window, _)| window.clone());
        if let Some(window) = target {
            state.focus_window(&window, serial);
        }
    }
    let Some(pointer) = state.seat.get_pointer() else {
        return;
    };
    let event = ButtonEvent {
        serial,
        time: state.now_ms(),
        button: code,
        state: if pressed {
            ButtonState::Pressed
        } else {
            ButtonState::Released
        },
    };
    pointer.button(state, &event);
    pointer.frame(state);
}

/// Wheel notches; positive dy scrolls content down, positive dx right.
fn axis(state: &mut State, dx: i32, dy: i32) {
    let Some(pointer) = state.seat.get_pointer() else {
        return;
    };
    let mut frame = AxisFrame::new(state.now_ms()).source(AxisSource::Wheel);
    if dy != 0 {
        frame = frame
            .value(Axis::Vertical, f64::from(dy) * 15.0)
            .v120(Axis::Vertical, dy * 120);
    }
    if dx != 0 {
        frame = frame
            .value(Axis::Horizontal, f64::from(dx) * 15.0)
            .v120(Axis::Horizontal, dx * 120);
    }
    pointer.axis(state, frame);
    pointer.frame(state);
}

fn key(state: &mut State, code: u32, pressed: bool) {
    let Some(keyboard) = state.seat.get_keyboard() else {
        return;
    };
    let key_state = if pressed {
        KeyState::Pressed
    } else {
        KeyState::Released
    };
    let time = state.now_ms();
    keyboard.input::<(), _>(
        state,
        Keycode::new(code + 8),
        key_state,
        SERIAL_COUNTER.next_serial(),
        time,
        |_, _, _| FilterResult::Forward,
    );
}

/// Characters the default layout can produce: evdev code and whether shift
/// is needed. Built from the same RMLVO defaults as the seat keyboard.
fn layout_characters() -> HashMap<char, (u32, bool)> {
    let context = xkb::Context::new(xkb::CONTEXT_NO_FLAGS);
    let Some(keymap) =
        xkb::Keymap::new_from_names(&context, "", "", "", "", None, xkb::COMPILE_NO_FLAGS)
    else {
        return HashMap::new();
    };
    let mut characters = HashMap::new();
    keymap.key_for_each(|keymap, keycode| {
        for level in 0..keymap.num_levels_for_key(keycode, 0).min(2) {
            for sym in keymap.key_get_syms_by_level(keycode, 0, level) {
                if let Some(ch) = char::from_u32(xkb::keysym_to_utf32(*sym))
                    && !ch.is_control()
                {
                    characters
                        .entry(ch)
                        .or_insert((keycode.raw().saturating_sub(8), level == 1));
                }
            }
        }
    });
    characters.insert('\n', (KEY_ENTER, false));
    characters.insert('\t', (KEY_TAB, false));
    characters
}

fn type_text(state: &mut State, text: &str) -> Result<()> {
    let characters = layout_characters();
    let missing: String = text
        .chars()
        .filter(|ch| !characters.contains_key(ch))
        .collect();
    ensure!(
        missing.is_empty(),
        "the private keyboard layout cannot type {missing:?}; use set_value for these characters"
    );
    for ch in text.chars() {
        let (code, shift) = characters[&ch];
        if shift {
            key(state, KEY_LEFTSHIFT, true);
        }
        key(state, code, true);
        key(state, code, false);
        if shift {
            key(state, KEY_LEFTSHIFT, false);
        }
    }
    Ok(())
}

/// Render the whole display, or one window's visible geometry, offscreen on
/// the display's GPU and write it as a PNG.
fn screenshot(state: &mut State, window: Option<&Window>, path: &str) -> Result<Value> {
    let (size, bytes) = match window {
        None => {
            let elements = space_render_elements::<_, Window, _>(
                &mut state.renderer,
                [&state.space],
                &state.output,
                1.0,
            )
            .map_err(|error| anyhow::anyhow!("{error:?}"))?;
            let size = Size::from(state.size);
            (size, render(&mut state.renderer, size, &elements)?)
        }
        Some(window) => {
            let geometry = window.geometry();
            ensure!(
                geometry.size.w > 0 && geometry.size.h > 0,
                "window has not drawn anything yet"
            );
            let elements: Vec<WaylandSurfaceRenderElement<GlesRenderer>> = window.render_elements(
                &mut state.renderer,
                Point::from((-geometry.loc.x, -geometry.loc.y)),
                Scale::from(1.0),
                1.0,
            );
            let size = Size::from((geometry.size.w, geometry.size.h));
            (size, render(&mut state.renderer, size, &elements)?)
        }
    };
    let file = std::fs::File::create(path).with_context(|| format!("creating {path}"))?;
    let mut encoder =
        png::Encoder::new(std::io::BufWriter::new(file), size.w as u32, size.h as u32);
    encoder.set_color(png::ColorType::Rgba);
    encoder.set_depth(png::BitDepth::Eight);
    encoder
        .write_header()
        .and_then(|mut writer| writer.write_image_data(&bytes))
        .context("encoding PNG")?;
    Ok(json!({"path": path, "width": size.w, "height": size.h}))
}

fn render<E: RenderElement<GlesRenderer>>(
    renderer: &mut GlesRenderer,
    size: Size<i32, smithay::utils::Physical>,
    elements: &[E],
) -> Result<Vec<u8>> {
    let buffer_size = Size::<i32, smithay::utils::Buffer>::from((size.w, size.h));
    let mut texture: GlesTexture = renderer
        .create_buffer(Fourcc::Abgr8888, buffer_size)
        .context("allocating the offscreen buffer")?;
    let mut framebuffer = renderer
        .bind(&mut texture)
        .context("binding offscreen buffer")?;
    let mut tracker = OutputDamageTracker::new(size, 1.0, Transform::Normal);
    tracker
        .render_output(
            renderer,
            &mut framebuffer,
            0,
            elements,
            [0.12, 0.12, 0.14, 1.0],
        )
        .map_err(|error| anyhow::anyhow!("render failed: {error:?}"))?;
    let mapping = renderer
        .copy_framebuffer(
            &framebuffer,
            Rectangle::from_size(buffer_size),
            Fourcc::Abgr8888,
        )
        .context("reading back the frame")?;
    let mut bytes = renderer
        .map_texture(&mapping)
        .context("mapping the frame")?
        .to_vec();
    // Composited alpha is meaningless for a screenshot; make it opaque.
    for pixel in bytes.as_chunks_mut::<4>().0 {
        pixel[3] = 255;
    }
    Ok(bytes)
}
