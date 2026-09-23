//! Compositor state, Wayland protocol handlers and the event loop.
use std::ffi::CStr;
use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use calloop::signals::{Signal, Signals};
use smithay::backend::allocator::dmabuf::Dmabuf;
use smithay::backend::drm::DrmNode;
use smithay::backend::egl::{EGLContext, EGLDevice, EGLDisplay};
use smithay::backend::renderer::gles::{GlesRenderer, ffi};
use smithay::backend::renderer::{ImportDma, utils::on_commit_buffer_handler};
use smithay::desktop::{
    PopupKind, PopupManager, Space, Window, WindowSurfaceType, find_popup_root_surface,
    get_popup_toplevel_coords,
};
use smithay::input::keyboard::XkbConfig;
use smithay::input::pointer::{CursorImageStatus, PointerHandle};
use smithay::input::{Seat, SeatHandler, SeatState};
use smithay::output::{Mode, Output, PhysicalProperties, Subpixel};
use smithay::reexports::calloop::generic::Generic;
use smithay::reexports::calloop::timer::{TimeoutAction, Timer};
use smithay::reexports::calloop::{EventLoop, Interest, LoopHandle, LoopSignal, PostAction};
use smithay::reexports::wayland_protocols::xdg::shell::server::xdg_toplevel;
use smithay::reexports::wayland_server::backend::{ClientData, ClientId, DisconnectReason};
use smithay::reexports::wayland_server::protocol::{wl_buffer, wl_seat, wl_surface::WlSurface};
use smithay::reexports::wayland_server::{Client, Display, DisplayHandle, Resource};
use smithay::utils::{Logical, Point, Serial, Size, Transform};
use smithay::wayland::buffer::BufferHandler;
use smithay::wayland::compositor::{
    CompositorClientState, CompositorHandler, CompositorState, get_parent, is_sync_subsurface,
    with_states,
};
use smithay::wayland::dmabuf::{
    DmabufFeedbackBuilder, DmabufGlobal, DmabufHandler, DmabufState, ImportNotifier,
};
use smithay::wayland::output::{OutputHandler, OutputManagerState};
use smithay::wayland::pointer_constraints::{
    PointerConstraint, PointerConstraintsHandler, PointerConstraintsState, with_pointer_constraint,
};
use smithay::wayland::relative_pointer::RelativePointerManagerState;
use smithay::wayland::selection::SelectionHandler;
use smithay::wayland::selection::data_device::{
    ClientDndGrabHandler, DataDeviceHandler, DataDeviceState, ServerDndGrabHandler,
    set_data_device_focus,
};
use smithay::wayland::shell::xdg::{
    PopupSurface, PositionerState, ToplevelSurface, XdgShellHandler, XdgShellState,
    XdgToplevelSurfaceData,
};
use smithay::wayland::shm::{ShmHandler, ShmState};
use smithay::wayland::socket::ListeningSocketSource;
use smithay::wayland::viewporter::ViewporterState;
use smithay::{
    delegate_compositor, delegate_data_device, delegate_dmabuf, delegate_output,
    delegate_pointer_constraints, delegate_relative_pointer, delegate_seat, delegate_shm,
    delegate_viewporter, delegate_xdg_shell,
};

use crate::control;

/// Stable per-display window identifier stored in each window's user data.
pub(crate) struct WindowId(pub(crate) u64);

static NEXT_WINDOW_ID: AtomicU64 = AtomicU64::new(1);

pub(crate) struct State {
    pub(crate) display_handle: DisplayHandle,
    pub(crate) loop_signal: LoopSignal,
    pub(crate) loop_handle: LoopHandle<'static, State>,
    pub(crate) start: Instant,
    pub(crate) space: Space<Window>,
    pub(crate) popups: PopupManager,
    pub(crate) seat: Seat<State>,
    pub(crate) output: Output,
    pub(crate) size: (i32, i32),
    pub(crate) renderer: GlesRenderer,
    pub(crate) renderer_info: RendererInfo,
    pub(crate) pointer_location: Point<f64, Logical>,
    /// The cursor the focused client asked for, drawn into screenshots on request.
    pub(crate) cursor: CursorImageStatus,
    compositor_state: CompositorState,
    xdg_shell_state: XdgShellState,
    shm_state: ShmState,
    seat_state: SeatState<State>,
    data_device_state: DataDeviceState,
    dmabuf_state: DmabufState,
    _dmabuf_global: Option<DmabufGlobal>,
    _output_manager_state: OutputManagerState,
}

#[derive(Clone)]
pub(crate) struct RendererInfo {
    pub(crate) gl_renderer: String,
    pub(crate) render_node: Option<String>,
    pub(crate) hardware: bool,
    pub(crate) dmabuf: bool,
}

#[derive(Default)]
struct ClientState {
    compositor_state: CompositorClientState,
}

impl ClientData for ClientState {
    fn initialized(&self, _client_id: ClientId) {}
    fn disconnected(&self, _client_id: ClientId, _reason: DisconnectReason) {}
}

struct Args {
    socket: String,
    control: PathBuf,
    size: (i32, i32),
    render_node: Option<PathBuf>,
}

fn parse_args() -> Result<Args> {
    let mut socket = None;
    let mut control = None;
    let mut size = (1920, 1080);
    let mut render_node = None;
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let value = args
            .next()
            .with_context(|| format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--socket" => socket = Some(value),
            "--control" => control = Some(PathBuf::from(value)),
            "--size" => {
                let (w, h) = value
                    .split_once('x')
                    .context("--size must look like 1920x1080")?;
                size = (w.parse()?, h.parse()?);
                if !(64..=8192).contains(&size.0) || !(64..=8192).contains(&size.1) {
                    bail!("--size must be between 64x64 and 8192x8192");
                }
            }
            // "auto" keeps the default: the boot VGA render node, then any GPU, then software.
            "--render-node" if value != "auto" => render_node = Some(PathBuf::from(value)),
            "--render-node" => {}
            _ => bail!("unknown argument {flag}"),
        }
    }
    Ok(Args {
        socket: socket.context("--socket is required")?,
        control: control.context("--control is required")?,
        size,
        render_node,
    })
}

/// Pick an EGL device: the requested render node, else the boot VGA GPU, else
/// any hardware GPU, else Mesa's software device (reported as non-hardware).
fn select_device(requested: Option<&PathBuf>) -> Result<(EGLDevice, Option<DrmNode>)> {
    let devices: Vec<EGLDevice> = EGLDevice::enumerate()
        .context("EGL device enumeration is unavailable (is Mesa's libEGL installed?)")?
        .collect();
    let with_nodes: Vec<(EGLDevice, Option<DrmNode>)> = devices
        .into_iter()
        .map(|device| {
            let node = device.try_get_render_node().ok().flatten();
            (device, node)
        })
        .collect();
    if let Some(path) = requested {
        let wanted = DrmNode::from_path(path)
            .with_context(|| format!("{} is not a DRM render node", path.display()))?;
        return with_nodes
            .into_iter()
            .find(|(_, node)| node.as_ref() == Some(&wanted))
            .with_context(|| format!("no EGL device renders on {}", path.display()));
    }
    let boot_vga = |node: &DrmNode| {
        let Some(name) = node
            .dev_path()
            .and_then(|p| p.file_name().map(|n| n.to_owned()))
        else {
            return false;
        };
        std::fs::read_to_string(
            PathBuf::from("/sys/class/drm")
                .join(name)
                .join("device/boot_vga"),
        )
        .is_ok_and(|value| value.trim() == "1")
    };
    let mut ranked = with_nodes;
    ranked.sort_by_key(|(device, node)| match node {
        Some(node) if boot_vga(node) => 0,
        Some(_) if !device.is_software() => 1,
        _ if device.is_software() => 3,
        _ => 2,
    });
    ranked
        .into_iter()
        .next()
        .context("no EGL device is available for headless rendering")
}

pub(crate) fn run() -> Result<()> {
    // Exit with the helper that owns this display, even before it connects.
    unsafe {
        libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM);
    }
    let args = parse_args()?;
    let mut event_loop: EventLoop<State> = EventLoop::try_new()?;
    let display: Display<State> = Display::new()?;
    let display_handle = display.handle();

    let (device, node) = select_device(args.render_node.as_ref())?;
    let hardware = !device.is_software() && node.is_some();
    let mut renderer = unsafe {
        let egl = EGLDisplay::new(device).context("creating the EGL display")?;
        let context = EGLContext::new(&egl).context("creating the EGL context")?;
        GlesRenderer::new(context).context("creating the GLES renderer")?
    };
    let gl_renderer = renderer
        .with_context(|gl| unsafe {
            let name = gl.GetString(ffi::RENDERER);
            if name.is_null() {
                String::new()
            } else {
                CStr::from_ptr(name.cast()).to_string_lossy().into_owned()
            }
        })
        .unwrap_or_default();

    let mut dmabuf_state = DmabufState::new();
    let dmabuf_global = match node.as_ref().filter(|_| hardware) {
        Some(node) => {
            let feedback = DmabufFeedbackBuilder::new(node.dev_id(), renderer.dmabuf_formats())
                .build()
                .context("building dmabuf feedback")?;
            Some(
                dmabuf_state
                    .create_global_with_default_feedback::<State>(&display_handle, &feedback),
            )
        }
        None => None,
    };
    let renderer_info = RendererInfo {
        gl_renderer,
        render_node: node
            .as_ref()
            .and_then(|node| node.dev_path())
            .map(|path| path.display().to_string()),
        hardware,
        dmabuf: dmabuf_global.is_some(),
    };

    let compositor_state = CompositorState::new::<State>(&display_handle);
    let xdg_shell_state = XdgShellState::new::<State>(&display_handle);
    let shm_state = ShmState::new::<State>(&display_handle, vec![]);
    let output_manager_state = OutputManagerState::new_with_xdg_output::<State>(&display_handle);
    let data_device_state = DataDeviceState::new::<State>(&display_handle);
    ViewporterState::new::<State>(&display_handle);
    RelativePointerManagerState::new::<State>(&display_handle);
    PointerConstraintsState::new::<State>(&display_handle);
    let mut seat_state = SeatState::new();
    let mut seat = seat_state.new_wl_seat(&display_handle, "borg-private");
    seat.add_keyboard(XkbConfig::default(), 400, 30)
        .context("loading the default keyboard layout")?;
    seat.add_pointer();

    let output = Output::new(
        "BORG-1".to_string(),
        PhysicalProperties {
            size: (0, 0).into(),
            subpixel: Subpixel::Unknown,
            make: "Borg".into(),
            model: "Private display".into(),
        },
    );
    let _output_global = output.create_global::<State>(&display_handle);
    let mode = Mode {
        size: args.size.into(),
        refresh: 60_000,
    };
    output.change_current_state(
        Some(mode),
        Some(Transform::Normal),
        None,
        Some((0, 0).into()),
    );
    output.set_preferred(mode);
    let mut space = Space::default();
    space.map_output(&output, (0, 0));

    let socket = ListeningSocketSource::with_name(&args.socket)
        .with_context(|| format!("binding Wayland socket {}", args.socket))?;
    let socket_name = socket.socket_name().to_string_lossy().into_owned();
    let handle = event_loop.handle();
    handle
        .insert_source(socket, |stream, _, state| {
            if let Err(error) = state
                .display_handle
                .insert_client(stream, std::sync::Arc::new(ClientState::default()))
            {
                eprintln!("borg-display: rejecting client: {error}");
            }
        })
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    handle
        .insert_source(
            Generic::new(display, Interest::READ, calloop::Mode::Level),
            |_, display, state| {
                // Safety: the display is never dropped while the loop runs.
                unsafe { display.get_mut().dispatch_clients(state)? };
                Ok(PostAction::Continue)
            },
        )
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    handle
        .insert_source(
            Timer::from_duration(Duration::from_millis(16)),
            |_, _, state| {
                state.frame_tick();
                TimeoutAction::ToDuration(Duration::from_millis(16))
            },
        )
        .map_err(|error| anyhow::anyhow!("{error}"))?;
    handle
        .insert_source(
            Signals::new(&[Signal::SIGTERM, Signal::SIGINT, Signal::SIGHUP])?,
            |_, _, state| state.loop_signal.stop(),
        )
        .map_err(|error| anyhow::anyhow!("{error}"))?;

    let _ = std::fs::remove_file(&args.control);
    let listener = UnixListener::bind(&args.control)
        .with_context(|| format!("binding control socket {}", args.control.display()))?;
    std::fs::set_permissions(
        &args.control,
        std::os::unix::fs::PermissionsExt::from_mode(0o600),
    )?;
    control::listen(&handle, listener)?;

    let mut state = State {
        display_handle,
        loop_signal: event_loop.get_signal(),
        loop_handle: handle.clone(),
        start: Instant::now(),
        space,
        popups: PopupManager::default(),
        seat,
        output,
        size: args.size,
        renderer,
        renderer_info: renderer_info.clone(),
        pointer_location: (0.0, 0.0).into(),
        cursor: CursorImageStatus::default_named(),
        compositor_state,
        xdg_shell_state,
        shm_state,
        seat_state,
        data_device_state,
        dmabuf_state,
        _dmabuf_global: dmabuf_global,
        _output_manager_state: output_manager_state,
    };

    // One readiness line on stdout; the owner parses it and then connects.
    println!(
        "{}",
        serde_json::json!({
            "ready": true,
            "wayland_display": socket_name,
            "control": args.control,
            "width": args.size.0,
            "height": args.size.1,
            "gl_renderer": renderer_info.gl_renderer,
            "render_node": renderer_info.render_node,
            "hardware": renderer_info.hardware,
            "dmabuf": renderer_info.dmabuf,
        })
    );
    let result = event_loop.run(None, &mut state, |_| {});
    let _ = std::fs::remove_file(&args.control);
    result.context("event loop failed")
}

impl State {
    pub(crate) fn now_ms(&self) -> u32 {
        self.start.elapsed().as_millis() as u32
    }

    fn frame_tick(&mut self) {
        let time = self.start.elapsed();
        let output = self.output.clone();
        for window in self.space.elements() {
            window.send_frame(&output, time, Some(Duration::ZERO), |_, _| {
                Some(output.clone())
            });
        }
        self.space.refresh();
        self.popups.cleanup();
        let _ = self.display_handle.flush_clients();
    }

    pub(crate) fn window_id(window: &Window) -> u64 {
        window
            .user_data()
            .get::<WindowId>()
            .map(|id| id.0)
            .unwrap_or_default()
    }

    pub(crate) fn find_window(&self, id: u64) -> Option<Window> {
        self.space
            .elements()
            .find(|window| Self::window_id(window) == id)
            .cloned()
    }

    fn window_for_surface(&self, surface: &WlSurface) -> Option<Window> {
        self.space
            .elements()
            .find(|window| window.toplevel().is_some_and(|t| t.wl_surface() == surface))
            .cloned()
    }

    pub(crate) fn focus_window(&mut self, window: &Window, serial: Serial) {
        self.space.raise_element(window, true);
        for other in self.space.elements() {
            if let Some(toplevel) = other.toplevel() {
                toplevel.send_pending_configure();
            }
        }
        let surface = window.toplevel().map(|t| t.wl_surface().clone());
        if let Some(keyboard) = self.seat.get_keyboard() {
            keyboard.set_focus(self, surface, serial);
        }
    }

    pub(crate) fn focused_window(&self) -> Option<Window> {
        let focus = self.seat.get_keyboard()?.current_focus()?;
        self.window_for_surface(&focus)
    }

    /// The active pointer constraint on the surface under the pointer, if any.
    pub(crate) fn active_constraint(&self) -> Option<Constraint> {
        let pointer = self.seat.get_pointer()?;
        let (surface, _) = self.surface_under(self.pointer_location)?;
        with_pointer_constraint(&surface, &pointer, |constraint| {
            let constraint = constraint.filter(|constraint| constraint.is_active())?;
            Some(match &*constraint {
                PointerConstraint::Locked(_) => Constraint::Locked,
                PointerConstraint::Confined(_) => Constraint::Confined,
            })
        })
    }

    /// Activate a pending lock or confinement once the pointer is over its
    /// surface (and inside its region), as a desktop compositor would.
    pub(crate) fn maybe_activate_constraint(&self) {
        let Some(pointer) = self.seat.get_pointer() else {
            return;
        };
        let Some((surface, origin)) = self.surface_under(self.pointer_location) else {
            return;
        };
        let local = self.pointer_location - origin;
        with_pointer_constraint(&surface, &pointer, |constraint| {
            if let Some(constraint) = constraint
                && !constraint.is_active()
                && constraint
                    .region()
                    .is_none_or(|region| region.contains(local.to_i32_round()))
            {
                constraint.activate();
            }
        });
    }

    pub(crate) fn surface_under(
        &self,
        position: Point<f64, Logical>,
    ) -> Option<(WlSurface, Point<f64, Logical>)> {
        self.space
            .element_under(position)
            .and_then(|(window, location)| {
                window
                    .surface_under(position - location.to_f64(), WindowSurfaceType::ALL)
                    .map(|(surface, offset)| (surface, (offset + location).to_f64()))
            })
    }

    fn output_logical_size(&self) -> Size<i32, Logical> {
        (self.size.0, self.size.1).into()
    }

    fn unconstrain_popup(&self, popup: &PopupSurface) {
        let Ok(root) = find_popup_root_surface(&PopupKind::Xdg(popup.clone())) else {
            return;
        };
        let Some(window) = self.window_for_surface(&root) else {
            return;
        };
        let Some(window_geometry) = self.space.element_geometry(&window) else {
            return;
        };
        let mut target = smithay::utils::Rectangle::from_size(self.output_logical_size());
        target.loc -= get_popup_toplevel_coords(&PopupKind::Xdg(popup.clone()));
        target.loc -= window_geometry.loc;
        popup.with_pending_state(|state| {
            state.geometry = state.positioner.get_unconstrained_geometry(target);
        });
    }

    /// App windows are asked to fill the display like a kiosk; dialogs and
    /// fixed-size windows keep their own size and are centred once.
    fn place_window(&mut self, window: &Window) {
        let size = window.geometry().size;
        if size.w <= 0 || size.h <= 0 || window.user_data().get::<Placed>().is_some() {
            return;
        }
        window.user_data().insert_if_missing(|| Placed);
        if (size.w, size.h) == self.size {
            return;
        }
        let x = ((self.size.0 - size.w) / 2).max(0);
        let y = ((self.size.1 - size.h) / 2).max(0);
        self.space.map_element(window.clone(), (x, y), true);
    }
}

struct Placed;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum Constraint {
    Locked,
    Confined,
}

impl PointerConstraintsHandler for State {
    fn new_constraint(&mut self, surface: &WlSurface, pointer: &PointerHandle<Self>) {
        if pointer.current_focus().as_ref() == Some(surface) {
            self.maybe_activate_constraint();
        }
    }

    fn cursor_position_hint(
        &mut self,
        surface: &WlSurface,
        pointer: &PointerHandle<Self>,
        location: Point<f64, Logical>,
    ) {
        let active = with_pointer_constraint(surface, pointer, |constraint| {
            constraint.is_some_and(|constraint| constraint.is_active())
        });
        if let Some((focus, origin)) = self.surface_under(self.pointer_location)
            && active
            && &focus == surface
        {
            self.pointer_location = origin + location;
            pointer.set_location(self.pointer_location);
        }
    }
}

impl CompositorHandler for State {
    fn compositor_state(&mut self) -> &mut CompositorState {
        &mut self.compositor_state
    }

    fn client_compositor_state<'a>(&self, client: &'a Client) -> &'a CompositorClientState {
        &client
            .get_data::<ClientState>()
            .expect("every client is inserted with ClientState")
            .compositor_state
    }

    fn commit(&mut self, surface: &WlSurface) {
        on_commit_buffer_handler::<Self>(surface);
        if !is_sync_subsurface(surface) {
            let mut root = surface.clone();
            while let Some(parent) = get_parent(&root) {
                root = parent;
            }
            if let Some(window) = self.window_for_surface(&root) {
                window.on_commit();
            }
        }
        self.popups.commit(surface);
        if let Some(window) = self.window_for_surface(surface) {
            let initial_configure_sent = with_states(surface, |states| {
                states
                    .data_map
                    .get::<XdgToplevelSurfaceData>()
                    .is_some_and(|data| data.lock().unwrap().initial_configure_sent)
            });
            if let Some(toplevel) = window.toplevel() {
                if !initial_configure_sent {
                    toplevel.send_configure();
                } else {
                    self.place_window(&window);
                }
            }
        }
        if let Some(PopupKind::Xdg(popup)) = self.popups.find_popup(surface)
            && !popup.is_initial_configure_sent()
        {
            let _ = popup.send_configure();
        }
    }
}

impl BufferHandler for State {
    fn buffer_destroyed(&mut self, _buffer: &wl_buffer::WlBuffer) {}
}

impl ShmHandler for State {
    fn shm_state(&self) -> &ShmState {
        &self.shm_state
    }
}

impl DmabufHandler for State {
    fn dmabuf_state(&mut self) -> &mut DmabufState {
        &mut self.dmabuf_state
    }

    fn dmabuf_imported(
        &mut self,
        _global: &DmabufGlobal,
        dmabuf: Dmabuf,
        notifier: ImportNotifier,
    ) {
        if self.renderer.import_dmabuf(&dmabuf, None).is_ok() {
            let _ = notifier.successful::<State>();
        } else {
            notifier.failed();
        }
    }
}

impl SeatHandler for State {
    type KeyboardFocus = WlSurface;
    type PointerFocus = WlSurface;
    type TouchFocus = WlSurface;

    fn seat_state(&mut self) -> &mut SeatState<Self> {
        &mut self.seat_state
    }

    fn cursor_image(&mut self, _seat: &Seat<Self>, image: CursorImageStatus) {
        self.cursor = image;
    }

    fn focus_changed(&mut self, seat: &Seat<Self>, focused: Option<&WlSurface>) {
        let client = focused.and_then(|surface| self.display_handle.get_client(surface.id()).ok());
        set_data_device_focus(&self.display_handle, seat, client);
    }
}

impl SelectionHandler for State {
    type SelectionUserData = ();
}

impl DataDeviceHandler for State {
    fn data_device_state(&self) -> &DataDeviceState {
        &self.data_device_state
    }
}

impl ClientDndGrabHandler for State {}
impl ServerDndGrabHandler for State {}

impl OutputHandler for State {}

impl XdgShellHandler for State {
    fn xdg_shell_state(&mut self) -> &mut XdgShellState {
        &mut self.xdg_shell_state
    }

    fn new_toplevel(&mut self, surface: ToplevelSurface) {
        if surface.parent().is_none() {
            let size = self.output_logical_size();
            surface.with_pending_state(|state| {
                state.size = Some(size);
                state.states.set(xdg_toplevel::State::Maximized);
            });
        }
        let window = Window::new_wayland_window(surface);
        let id = NEXT_WINDOW_ID.fetch_add(1, Ordering::Relaxed);
        window.user_data().insert_if_missing(|| WindowId(id));
        self.space.map_element(window.clone(), (0, 0), true);
        self.focus_window(&window, smithay::utils::SERIAL_COUNTER.next_serial());
    }

    fn toplevel_destroyed(&mut self, surface: ToplevelSurface) {
        if let Some(window) = self.window_for_surface(surface.wl_surface()) {
            self.space.unmap_elem(&window);
        }
        if let Some(top) = self.space.elements().last().cloned() {
            self.focus_window(&top, smithay::utils::SERIAL_COUNTER.next_serial());
        }
    }

    fn new_popup(&mut self, surface: PopupSurface, positioner: PositionerState) {
        surface.with_pending_state(|state| state.geometry = positioner.get_geometry());
        self.unconstrain_popup(&surface);
        let _ = self.popups.track_popup(PopupKind::Xdg(surface));
    }

    fn reposition_request(
        &mut self,
        surface: PopupSurface,
        positioner: PositionerState,
        token: u32,
    ) {
        surface.with_pending_state(|state| {
            state.geometry = positioner.get_geometry();
            state.positioner = positioner;
        });
        self.unconstrain_popup(&surface);
        surface.send_repositioned(token);
    }

    fn grab(&mut self, _surface: PopupSurface, _seat: wl_seat::WlSeat, _serial: Serial) {}

    fn fullscreen_request(
        &mut self,
        surface: ToplevelSurface,
        _output: Option<smithay::reexports::wayland_server::protocol::wl_output::WlOutput>,
    ) {
        let size = self.output_logical_size();
        surface.with_pending_state(|state| {
            state.size = Some(size);
            state.states.set(xdg_toplevel::State::Fullscreen);
        });
        if let Some(window) = self.window_for_surface(surface.wl_surface()) {
            self.space.map_element(window, (0, 0), true);
        }
        surface.send_pending_configure();
    }

    fn unfullscreen_request(&mut self, surface: ToplevelSurface) {
        surface.with_pending_state(|state| {
            state.states.unset(xdg_toplevel::State::Fullscreen);
        });
        surface.send_pending_configure();
    }
}

delegate_compositor!(State);
delegate_shm!(State);
delegate_dmabuf!(State);
delegate_seat!(State);
delegate_data_device!(State);
delegate_output!(State);
delegate_xdg_shell!(State);
delegate_viewporter!(State);
delegate_relative_pointer!(State);
delegate_pointer_constraints!(State);
