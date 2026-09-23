//! `borg-display`: a headless Wayland compositor that gives one Borg session a
//! private display. Apps render on the GPU through dmabuf, and every input
//! event and capture goes through the owner's control socket, never the
//! user's seat.

#[cfg(target_os = "linux")]
mod compositor;
#[cfg(target_os = "linux")]
mod control;

#[cfg(target_os = "linux")]
fn main() {
    // The self-updater verifies a staged compositor against the release version.
    if std::env::args().nth(1).as_deref() == Some("--version") {
        println!("borg-display {}", env!("CARGO_PKG_VERSION"));
        return;
    }
    if let Err(error) = compositor::run() {
        eprintln!("borg-display: {error:#}");
        std::process::exit(1);
    }
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("borg-display: private displays are only available on Linux");
    std::process::exit(1);
}
