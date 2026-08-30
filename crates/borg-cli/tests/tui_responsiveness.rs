#![cfg(target_os = "linux")]

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::fd::FromRawFd;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, Instant};

const PROBE_SAMPLES: usize = 24;
const INPUT_P95_SLO: Duration = Duration::from_millis(100);
const INPUT_MAX_SLO: Duration = Duration::from_millis(250);
const ACTIVE_CPU_SAMPLE: Duration = Duration::from_secs(1);
const ACTIVE_CPU_RATIO_SLO: f64 = 0.25;
const PTY_ROWS: usize = 40;
const PTY_COLS: usize = 120;

#[test]
#[ignore = "explicit live TUI, streaming, and storage-pressure performance gate"]
fn live_tui_input_latency_under_storage_pressure() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let profile_root = workspace.join("target/tui-responsiveness");
    fs::create_dir_all(&profile_root).expect("create TUI profile root");
    let pressure_fixture =
        tempfile::tempdir_in(profile_root).expect("create storage-pressure fixture");
    // Unix-domain control sockets have a short path limit. Keep the isolated
    // Borg runtime under /tmp while the pressure file stays on the workspace
    // filesystem that this profile is intended to exercise.
    let runtime = tempfile::tempdir().expect("create isolated Borg runtime");
    let borg_home = runtime.path().join("borg-home");
    let config_home = runtime.path().join("config");
    fs::create_dir_all(config_home.join("borg")).expect("create isolated config root");
    fs::write(
        config_home.join("borg/agent.toml"),
        "[updates]\nauto_install = false\n",
    )
    .expect("disable updates in performance fixture");
    fs::write(
        config_home.join("borg/editor.toml"),
        "[presentation]\nrefresh_rate_fps = 165\ndictation_icon = \"emoji\"\n",
    )
    .expect("configure the isolated editor fixture");

    let (endpoint, stream_started, server) = spawn_streaming_provider();
    let executable = std::env::var("BORG_TUI_STRESS_EXE")
        .unwrap_or_else(|_| env!("CARGO_BIN_EXE_borg").to_string());
    let mut terminal = PtyChild::spawn(
        &executable,
        runtime.path(),
        &borg_home,
        &config_home,
        &endpoint,
    )
    .expect("start Borg in a pseudo-terminal");
    terminal
        .wait_for_output(Duration::from_secs(10))
        .expect("wait for first TUI paint");
    stream_started
        .recv_timeout(Duration::from_secs(10))
        .expect("mock provider did not receive the prompt");
    terminal
        .wait_for_pattern(b"live-", Duration::from_secs(10))
        .expect("wait for the active streaming screen");

    let cpu_before = terminal.cpu_time().expect("read initial Borg CPU time");
    let cpu_sample_started = Instant::now();
    while cpu_sample_started.elapsed() < ACTIVE_CPU_SAMPLE {
        terminal
            .read_available(None)
            .expect("drain output during active CPU sample");
        thread::sleep(Duration::from_millis(2));
    }
    let cpu_ratio = terminal
        .cpu_time()
        .expect("read final Borg CPU time")
        .saturating_sub(cpu_before)
        .as_secs_f64()
        / cpu_sample_started.elapsed().as_secs_f64();

    let stop_pressure = Arc::new(AtomicBool::new(false));
    let pressure_bytes = Arc::new(AtomicU64::new(0));
    let pressure = spawn_storage_pressure(
        pressure_fixture.path().join("storage-pressure.bin"),
        Arc::clone(&stop_pressure),
        Arc::clone(&pressure_bytes),
    );
    let io_pressure_before = io_pressure_total();

    let mut samples = Vec::with_capacity(PROBE_SAMPLES);
    for sample in 0..PROBE_SAMPLES {
        if sample > 0 {
            terminal
                .write_all_retry(b"\x03")
                .expect("clear previous latency probe");
            thread::sleep(Duration::from_millis(10));
        }
        terminal.drain_output().expect("drain old TUI output");
        let marker = format!("borg-latency-probe-{sample:03}-ZXQ");
        samples.push(
            terminal
                .type_and_measure(&marker, Duration::from_secs(2))
                .unwrap_or_else(|error| panic!("latency probe {sample} failed: {error}")),
        );
    }

    stop_pressure.store(true, Ordering::Release);
    pressure.join().expect("storage-pressure worker panicked");
    server.join().expect("streaming-provider worker panicked");
    let io_pressure_after = io_pressure_total();
    let written = pressure_bytes.load(Ordering::Acquire);

    samples.sort_unstable();
    let p50 = percentile(&samples, 50);
    let p95 = percentile(&samples, 95);
    let max = *samples.last().expect("latency samples");
    let pressure_delta = io_pressure_after.saturating_sub(io_pressure_before);
    eprintln!(
        "live TUI/storage profile: active_cpu={:.1}% samples={} input_p50={p50:?} input_p95={p95:?} input_max={max:?} pressure_bytes={} io_full_stall={:?}",
        cpu_ratio * 100.0,
        samples.len(),
        written,
        Duration::from_micros(pressure_delta),
    );

    assert!(
        cpu_ratio <= ACTIVE_CPU_RATIO_SLO,
        "active TUI CPU exceeded {:.0}% of one core: {:.1}%",
        ACTIVE_CPU_RATIO_SLO * 100.0,
        cpu_ratio * 100.0,
    );
    assert!(
        p95 <= INPUT_P95_SLO,
        "input-to-paint p95 exceeded {INPUT_P95_SLO:?}: {p95:?}"
    );
    assert!(
        max <= INPUT_MAX_SLO,
        "input-to-paint max exceeded {INPUT_MAX_SLO:?}: {max:?}"
    );
}

fn spawn_streaming_provider() -> (String, mpsc::Receiver<()>, thread::JoinHandle<()>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind mock provider");
    let address = listener.local_addr().expect("mock provider address");
    let (started_tx, started_rx) = mpsc::channel();
    let server = thread::spawn(move || {
        let (mut socket, _) = listener.accept().expect("accept provider request");
        read_http_request(&mut socket).expect("read provider request");
        started_tx.send(()).expect("publish provider start");

        const DELTAS: usize = 1_500;
        let mut frames = Vec::with_capacity(DELTAS + 2);
        for index in 0..DELTAS {
            frames.push(format!(
                "data: {{\"choices\":[{{\"delta\":{{\"reasoning_content\":\"live-{index} \"}},\"finish_reason\":null}}]}}\n\n"
            ));
        }
        frames.push(
            "data: {\"choices\":[{\"delta\":{\"content\":\"stream complete\"},\"finish_reason\":\"stop\"}],\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":1500}}\n\n"
                .to_string(),
        );
        frames.push("data: [DONE]\n\n".to_string());
        let content_len = frames.iter().map(String::len).sum::<usize>();
        write!(
            socket,
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {content_len}\r\nconnection: close\r\n\r\n"
        )
        .expect("write provider response headers");
        socket.flush().expect("flush provider response headers");
        for frame in frames {
            if socket.write_all(frame.as_bytes()).is_err() || socket.flush().is_err() {
                return;
            }
            thread::sleep(Duration::from_millis(2));
        }
    });
    (format!("http://{address}/v1"), started_rx, server)
}

fn read_http_request(socket: &mut std::net::TcpStream) -> io::Result<()> {
    let mut request = Vec::new();
    let expected_len = loop {
        let mut chunk = [0_u8; 8192];
        let read = socket.read(&mut chunk)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "provider request ended before its headers",
            ));
        }
        request.extend_from_slice(&chunk[..read]);
        let Some(header_end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") else {
            continue;
        };
        let headers = String::from_utf8_lossy(&request[..header_end]);
        let content_len = headers
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .map(str::trim)
                    .and_then(|value| value.parse::<usize>().ok())
            })
            .unwrap_or(0);
        break header_end + 4 + content_len;
    };
    while request.len() < expected_len {
        let mut chunk = [0_u8; 8192];
        let read = socket.read(&mut chunk)?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "provider request ended before its body",
            ));
        }
        request.extend_from_slice(&chunk[..read]);
    }
    Ok(())
}

fn spawn_storage_pressure(
    path: std::path::PathBuf,
    stop: Arc<AtomicBool>,
    bytes: Arc<AtomicU64>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        const BUFFER_BYTES: usize = 1024 * 1024;
        const FILE_BYTES: u64 = 256 * 1024 * 1024;
        const SYNC_BYTES: u64 = 8 * 1024 * 1024;
        let buffer = vec![0xA5; BUFFER_BYTES];
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(path)
            .expect("open storage-pressure file");
        let mut offset = 0_u64;
        let mut unsynced = 0_u64;
        while !stop.load(Ordering::Acquire) {
            file.write_all(&buffer)
                .expect("write storage-pressure block");
            offset += BUFFER_BYTES as u64;
            unsynced += BUFFER_BYTES as u64;
            bytes.fetch_add(BUFFER_BYTES as u64, Ordering::Relaxed);
            if unsynced >= SYNC_BYTES {
                file.sync_data().expect("sync storage-pressure file");
                unsynced = 0;
            }
            if offset >= FILE_BYTES {
                file.set_len(0).expect("truncate storage-pressure file");
                file.seek(SeekFrom::Start(0))
                    .expect("rewind storage-pressure file");
                offset = 0;
            }
        }
        file.sync_data().expect("finish storage-pressure file");
    })
}

fn percentile(samples: &[Duration], percentile: usize) -> Duration {
    samples[(samples.len() * percentile).div_ceil(100).saturating_sub(1)]
}

fn io_pressure_total() -> u64 {
    fs::read_to_string("/proc/pressure/io")
        .ok()
        .and_then(|source| {
            source.lines().find_map(|line| {
                let mut fields = line.split_whitespace();
                (fields.next() == Some("full"))
                    .then(|| fields.find_map(|field| field.strip_prefix("total=")?.parse().ok()))
                    .flatten()
            })
        })
        .unwrap_or(0)
}

struct PtyChild {
    child: Child,
    master: File,
    log_path: std::path::PathBuf,
    screen: VirtualScreen,
}

impl PtyChild {
    fn spawn(
        executable: &str,
        cwd: &Path,
        borg_home: &Path,
        config_home: &Path,
        endpoint: &str,
    ) -> io::Result<Self> {
        let mut master_fd = -1;
        let mut slave_fd = -1;
        let size = libc::winsize {
            ws_row: PTY_ROWS as u16,
            ws_col: PTY_COLS as u16,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        // SAFETY: openpty initializes both descriptors and only reads the winsize value.
        if unsafe {
            libc::openpty(
                &mut master_fd,
                &mut slave_fd,
                std::ptr::null_mut(),
                std::ptr::null(),
                &size,
            )
        } == -1
        {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: fcntl only changes the status flags of the valid master descriptor.
        if unsafe { libc::fcntl(master_fd, libc::F_SETFL, libc::O_NONBLOCK) } == -1 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: dup returns independently owned descriptors for stdout and stderr.
        let stdout_fd = unsafe { libc::dup(slave_fd) };
        let stderr_fd = unsafe { libc::dup(slave_fd) };
        if stdout_fd == -1 || stderr_fd == -1 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: every descriptor was freshly returned by openpty/dup and is owned here.
        let stdin = unsafe { File::from_raw_fd(slave_fd) };
        // SAFETY: see the ownership note above.
        let stdout = unsafe { File::from_raw_fd(stdout_fd) };
        // SAFETY: see the ownership note above.
        let stderr = unsafe { File::from_raw_fd(stderr_fd) };
        // SAFETY: the master descriptor is independently owned by the parent process.
        let master = unsafe { File::from_raw_fd(master_fd) };
        let mut command = Command::new(executable);
        command
            .args([
                "--provider",
                "open-ai-compatible",
                "--model",
                "tui-stress-model",
                "start the responsiveness stream",
            ])
            .current_dir(cwd)
            .env("HOME", cwd)
            .env("XDG_CONFIG_HOME", config_home)
            .env("BORG_HOME", borg_home)
            .env("BORG_LIMITS", "0")
            .env("BORG_TUI", "1")
            .env("BORG_TUI_SCREEN", "alternate")
            .env("BORG_TUI_FPS", "165")
            .env("TERM", "xterm-256color")
            .env("BORG_OPENAI_COMPATIBLE_BASE_URL", endpoint)
            .env("BORG_OPENAI_COMPATIBLE_MODEL", "tui-stress-model")
            .env("BORG_OPENAI_COMPATIBLE_API_KEY", "local-test")
            .env("BORG_OPENAI_COMPATIBLE_CONTEXT_WINDOW_TOKENS", "1000000")
            .env("RUST_LOG", "borg=debug")
            .stdin(Stdio::from(stdin))
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
        // SAFETY: this closure runs after fork and before exec. It creates a
        // fresh session and makes the already-wired PTY stdin its controlling
        // terminal so crossterm reads the same input stream the harness writes.
        unsafe {
            command.pre_exec(|| {
                if libc::setsid() == -1 {
                    return Err(io::Error::last_os_error());
                }
                if libc::ioctl(libc::STDIN_FILENO, libc::TIOCSCTTY, 0) == -1 {
                    return Err(io::Error::last_os_error());
                }
                if libc::tcsetpgrp(libc::STDIN_FILENO, libc::getpgrp()) == -1 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let child = command.spawn()?;
        Ok(Self {
            child,
            master,
            log_path: borg_home.join("logs/borg.log"),
            screen: VirtualScreen::new(PTY_ROWS, PTY_COLS),
        })
    }

    fn wait_for_output(&mut self, timeout: Duration) -> io::Result<()> {
        let deadline = Instant::now() + timeout;
        let mut received = 0_usize;
        let mut output = Vec::new();
        while Instant::now() < deadline {
            received += self.read_available(Some(&mut output))?;
            if received >= 256 {
                return Ok(());
            }
            if let Some(status) = self.child.try_wait()? {
                return Err(io::Error::other(format!(
                    "Borg exited before first paint: {status}; output={}",
                    String::from_utf8_lossy(&output)
                )));
            }
            thread::sleep(Duration::from_millis(2));
        }
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "Borg did not paint the TUI",
        ))
    }

    fn wait_for_pattern(&mut self, pattern: &[u8], timeout: Duration) -> io::Result<()> {
        let deadline = Instant::now() + timeout;
        let mut output = Vec::new();
        while Instant::now() < deadline {
            self.read_available(Some(&mut output))?;
            if output
                .windows(pattern.len())
                .any(|window| window == pattern)
            {
                return Ok(());
            }
            if let Some(status) = self.child.try_wait()? {
                return Err(io::Error::other(format!(
                    "Borg exited while waiting for active streaming: {status}"
                )));
            }
            thread::sleep(Duration::from_millis(2));
        }
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            "Borg did not paint the active streaming screen",
        ))
    }

    fn type_and_measure(&mut self, marker: &str, timeout: Duration) -> io::Result<Duration> {
        let started = Instant::now();
        self.write_all_retry(marker.as_bytes())?;
        let deadline = started + timeout;
        let mut output = Vec::new();
        while Instant::now() < deadline {
            self.read_available(Some(&mut output))?;
            if self.screen.contains(marker) {
                return Ok(started.elapsed());
            }
            if let Some(status) = self.child.try_wait()? {
                return Err(io::Error::other(format!(
                    "Borg exited during latency probe: {status}"
                )));
            }
            thread::sleep(Duration::from_millis(1));
        }
        Err(io::Error::new(
            io::ErrorKind::TimedOut,
            format!(
                "TUI did not paint marker {marker}; screen={:?}; output tail={:?}; log={}",
                self.screen.text(),
                String::from_utf8_lossy(&output[output.len().saturating_sub(4096)..]),
                fs::read_to_string(&self.log_path).unwrap_or_default(),
            ),
        ))
    }

    fn drain_output(&mut self) -> io::Result<()> {
        while self.read_available(None)? > 0 {}
        Ok(())
    }

    fn cpu_time(&self) -> io::Result<Duration> {
        let stat = fs::read_to_string(format!("/proc/{}/stat", self.child.id()))?;
        let fields = stat
            .rsplit_once(") ")
            .ok_or_else(|| io::Error::other("malformed process stat"))?
            .1
            .split_whitespace()
            .collect::<Vec<_>>();
        let user_ticks = fields
            .get(11)
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| io::Error::other("missing process user CPU time"))?;
        let system_ticks = fields
            .get(12)
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| io::Error::other("missing process system CPU time"))?;
        // SAFETY: sysconf reads the host's immutable clock-tick setting.
        let ticks_per_second = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
        if ticks_per_second <= 0 {
            return Err(io::Error::other("invalid process clock-tick rate"));
        }
        Ok(Duration::from_secs_f64(
            (user_ticks + system_ticks) as f64 / ticks_per_second as f64,
        ))
    }

    fn read_available(&mut self, mut output: Option<&mut Vec<u8>>) -> io::Result<usize> {
        let mut total = 0;
        loop {
            let mut chunk = [0_u8; 16 * 1024];
            match self.master.read(&mut chunk) {
                Ok(0) => break,
                Ok(read) => {
                    total += read;
                    self.screen.feed(&chunk[..read]);
                    if let Some(output) = output.as_deref_mut() {
                        output.extend_from_slice(&chunk[..read]);
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) if error.raw_os_error() == Some(libc::EIO) => break,
                Err(error) => return Err(error),
            }
        }
        Ok(total)
    }

    fn write_all_retry(&mut self, mut bytes: &[u8]) -> io::Result<()> {
        while !bytes.is_empty() {
            match self.master.write(bytes) {
                Ok(0) => return Err(io::ErrorKind::WriteZero.into()),
                Ok(written) => bytes = &bytes[written..],
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(1));
                }
                Err(error) => return Err(error),
            }
        }
        self.master.flush()
    }
}

struct VirtualScreen {
    parser: termwiz::escape::parser::Parser,
    cells: Vec<Vec<char>>,
    row: usize,
    col: usize,
}

impl VirtualScreen {
    fn new(rows: usize, cols: usize) -> Self {
        Self {
            parser: termwiz::escape::parser::Parser::new(),
            cells: vec![vec![' '; cols]; rows],
            row: 0,
            col: 0,
        }
    }

    fn feed(&mut self, bytes: &[u8]) {
        for action in self.parser.parse_as_vec(bytes) {
            self.apply(action);
        }
    }

    fn contains(&self, text: &str) -> bool {
        let needle = text.chars().collect::<Vec<_>>();
        self.cells
            .iter()
            .any(|row| row.windows(needle.len()).any(|window| window == needle))
    }

    fn text(&self) -> String {
        self.cells
            .iter()
            .map(|row| row.iter().collect::<String>().trim_end().to_string())
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn apply(&mut self, action: termwiz::escape::Action) {
        use termwiz::escape::csi::{Cursor, Edit, EraseInDisplay, EraseInLine};
        use termwiz::escape::{Action, CSI, ControlCode};

        match action {
            Action::Print(character) => self.put(character),
            Action::PrintString(text) => {
                for character in text.chars() {
                    self.put(character);
                }
            }
            Action::Control(ControlCode::CarriageReturn) => self.col = 0,
            Action::Control(ControlCode::LineFeed) => {
                self.row = (self.row + 1).min(self.cells.len().saturating_sub(1));
            }
            Action::Control(ControlCode::Backspace) => self.col = self.col.saturating_sub(1),
            Action::CSI(CSI::Cursor(Cursor::Position { line, col }))
            | Action::CSI(CSI::Cursor(Cursor::CharacterAndLinePosition { line, col })) => {
                self.row = line.as_zero_based() as usize;
                self.col = col.as_zero_based() as usize;
                self.clamp_cursor();
            }
            Action::CSI(CSI::Cursor(Cursor::CharacterAbsolute(col)))
            | Action::CSI(CSI::Cursor(Cursor::CharacterPositionAbsolute(col))) => {
                self.col = col.as_zero_based() as usize;
                self.clamp_cursor();
            }
            Action::CSI(CSI::Cursor(Cursor::LinePositionAbsolute(line))) => {
                self.row = line.saturating_sub(1) as usize;
                self.clamp_cursor();
            }
            Action::CSI(CSI::Cursor(Cursor::Left(amount)))
            | Action::CSI(CSI::Cursor(Cursor::CharacterPositionBackward(amount))) => {
                self.col = self.col.saturating_sub(amount as usize);
            }
            Action::CSI(CSI::Cursor(Cursor::Right(amount)))
            | Action::CSI(CSI::Cursor(Cursor::CharacterPositionForward(amount))) => {
                self.col = self.col.saturating_add(amount as usize);
                self.clamp_cursor();
            }
            Action::CSI(CSI::Cursor(Cursor::Up(amount)))
            | Action::CSI(CSI::Cursor(Cursor::LinePositionBackward(amount))) => {
                self.row = self.row.saturating_sub(amount as usize);
            }
            Action::CSI(CSI::Cursor(Cursor::Down(amount)))
            | Action::CSI(CSI::Cursor(Cursor::LinePositionForward(amount))) => {
                self.row = self.row.saturating_add(amount as usize);
                self.clamp_cursor();
            }
            Action::CSI(CSI::Edit(Edit::EraseInLine(mode))) => match mode {
                EraseInLine::EraseToEndOfLine => self.cells[self.row][self.col..].fill(' '),
                EraseInLine::EraseToStartOfLine => self.cells[self.row][..=self.col].fill(' '),
                EraseInLine::EraseLine => self.cells[self.row].fill(' '),
            },
            Action::CSI(CSI::Edit(Edit::EraseInDisplay(mode))) => match mode {
                EraseInDisplay::EraseDisplay | EraseInDisplay::EraseScrollback => {
                    for row in &mut self.cells {
                        row.fill(' ');
                    }
                }
                EraseInDisplay::EraseToEndOfDisplay => {
                    self.cells[self.row][self.col..].fill(' ');
                    for row in self.cells.iter_mut().skip(self.row + 1) {
                        row.fill(' ');
                    }
                }
                EraseInDisplay::EraseToStartOfDisplay => {
                    for row in self.cells.iter_mut().take(self.row) {
                        row.fill(' ');
                    }
                    self.cells[self.row][..=self.col].fill(' ');
                }
            },
            _ => {}
        }
    }

    fn put(&mut self, character: char) {
        self.cells[self.row][self.col] = character;
        let width = unicode_width::UnicodeWidthChar::width(character)
            .unwrap_or(0)
            .max(1);
        for offset in 1..width {
            if self.col + offset < self.cells[self.row].len() {
                self.cells[self.row][self.col + offset] = ' ';
            }
        }
        self.col += width;
        if self.col >= self.cells[self.row].len() {
            self.col = 0;
            self.row = (self.row + 1).min(self.cells.len().saturating_sub(1));
        }
    }

    fn clamp_cursor(&mut self) {
        self.row = self.row.min(self.cells.len().saturating_sub(1));
        self.col = self.col.min(self.cells[self.row].len().saturating_sub(1));
    }
}

impl Drop for PtyChild {
    fn drop(&mut self) {
        let _ = self.write_all_retry(b"\x15\x03\x03");
        for _ in 0..20 {
            if self.child.try_wait().ok().flatten().is_some() {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
