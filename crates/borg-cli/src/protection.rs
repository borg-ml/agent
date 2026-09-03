use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Output};

use anyhow::{Context, Result, bail, ensure};
use regex::Regex;
use serde_json::json;

use crate::cli::{LimitsProtectArgs, LimitsProtectCommand};

const DROP_IN_NAME: &str = "50-borg-protection.conf";
const UNIT_MARKER: &str = "# Managed by Borg Agent protected service";
const PROCESS_MARKER: &str = "# ProcessName=";
const POLICY_VERSION: u32 = 1;
const CPU_WEIGHT: u64 = 200;
const IO_WEIGHT: u64 = 200;
const PROGRESSIVE_RESTART_VERSION: u32 = 254;
const RESTART_STEPS: u64 = 8;

enum ManagedDropIn {
    Absent,
    Borg(String),
    Foreign,
}

struct ProtectionEnvironment {
    controllers: BTreeSet<String>,
    progressive_restart: bool,
}

impl ProtectionEnvironment {
    fn cpu_supported(&self) -> bool {
        self.controllers.contains("cpu")
    }

    fn io_supported(&self) -> bool {
        self.controllers.contains("io")
    }
}

struct ProtectionStatus {
    service: String,
    state: Option<String>,
    ready: bool,
    earlyoom_unprotected: Vec<String>,
    error: Option<String>,
}

pub(crate) fn run(args: LimitsProtectArgs, json_output: bool) -> Result<()> {
    match args.command.unwrap_or(LimitsProtectCommand::List) {
        LimitsProtectCommand::Add { service } => add(&service, json_output),
        LimitsProtectCommand::List => list(json_output),
        LimitsProtectCommand::Remove { service } => remove(&service, json_output),
    }
}

pub(crate) fn configured_service_names() -> Vec<String> {
    configured_drop_ins()
        .unwrap_or_default()
        .into_iter()
        .map(|(service, _)| service)
        .collect()
}

fn add(service: &str, json_output: bool) -> Result<()> {
    ensure!(
        cfg!(target_os = "linux"),
        "protected services currently require Linux with a systemd user manager"
    );
    let service = normalize_service_name(service)?;
    let environment = detect_environment()?;
    let service_properties = service_properties(&service, environment.progressive_restart)?;
    ensure!(
        service_properties
            .get("LoadState")
            .is_some_and(|state| state == "loaded"),
        "{service} is not an installed systemd user service; Borg can supervise services, not arbitrary process names"
    );
    ensure!(
        service_properties
            .get("Type")
            .is_none_or(|kind| kind != "oneshot"),
        "{service} is a one-shot unit and cannot use automatic restart protection"
    );
    let path = drop_in_path(&service).context("HOME or XDG_CONFIG_HOME is required")?;
    ensure_safe_parent(&path)?;
    let previous = match read_managed_drop_in(&path)? {
        ManagedDropIn::Absent => None,
        ManagedDropIn::Borg(source) => Some(source),
        ManagedDropIn::Foreign => bail!(
            "{} already exists and is not managed by Borg; it was left untouched",
            path.display()
        ),
    };
    let mut process_names = service_process_names(&service, &service_properties);
    if let Some(source) = previous.as_deref() {
        process_names.extend(stored_process_names(source));
    }
    let source = drop_in_source(&environment, &process_names);
    let unchanged = previous.as_deref() == Some(source.as_str());
    write_atomic(&path, source.as_bytes())?;

    let apply = (|| -> Result<()> {
        systemctl(&["daemon-reload"])?;
        verify_effective(&service, &path, &environment)
    })();
    if let Err(error) = apply {
        restore_drop_in(&path, previous.as_deref());
        return Err(error).context(format!(
            "{service} was not protected; its previous configuration was restored"
        ));
    }
    let state = service_state(&service).ok();
    let earlyoom_unprotected = earlyoom_unprotected_processes(&process_names)?;
    let ready = earlyoom_unprotected.is_empty();

    if json_output {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "service": service,
                "protected": true,
                "ready": ready,
                "state": state,
                "process_names": process_names,
                "earlyoom_unprotected_processes": earlyoom_unprotected,
                "unit": path,
            }))?
        );
    } else if unchanged {
        println!("Protection for {service} is already current.");
        print_state_guidance(&service, state.as_deref());
    } else {
        println!("Protected {service}.");
        println!("  It restarts after failure with a bounded retry delay.");
        if environment.cpu_supported() {
            println!("  It receives higher CPU priority when the machine is busy.");
        }
        if environment.io_supported() {
            println!("  It receives higher disk I/O priority under contention.");
        }
        println!("  Protection applied without restarting the service.");
        print_state_guidance(&service, state.as_deref());
    }
    if !json_output && !earlyoom_unprotected.is_empty() {
        println!(
            "  Attention: the active earlyoom policy can still select: {}.",
            earlyoom_unprotected.join(", ")
        );
        println!("  Add those names to earlyoom's `--ignore` regex, then restart earlyoom.");
    }
    Ok(())
}

fn list(json_output: bool) -> Result<()> {
    let entries = configured_drop_ins()?;
    let statuses = entries
        .into_iter()
        .map(|(service, path)| protection_status(service, path))
        .collect::<Vec<_>>();
    if json_output {
        let services = statuses
            .iter()
            .map(|status| {
                json!({
                    "service": status.service,
                    "state": status.state,
                    "ready": status.ready,
                    "earlyoom_unprotected_processes": status.earlyoom_unprotected,
                    "error": status.error,
                })
            })
            .collect::<Vec<_>>();
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({ "protected_services": services }))?
        );
        return Ok(());
    }

    if statuses.is_empty() {
        println!("No user services are protected by Borg.");
        println!("Run `borg limits protect add <service>` to add one.");
        return Ok(());
    }
    println!("Protected user services:");
    for status in statuses {
        let summary = match (status.ready, status.state.as_deref()) {
            (true, Some("active" | "activating" | "reloading")) => "active and protected",
            (true, Some("failed")) => "protected, but the service has failed",
            (true, Some("inactive")) => "protected; currently inactive",
            (true, Some(_)) => "protected",
            _ => "configured but needs attention",
        };
        println!("  {}: {summary}.", status.service);
        if status.state.as_deref() == Some("failed") {
            println!(
                "    Recover with `systemctl --user reset-failed {0} && systemctl --user start {0}`.",
                status.service
            );
        } else if status.state.as_deref() == Some("inactive") {
            println!(
                "    Start or enable the service when wanted; Borg does not change session startup."
            );
        }
        if let Some(error) = status.error {
            println!("    {error}");
        }
    }
    println!(
        "Protection adds restart and resource priority; it is not a security boundary from other processes running as your user."
    );
    Ok(())
}

fn remove(service: &str, json_output: bool) -> Result<()> {
    let service = normalize_service_name(service)?;
    let path = drop_in_path(&service).context("HOME or XDG_CONFIG_HOME is required")?;
    ensure_safe_parent(&path)?;
    match read_managed_drop_in(&path)? {
        ManagedDropIn::Absent => {
            if json_output {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "service": service,
                        "protected": false,
                        "changed": false,
                    }))?
                );
            } else {
                println!("{service} is not protected by Borg.");
            }
            return Ok(());
        }
        ManagedDropIn::Borg(_) => {}
        ManagedDropIn::Foreign => bail!(
            "{} is not managed by Borg and was left untouched",
            path.display()
        ),
    };
    fs::remove_file(&path).with_context(|| format!("failed to remove {}", path.display()))?;
    let reload = systemctl(&["daemon-reload"]);
    if let Err(error) = reload {
        remove_empty_parent(&path);
        if json_output {
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "service": service,
                    "protected": false,
                    "changed": true,
                    "reload_warning": format!("{error:#}"),
                }))?
            );
        } else {
            println!("Removed Borg's saved protection for {service}.");
            println!(
                "  The user systemd manager could not reload, so the running manager may retain it until your next login: {error:#}"
            );
        }
        return Ok(());
    }
    remove_empty_parent(&path);

    if json_output {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "service": service,
                "protected": false,
                "changed": true,
            }))?
        );
    } else {
        println!("Removed Borg protection from {service}.");
        println!("  The service was not interrupted.");
    }
    Ok(())
}

fn protection_status(service: String, path: PathBuf) -> ProtectionStatus {
    let source = match read_managed_drop_in(&path) {
        Ok(ManagedDropIn::Borg(source)) => source,
        Ok(_) => {
            return ProtectionStatus {
                service,
                state: None,
                ready: false,
                earlyoom_unprotected: Vec::new(),
                error: Some("the protection drop-in is no longer managed by Borg".to_string()),
            };
        }
        Err(error) => {
            return ProtectionStatus {
                service,
                state: None,
                ready: false,
                earlyoom_unprotected: Vec::new(),
                error: Some(format!("failed to read protection: {error:#}")),
            };
        }
    };
    let environment = match detect_environment() {
        Ok(environment) => environment,
        Err(error) => {
            return ProtectionStatus {
                service,
                state: None,
                ready: false,
                earlyoom_unprotected: Vec::new(),
                error: Some(format!("cannot reach effective protection: {error:#}")),
            };
        }
    };
    let properties = match service_properties(&service, environment.progressive_restart) {
        Ok(properties) => properties,
        Err(error) => {
            return ProtectionStatus {
                service,
                state: None,
                ready: false,
                earlyoom_unprotected: Vec::new(),
                error: Some(format!("cannot inspect service: {error:#}")),
            };
        }
    };
    let state = properties.get("ActiveState").cloned();
    let properties_ready = properties_match(&properties, &path, &environment);
    let mut process_names = stored_process_names(&source);
    process_names.extend(service_process_names(&service, &properties));
    let (earlyoom_unprotected, earlyoom_error) =
        match earlyoom_unprotected_processes(&process_names) {
            Ok(names) => (names, None),
            Err(error) => (
                Vec::new(),
                Some(format!(
                    "cannot inspect the active earlyoom policy: {error:#}"
                )),
            ),
        };
    let ready = properties_ready && earlyoom_unprotected.is_empty() && earlyoom_error.is_none();
    let error = if !properties_ready {
        Some("effective systemd policy differs; run `borg limits protect add` again".to_string())
    } else if !earlyoom_unprotected.is_empty() {
        Some(format!(
            "active earlyoom can still select {}; add these names to its `--ignore` regex",
            earlyoom_unprotected.join(", ")
        ))
    } else {
        earlyoom_error
    };
    ProtectionStatus {
        service,
        state,
        ready,
        earlyoom_unprotected,
        error,
    }
}

fn drop_in_source(environment: &ProtectionEnvironment, process_names: &BTreeSet<String>) -> String {
    let mut source = format!("{UNIT_MARKER}\n# PolicyVersion={POLICY_VERSION}\n");
    for process_name in process_names {
        source.push_str(&format!("{PROCESS_MARKER}{process_name}\n"));
    }
    source.push_str("\n[Unit]\nStartLimitIntervalSec=0\n\n[Service]\nRestart=on-failure\n");
    if environment.progressive_restart {
        source.push_str("RestartSec=1s\nRestartSteps=8\nRestartMaxDelaySec=1min\n");
    } else {
        source.push_str("RestartSec=5s\n");
    }
    source.push_str("OOMPolicy=stop\n");
    if environment.cpu_supported() {
        source.push_str(&format!("CPUWeight={CPU_WEIGHT}\n"));
    }
    if environment.io_supported() {
        source.push_str(&format!("IOWeight={IO_WEIGHT}\n"));
    }
    source
}

fn verify_effective(service: &str, path: &Path, environment: &ProtectionEnvironment) -> Result<()> {
    let properties = service_properties(service, environment.progressive_restart)?;
    ensure!(
        properties_match(&properties, path, environment),
        "{service} loaded, but its effective restart or resource policy does not match Borg's protection"
    );
    Ok(())
}

fn properties_match(
    properties: &BTreeMap<String, String>,
    path: &Path,
    environment: &ProtectionEnvironment,
) -> bool {
    let required = [
        ("LoadState", "loaded".to_string()),
        ("Restart", "on-failure".to_string()),
        ("OOMPolicy", "stop".to_string()),
        ("StartLimitIntervalUSec", "0".to_string()),
    ];
    if required
        .iter()
        .any(|(name, expected)| properties.get(*name) != Some(expected))
    {
        return false;
    }
    if !properties.get("DropInPaths").is_some_and(|paths| {
        paths
            .split_whitespace()
            .any(|loaded| same_file_path(Path::new(loaded), path))
    }) {
        return false;
    }
    if environment.cpu_supported() && properties.get("CPUWeight") != Some(&CPU_WEIGHT.to_string()) {
        return false;
    }
    if environment.io_supported() && properties.get("IOWeight") != Some(&IO_WEIGHT.to_string()) {
        return false;
    }
    if environment.progressive_restart {
        if properties.get("RestartSteps") != Some(&RESTART_STEPS.to_string())
            || properties.get("RestartMaxDelayUSec") != Some(&"1min".to_string())
        {
            return false;
        }
    } else if properties.get("RestartUSec") != Some(&"5s".to_string()) {
        return false;
    }
    true
}

fn service_properties(
    service: &str,
    progressive_restart: bool,
) -> Result<BTreeMap<String, String>> {
    let properties = if progressive_restart {
        "LoadState,ActiveState,Type,Restart,RestartUSec,RestartSteps,RestartMaxDelayUSec,OOMPolicy,CPUWeight,IOWeight,StartLimitIntervalUSec,DropInPaths,ControlGroup"
    } else {
        "LoadState,ActiveState,Type,Restart,RestartUSec,OOMPolicy,CPUWeight,IOWeight,StartLimitIntervalUSec,DropInPaths,ControlGroup"
    };
    let output = systemctl_output(&["show", service, &format!("--property={properties}")])?;
    Ok(parse_key_values(&String::from_utf8_lossy(&output.stdout)))
}

fn service_state(service: &str) -> Result<String> {
    let output = systemctl_output(&["show", service, "--property=ActiveState", "--value"])?;
    let state = String::from_utf8(output.stdout).context("systemd returned a non-UTF-8 state")?;
    let state = state.trim();
    ensure!(
        !state.is_empty(),
        "systemd did not report the service state"
    );
    Ok(state.to_string())
}

fn service_process_names(service: &str, properties: &BTreeMap<String, String>) -> BTreeSet<String> {
    let mut names = BTreeSet::new();
    if let Some(stem) = service.strip_suffix(".service") {
        let stem = stem.split('@').next().unwrap_or(stem);
        if !stem.is_empty() {
            names.insert(stem.to_string());
        }
    }
    let Some(control_group) = properties
        .get("ControlGroup")
        .filter(|group| group.starts_with('/') && !group.contains(".."))
    else {
        return names;
    };
    let cgroup = Path::new("/sys/fs/cgroup").join(control_group.trim_start_matches('/'));
    let Ok(processes) = fs::read_to_string(cgroup.join("cgroup.procs")) else {
        return names;
    };
    for pid in processes.lines().filter_map(|pid| pid.parse::<u32>().ok()) {
        let Ok(name) = fs::read_to_string(format!("/proc/{pid}/comm")) else {
            continue;
        };
        let name = name.trim();
        if !name.is_empty() {
            names.insert(name.to_string());
        }
    }
    names
}

fn stored_process_names(source: &str) -> BTreeSet<String> {
    source
        .lines()
        .filter_map(|line| line.strip_prefix(PROCESS_MARKER))
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(str::to_string)
        .collect()
}

fn earlyoom_unprotected_processes(process_names: &BTreeSet<String>) -> Result<Vec<String>> {
    let output = systemctl_system_output(&[
        "show",
        "earlyoom.service",
        "--property=LoadState,ActiveState,MainPID",
    ])?;
    let properties = parse_key_values(&String::from_utf8_lossy(&output.stdout));
    if properties
        .get("LoadState")
        .is_none_or(|state| state != "loaded")
        || properties
            .get("ActiveState")
            .is_none_or(|state| state != "active")
    {
        return Ok(Vec::new());
    }
    let pid = properties
        .get("MainPID")
        .context("earlyoom did not report a main process")?
        .parse::<u32>()
        .context("earlyoom returned an invalid main process")?;
    ensure!(pid > 0, "earlyoom has no running main process");
    let command_line = fs::read(format!("/proc/{pid}/cmdline"))
        .context("failed to read the active earlyoom command line")?;
    let arguments = command_line
        .split(|byte| *byte == 0)
        .filter(|argument| !argument.is_empty())
        .map(|argument| String::from_utf8_lossy(argument).into_owned())
        .collect::<Vec<_>>();
    let safe_patterns = earlyoom_ignore_patterns(&arguments)?;
    Ok(unprotected_processes(process_names, &safe_patterns))
}

fn earlyoom_ignore_patterns(arguments: &[String]) -> Result<Vec<Regex>> {
    let mut safe_patterns = Vec::new();
    let mut index = 0;
    while index < arguments.len() {
        let argument = &arguments[index];
        if argument == "--ignore" {
            if let Some(pattern) = arguments.get(index + 1) {
                safe_patterns.push(pattern.clone());
                index += 1;
            }
        } else if let Some((option, pattern)) = argument.split_once('=')
            && option == "--ignore"
        {
            safe_patterns.push(pattern.to_string());
        }
        index += 1;
    }
    let safe_patterns = safe_patterns
        .into_iter()
        .map(|pattern| {
            Regex::new(&pattern).with_context(|| format!("invalid earlyoom regex `{pattern}`"))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(safe_patterns)
}

fn unprotected_processes(process_names: &BTreeSet<String>, safe_patterns: &[Regex]) -> Vec<String> {
    process_names
        .iter()
        .filter(|name| !safe_patterns.iter().any(|pattern| pattern.is_match(name)))
        .cloned()
        .collect()
}

fn detect_environment() -> Result<ProtectionEnvironment> {
    Ok(ProtectionEnvironment {
        controllers: delegated_controllers().unwrap_or_default(),
        progressive_restart: systemd_version()
            .is_some_and(|version| version >= PROGRESSIVE_RESTART_VERSION),
    })
}

fn delegated_controllers() -> Result<BTreeSet<String>> {
    ensure!(
        Path::new("/sys/fs/cgroup/cgroup.controllers").is_file(),
        "cgroup v2 is not available"
    );
    let output = systemctl_output(&["show", "app.slice", "--property=ControlGroup", "--value"])?;
    let control_group = String::from_utf8(output.stdout)
        .context("systemctl returned a non-UTF-8 control-group path")?;
    let control_group = control_group.trim();
    ensure!(
        !control_group.is_empty(),
        "the user manager did not expose app.slice"
    );
    let path = Path::new("/sys/fs/cgroup").join(control_group.trim_start_matches('/'));
    let controllers = fs::read_to_string(path.join("cgroup.controllers"))
        .with_context(|| format!("failed to inspect controllers delegated to {control_group}"))?
        .split_whitespace()
        .map(str::to_string)
        .collect();
    Ok(controllers)
}

fn systemd_version() -> Option<u32> {
    let output = ProcessCommand::new("systemctl")
        .arg("--version")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()?
        .lines()
        .next()?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}

fn configured_drop_ins() -> Result<Vec<(String, PathBuf)>> {
    let Some(root) = manager_unit_directory().or_else(fallback_unit_directory) else {
        return Ok(Vec::new());
    };
    let entries = match fs::read_dir(&root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(error).with_context(|| format!("failed to read {}", root.display()));
        }
    };
    let mut protected = Vec::new();
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name
            .to_str()
            .and_then(|name| name.strip_suffix(".service.d"))
        else {
            continue;
        };
        let path = entry.path().join(DROP_IN_NAME);
        if matches!(read_managed_drop_in(&path), Ok(ManagedDropIn::Borg(_))) {
            protected.push((format!("{name}.service"), path));
        }
    }
    protected.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(protected)
}

fn drop_in_path(service: &str) -> Option<PathBuf> {
    manager_unit_directory()
        .or_else(fallback_unit_directory)
        .map(|root| root.join(format!("{service}.d")).join(DROP_IN_NAME))
}

fn manager_unit_directory() -> Option<PathBuf> {
    if !cfg!(target_os = "linux") {
        return None;
    }
    let output = systemctl_output(&["show", "--property=UnitPath", "--value"]).ok()?;
    String::from_utf8(output.stdout)
        .ok()?
        .split_whitespace()
        .map(PathBuf::from)
        .find(|path| {
            path.ends_with("systemd/user")
                && !["/etc", "/run", "/usr", "/var"]
                    .iter()
                    .any(|root| path.starts_with(root))
        })
}

fn fallback_unit_directory() -> Option<PathBuf> {
    std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))
        .map(|root| root.join("systemd/user"))
}

fn read_managed_drop_in(path: &Path) -> Result<ManagedDropIn> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ManagedDropIn::Absent);
        }
        Err(error) => {
            return Err(error).with_context(|| format!("failed to inspect {}", path.display()));
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Ok(ManagedDropIn::Foreign);
    }
    let source =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    if source.starts_with(UNIT_MARKER) {
        Ok(ManagedDropIn::Borg(source))
    } else {
        Ok(ManagedDropIn::Foreign)
    }
}

fn ensure_safe_parent(path: &Path) -> Result<()> {
    let parent = path
        .parent()
        .context("protection drop-in path has no parent")?;
    match fs::symlink_metadata(parent) {
        Ok(metadata) => ensure!(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "{} is not a regular directory and was left untouched",
            parent.display()
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(error).with_context(|| format!("failed to inspect {}", parent.display()));
        }
    }
    Ok(())
}

fn write_atomic(path: &Path, contents: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .context("protection drop-in path has no parent")?;
    fs::create_dir_all(parent).with_context(|| format!("failed to create {}", parent.display()))?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent).with_context(|| {
        format!(
            "failed to create a temporary file beside {}",
            path.display()
        )
    })?;
    temporary.as_file_mut().write_all(contents)?;
    temporary.as_file_mut().sync_all()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        temporary
            .as_file_mut()
            .set_permissions(fs::Permissions::from_mode(0o644))?;
    }
    temporary
        .persist(path)
        .map_err(|error| error.error)
        .with_context(|| format!("failed to atomically replace {}", path.display()))?;
    let _ = OpenOptions::new().read(true).open(parent)?.sync_all();
    Ok(())
}

fn restore_drop_in(path: &Path, previous: Option<&str>) {
    if let Some(previous) = previous {
        let _ = write_atomic(path, previous.as_bytes());
    } else {
        let _ = fs::remove_file(path);
        remove_empty_parent(path);
    }
    let _ = systemctl(&["daemon-reload"]);
}

fn remove_empty_parent(path: &Path) {
    if let Some(parent) = path.parent() {
        let _ = fs::remove_dir(parent);
    }
}

fn normalize_service_name(service: &str) -> Result<String> {
    let service = if service.ends_with(".service") {
        service.to_string()
    } else if service.contains('.') {
        bail!("{service} is not a service; use a systemd user service name such as dms.service")
    } else {
        format!("{service}.service")
    };
    ensure!(
        service.len() <= 255
            && !service.starts_with(['-', '.'])
            && !service.ends_with("@.service")
            && service
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || ":_.@\\-".contains(character)),
        "invalid systemd user service name: {service}"
    );
    Ok(service)
}

fn print_state_guidance(service: &str, state: Option<&str>) {
    match state {
        Some("failed") => println!(
            "  The service is failed. Recover with `systemctl --user reset-failed {service} && systemctl --user start {service}`."
        ),
        Some("inactive") => println!(
            "  The service is not running; start or enable it when wanted. Borg does not change session startup."
        ),
        None => println!("  Borg could not read the service's current state."),
        _ => {}
    }
}

fn same_file_path(left: &Path, right: &Path) -> bool {
    match (left.canonicalize(), right.canonicalize()) {
        (Ok(left), Ok(right)) => left == right,
        _ => left == right,
    }
}

fn systemctl(args: &[&str]) -> Result<()> {
    systemctl_output(args).map(|_| ())
}

fn systemctl_output(args: &[&str]) -> Result<Output> {
    let mut command = ProcessCommand::new("systemctl");
    command.arg("--user").args(args);
    configure_systemd_user_bus(&mut command);
    let output = command.output().context("failed to run systemctl --user")?;
    ensure!(
        output.status.success(),
        "systemctl --user {} failed with {}: {}",
        args.join(" "),
        output.status,
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(output)
}

fn systemctl_system_output(args: &[&str]) -> Result<Output> {
    let output = ProcessCommand::new("systemctl")
        .args(args)
        .output()
        .context("failed to run systemctl")?;
    ensure!(
        output.status.success(),
        "systemctl {} failed with {}: {}",
        args.join(" "),
        output.status,
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(output)
}

fn configure_systemd_user_bus(_command: &mut ProcessCommand) {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::MetadataExt;

        let runtime_dir = std::env::var_os("XDG_RUNTIME_DIR")
            .map(PathBuf::from)
            .or_else(|| {
                let home = std::env::var_os("HOME").map(PathBuf::from)?;
                let uid = fs::metadata(home).ok()?.uid();
                let candidate = PathBuf::from(format!("/run/user/{uid}"));
                candidate.is_dir().then_some(candidate)
            });
        let Some(runtime_dir) = runtime_dir else {
            return;
        };
        if std::env::var_os("XDG_RUNTIME_DIR").is_none() {
            _command.env("XDG_RUNTIME_DIR", &runtime_dir);
        }
        if std::env::var_os("DBUS_SESSION_BUS_ADDRESS").is_none() {
            let bus = runtime_dir.join("bus");
            if bus.exists() {
                _command.env(
                    "DBUS_SESSION_BUS_ADDRESS",
                    format!("unix:path={}", bus.display()),
                );
            }
        }
    }
}

fn parse_key_values(source: &str) -> BTreeMap<String, String> {
    source
        .lines()
        .filter_map(|line| {
            let (name, value) = line.split_once('=')?;
            Some((name.trim().to_string(), value.trim().to_string()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn environment() -> ProtectionEnvironment {
        ProtectionEnvironment {
            controllers: ["cpu", "memory"].into_iter().map(str::to_string).collect(),
            progressive_restart: true,
        }
    }

    #[test]
    fn protected_service_policy_recovers_forever_without_capping_memory() {
        let process_names = ["dms", "qs"].into_iter().map(str::to_string).collect();
        let source = drop_in_source(&environment(), &process_names);
        assert!(source.starts_with(UNIT_MARKER));
        assert!(source.contains("# ProcessName=dms"));
        assert!(source.contains("# ProcessName=qs"));
        assert_eq!(stored_process_names(&source), process_names);
        assert!(source.contains("Restart=on-failure"));
        assert!(source.contains("StartLimitIntervalSec=0"));
        assert!(source.contains("RestartSteps=8"));
        assert!(source.contains("RestartMaxDelaySec=1min"));
        assert!(source.contains("CPUWeight=200"));
        assert!(!source.contains("MemoryMax="));
        assert!(!source.contains("MemoryLow="));

        let source = drop_in_source(
            &ProtectionEnvironment {
                controllers: BTreeSet::new(),
                progressive_restart: false,
            },
            &BTreeSet::new(),
        );
        assert!(source.contains("RestartSec=5s"));
        assert!(!source.contains("RestartSteps="));
    }

    #[test]
    fn service_names_are_shell_friendly_but_strict() {
        assert_eq!(normalize_service_name("dms").unwrap(), "dms.service");
        assert_eq!(
            normalize_service_name("worker@main.service").unwrap(),
            "worker@main.service"
        );
        assert!(normalize_service_name("../../dms").is_err());
        assert!(normalize_service_name("dms.timer").is_err());
    }

    #[test]
    fn earlyoom_must_cover_every_recorded_service_process() {
        let arguments = [
            "/usr/bin/earlyoom",
            "--avoid",
            "borg|qs",
            "--ignore=dms|never-kill",
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<Vec<_>>();
        let patterns = earlyoom_ignore_patterns(&arguments).unwrap();
        let processes = ["dms", "qs", "never-kill"]
            .into_iter()
            .map(str::to_string)
            .collect();
        assert_eq!(unprotected_processes(&processes, &patterns), ["qs"]);
    }

    #[test]
    fn foreign_or_symlinked_drop_ins_are_never_claimed() {
        let directory = tempfile::tempdir().unwrap();
        let foreign = directory.path().join(DROP_IN_NAME);
        fs::write(&foreign, "[Service]\nRestart=always\n").unwrap();
        assert!(matches!(
            read_managed_drop_in(&foreign).unwrap(),
            ManagedDropIn::Foreign
        ));

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            let target = directory.path().join("target.conf");
            let linked = directory.path().join("linked.conf");
            fs::write(&target, format!("{UNIT_MARKER}\n")).unwrap();
            symlink(&target, &linked).unwrap();
            assert!(matches!(
                read_managed_drop_in(&linked).unwrap(),
                ManagedDropIn::Foreign
            ));
        }
    }
}
