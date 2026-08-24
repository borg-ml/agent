use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command as ProcessCommand, Output};

use anyhow::{Context, Result, bail, ensure};
use serde_json::json;
use uuid::Uuid;

use crate::cli::{Command, LimitsArgs, LimitsCommand};

const SLICE_NAME: &str = "app-borg.slice";
const UNIT_MARKER: &str = "# Managed by Borg Agent limits";
const POLICY_VERSION: u32 = 1;
const GIB: u64 = 1024 * 1024 * 1024;
const SESSION_TASKS_MAX: u64 = 8_192;
const AGGREGATE_TASKS_MAX: u64 = 32_768;
const CPU_WEIGHT: u64 = 80;
const IO_WEIGHT: u64 = 50;

#[derive(Debug, Clone, PartialEq, Eq)]
struct LimitPolicy {
    total_memory: u64,
    aggregate_memory_high: u64,
    aggregate_memory_max: u64,
    aggregate_swap_max: u64,
    aggregate_tasks_max: u64,
    session_memory_high: u64,
    session_memory_max: u64,
    session_swap_max: u64,
    session_tasks_max: u64,
}

impl LimitPolicy {
    fn for_total_memory(total_memory: u64) -> Result<Self> {
        ensure!(
            total_memory >= GIB,
            "Borg limits require at least 1 GiB of physical memory"
        );
        let hard_reserve = (total_memory / 8).max(4 * GIB).min(total_memory / 3);
        let soft_reserve = (total_memory / 5).max(6 * GIB).min(total_memory / 2);
        let aggregate_memory_max = total_memory - hard_reserve;
        let aggregate_memory_high = total_memory - soft_reserve;
        let session_memory_max = (total_memory * 4 / 5).min(aggregate_memory_max);
        let session_memory_high = (total_memory * 7 / 10).min(session_memory_max * 9 / 10);
        Ok(Self {
            total_memory,
            aggregate_memory_high,
            aggregate_memory_max,
            aggregate_swap_max: (total_memory / 4).max(2 * GIB),
            aggregate_tasks_max: AGGREGATE_TASKS_MAX,
            session_memory_high,
            session_memory_max,
            session_swap_max: (total_memory / 8).max(GIB),
            session_tasks_max: SESSION_TASKS_MAX,
        })
    }
}

#[derive(Debug)]
struct LimitEnvironment {
    policy: LimitPolicy,
    controllers: BTreeSet<String>,
    swap_controller: bool,
}

impl LimitEnvironment {
    fn memory_supported(&self) -> bool {
        self.controllers.contains("memory")
    }

    fn cpu_supported(&self) -> bool {
        self.controllers.contains("cpu")
    }

    fn io_supported(&self) -> bool {
        self.controllers.contains("io")
    }

    fn pids_supported(&self) -> bool {
        self.controllers.contains("pids")
    }
}

enum ManagedUnit {
    Absent,
    Borg(String),
    Foreign,
}

pub(crate) async fn run(args: LimitsArgs) -> Result<()> {
    let json_output = args.json;
    match args.command.unwrap_or(LimitsCommand::Status) {
        LimitsCommand::Enable => enable(json_output),
        LimitsCommand::Status => status(json_output),
        LimitsCommand::Disable => disable(json_output),
        LimitsCommand::Protect(args) => crate::protection::run(args, json_output),
    }
}

pub(crate) fn reexec_local_agent_if_enabled(command: &Command, no_limits: bool) -> Result<()> {
    if !matches!(command, Command::Agent(_) | Command::Resume { .. }) {
        return Ok(());
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = no_limits;
        return Ok(());
    }
    #[cfg(target_os = "linux")]
    {
        if inside_managed_slice()? {
            return Ok(());
        }
        let Some(path) = managed_unit_path() else {
            return Ok(());
        };
        let ManagedUnit::Borg(source) = read_managed_unit(&path)? else {
            return Ok(());
        };
        if no_limits || environment_bypasses_limits() {
            eprintln!("Warning: Borg limits are bypassed for this run.");
            return Ok(());
        }
        let environment = detect_environment().with_context(|| {
            "Borg limits are enabled but this session cannot reach the Linux user cgroup manager; run `borg limits status`, `borg limits disable`, or retry once with `borg --no-limits` or `BORG_LIMITS=0 borg`"
        })?;
        ensure!(
            environment.memory_supported(),
            "Borg limits are enabled but the memory controller is no longer available; run `borg limits status`, `borg limits disable`, or retry once with `borg --no-limits` or `BORG_LIMITS=0 borg`"
        );
        let expected = slice_unit(&environment);
        if source != expected {
            eprintln!(
                "Warning: Borg's saved limits should be refreshed with `borg limits enable`; continuing with the existing aggregate boundary."
            );
        }
        let executable = std::env::current_exe().context("failed to locate the Borg executable")?;
        let unit = format!("borg-session-{}", Uuid::new_v4());
        let mut process = ProcessCommand::new("systemd-run");
        process.args(scope_args(&environment, &unit));
        process.arg(executable);
        process.args(std::env::args_os().skip(1));
        configure_systemd_user_bus(&mut process);
        use std::os::unix::process::CommandExt;
        let error = process.exec();
        bail!(
            "failed to enter the configured Borg limits: {error}; run `borg limits status`, `borg limits disable`, or retry once with `borg --no-limits` or `BORG_LIMITS=0 borg`"
        );
    }
}

fn enable(json_output: bool) -> Result<()> {
    ensure!(
        cfg!(target_os = "linux"),
        "`borg limits` currently requires Linux with systemd and cgroup v2"
    );
    let environment = detect_environment()?;
    ensure!(
        environment.memory_supported(),
        "the user systemd manager does not have the memory controller; Borg cannot enforce a meaningful memory boundary on this machine"
    );
    let path = managed_unit_path().context("HOME or XDG_CONFIG_HOME is required")?;
    let previous = match read_managed_unit(&path)? {
        ManagedUnit::Absent => None,
        ManagedUnit::Borg(source) => Some(source),
        ManagedUnit::Foreign => bail!(
            "{} already exists and is not managed by Borg; move or rename it before enabling Borg limits",
            path.display()
        ),
    };
    let active_sessions = active_session_names()?;
    let source = slice_unit(&environment);
    verify_unit_source(&source)?;
    write_atomic(&path, source.as_bytes())?;

    let apply = (|| -> Result<()> {
        systemctl(&["daemon-reload"])?;
        if active_sessions.is_empty() {
            systemctl(&["restart", SLICE_NAME])?;
        } else {
            systemctl(&["start", SLICE_NAME])?;
        }
        verify_slice_properties(&environment, &path)?;
        verify_scope(&environment)?;
        Ok(())
    })();
    if let Err(error) = apply {
        restore_unit(&path, previous.as_deref(), active_sessions.is_empty());
        return Err(error)
            .context("Borg limits were not enabled; the previous configuration was restored");
    }

    if json_output {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "enabled": true,
                "ready": true,
                "unit": path,
                "policy": policy_json(&environment),
                "active_sessions": active_sessions.len(),
                "docker_contained": false,
            }))?
        );
    } else {
        println!("Borg limits are enabled and verified.");
        println!(
            "  All sessions share a {} hard boundary; reclaim begins above {}.",
            format_bytes(environment.policy.aggregate_memory_max),
            format_bytes(environment.policy.aggregate_memory_high),
        );
        println!(
            "  Each session has a {} hard boundary; reclaim begins above {}.",
            format_bytes(environment.policy.session_memory_max),
            format_bytes(environment.policy.session_memory_high)
        );
        if environment.cpu_supported() {
            println!("  CPU stays unrestricted and yields only when the machine is busy.");
        } else {
            println!("  CPU priority is unavailable from this user manager.");
        }
        if environment.io_supported() {
            println!("  Disk I/O also yields under contention.");
        } else {
            println!(
                "  Disk I/O priority is unavailable from this user manager; memory limits are still enforced."
            );
        }
        println!(
            "Future `borg` sessions are protected automatically; no wrapper or sudo is needed."
        );
        println!(
            "Docker containers use Docker's own limits and are not contained by this feature."
        );
    }
    Ok(())
}

fn status(json_output: bool) -> Result<()> {
    let Some(path) = managed_unit_path() else {
        return print_disabled_status(json_output, None);
    };
    let source = match read_managed_unit(&path)? {
        ManagedUnit::Absent => return print_disabled_status(json_output, None),
        ManagedUnit::Foreign => {
            return print_disabled_status(json_output, Some(path));
        }
        ManagedUnit::Borg(source) => source,
    };
    let environment = match detect_environment() {
        Ok(environment) => environment,
        Err(error) => {
            if json_output {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&json!({
                        "enabled": true,
                        "ready": false,
                        "unit": path,
                        "error": format!("{error:#}"),
                    }))?
                );
            } else {
                println!("Borg limits are configured but cannot be enforced here.");
                println!("  {error:#}");
                println!(
                    "Run `borg limits disable` to turn them off, or use `borg --no-limits` once."
                );
            }
            return Ok(());
        }
    };
    let current = source == slice_unit(&environment);
    let properties = unit_properties().unwrap_or_default();
    let effective = current && properties_match(&environment, &path, &properties);
    let sessions = active_session_names().unwrap_or_default();
    let protected_services = crate::protection::configured_service_names();
    let events = properties
        .get("ControlGroup")
        .and_then(|group| memory_events(group).ok());

    if json_output {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "enabled": true,
                "ready": effective,
                "current_policy": current,
                "unit": path,
                "policy": policy_json(&environment),
                "active_sessions": sessions,
                "protected_services": protected_services,
                "memory_events": events,
                "inside_limits": inside_managed_slice().unwrap_or(false),
                "docker_contained": false,
            }))?
        );
    } else {
        println!(
            "Borg limits are {}.",
            if effective {
                "on and ready"
            } else {
                "configured but need attention"
            }
        );
        println!(
            "  All sessions: reclaim above {}; hard boundary {}.",
            format_bytes(environment.policy.aggregate_memory_high),
            format_bytes(environment.policy.aggregate_memory_max)
        );
        println!(
            "  Each session: reclaim above {}; hard boundary {}.",
            format_bytes(environment.policy.session_memory_high),
            format_bytes(environment.policy.session_memory_max)
        );
        println!("  Active protected sessions: {}.", sessions.len());
        println!("  Protected user services: {}.", protected_services.len());
        println!(
            "  CPU priority: {}.",
            if environment.cpu_supported() {
                "available; no hard CPU quota"
            } else {
                "not delegated by this OS"
            }
        );
        println!(
            "  I/O priority: {}.",
            if environment.io_supported() {
                "available"
            } else {
                "not delegated by this OS"
            }
        );
        if let Some(events) = events {
            println!(
                "  Since the slice started: reclaimed under pressure {} times; OOM kills {}.",
                events.get("high").copied().unwrap_or(0),
                events.get("oom_kill").copied().unwrap_or(0)
            );
        }
        if !current {
            println!(
                "The saved policy is from another Borg/system configuration; run `borg limits enable` to refresh it."
            );
        } else if !effective {
            println!(
                "The systemd unit does not match the saved policy; run `borg limits enable` to repair it."
            );
        }
        println!("Docker containers use Docker's own limits and are not covered.");
    }
    Ok(())
}

fn disable(json_output: bool) -> Result<()> {
    let path = managed_unit_path().context("HOME or XDG_CONFIG_HOME is required")?;
    match read_managed_unit(&path)? {
        ManagedUnit::Absent => return print_disabled_status(json_output, None),
        ManagedUnit::Foreign => bail!(
            "{} is not managed by Borg and was left untouched",
            path.display()
        ),
        ManagedUnit::Borg(_) => {}
    }
    let sessions = active_session_names().unwrap_or_default();
    fs::remove_file(&path).with_context(|| format!("failed to remove {}", path.display()))?;
    let reload_warning = systemctl(&["daemon-reload"])
        .err()
        .map(|error| format!("{error:#}"));
    if reload_warning.is_none()
        && sessions.is_empty()
        && active_session_names().is_ok_and(|sessions| sessions.is_empty())
    {
        let _ = systemctl(&["stop", SLICE_NAME]);
    }
    if json_output {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "enabled": false,
                "active_sessions_still_limited": sessions,
                "reload_warning": reload_warning,
            }))?
        );
    } else {
        println!("Borg limits are disabled for future sessions.");
        if sessions.is_empty() {
            println!("No protected sessions were running.");
        } else {
            println!(
                "{} active session(s) keep their existing limits until they exit; none were stopped.",
                sessions.len()
            );
        }
        if let Some(warning) = reload_warning {
            println!(
                "The limits file was removed, but the user systemd manager could not reload: {warning}"
            );
        }
        println!("Run `borg limits enable` to restore them.");
        if !crate::protection::configured_service_names().is_empty() {
            println!("Protected user services remain configured.");
        }
    }
    Ok(())
}

fn print_disabled_status(json_output: bool, conflict: Option<PathBuf>) -> Result<()> {
    let active_sessions = if cfg!(target_os = "linux") {
        active_session_names().unwrap_or_default()
    } else {
        Vec::new()
    };
    let protected_services = crate::protection::configured_service_names();
    if json_output {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "enabled": false,
                "ready": false,
                "conflicting_unit": conflict,
                "active_sessions_still_limited": active_sessions,
                "protected_services": protected_services,
            }))?
        );
    } else {
        println!("Borg limits are off.");
        if let Some(path) = conflict {
            println!("  {} exists but is not managed by Borg.", path.display());
        } else if cfg!(target_os = "linux") {
            println!(
                "Run `borg limits enable` to protect local agent workloads. It does not need sudo."
            );
        } else {
            println!("This feature currently requires Linux with systemd and cgroup v2.");
        }
        if !active_sessions.is_empty() {
            println!(
                "{} session(s) that started earlier keep their existing limits until they exit.",
                active_sessions.len()
            );
        }
        if !protected_services.is_empty() {
            println!(
                "{} user service(s) remain protected; inspect them with `borg limits protect list`.",
                protected_services.len()
            );
        }
    }
    Ok(())
}

fn detect_environment() -> Result<LimitEnvironment> {
    ensure!(
        Path::new("/sys/fs/cgroup/cgroup.controllers").is_file(),
        "cgroup v2 is not available; Borg limits require a Linux cgroup v2 host"
    );
    let output = systemctl_output(&["show", "app.slice", "--property=ControlGroup", "--value"])?;
    let control_group = String::from_utf8(output.stdout)
        .context("systemctl returned a non-UTF-8 control-group path")?;
    let control_group = control_group.trim();
    ensure!(
        !control_group.is_empty(),
        "the systemd user manager did not expose app.slice; graphical login or user lingering may be required"
    );
    let cgroup_path = Path::new("/sys/fs/cgroup").join(control_group.trim_start_matches('/'));
    let controllers = fs::read_to_string(cgroup_path.join("cgroup.controllers"))
        .with_context(|| format!("failed to inspect controllers delegated to {control_group}"))?
        .split_whitespace()
        .map(str::to_string)
        .collect::<BTreeSet<_>>();
    let total_memory = quantize_memory(physical_memory_bytes()?);
    Ok(LimitEnvironment {
        policy: LimitPolicy::for_total_memory(total_memory)?,
        controllers,
        swap_controller: cgroup_path.join("memory.swap.max").is_file(),
    })
}

fn physical_memory_bytes() -> Result<u64> {
    let source = fs::read_to_string("/proc/meminfo").context("failed to read /proc/meminfo")?;
    let kib = source
        .lines()
        .find_map(|line| {
            let value = line.strip_prefix("MemTotal:")?;
            value.split_whitespace().next()?.parse::<u64>().ok()
        })
        .context("/proc/meminfo has no MemTotal value")?;
    kib.checked_mul(1024)
        .context("physical memory size overflow")
}

fn quantize_memory(bytes: u64) -> u64 {
    ((bytes.saturating_add(GIB / 2)) / GIB).max(1) * GIB
}

fn slice_unit(environment: &LimitEnvironment) -> String {
    let policy = &environment.policy;
    let mut source = format!(
        "{UNIT_MARKER}\n# PolicyVersion={POLICY_VERSION}\n# PhysicalMemory={}\n# Re-run `borg limits enable` after changing installed RAM.\n\n[Unit]\nDescription=Borg Agent aggregate resource limits\n\n[Slice]\nMemoryAccounting=yes\nMemoryHigh={}\nMemoryMax={}\n",
        policy.total_memory, policy.aggregate_memory_high, policy.aggregate_memory_max
    );
    if environment.swap_controller {
        source.push_str(&format!("MemorySwapMax={}\n", policy.aggregate_swap_max));
    }
    if environment.cpu_supported() {
        source.push_str(&format!("CPUWeight={CPU_WEIGHT}\n"));
    }
    if environment.io_supported() {
        source.push_str(&format!("IOAccounting=yes\nIOWeight={IO_WEIGHT}\n"));
    }
    if environment.pids_supported() {
        source.push_str(&format!(
            "TasksAccounting=yes\nTasksMax={}\n",
            policy.aggregate_tasks_max
        ));
    }
    source
}

fn scope_args(environment: &LimitEnvironment, unit: &str) -> Vec<OsString> {
    let policy = &environment.policy;
    let mut args = vec![
        "--user".into(),
        "--scope".into(),
        "--quiet".into(),
        "--collect".into(),
        "--same-dir".into(),
        format!("--unit={unit}").into(),
        format!("--slice={SLICE_NAME}").into(),
        "--description=Borg Agent protected session".into(),
        format!("--property=MemoryHigh={}", policy.session_memory_high).into(),
        format!("--property=MemoryMax={}", policy.session_memory_max).into(),
    ];
    if environment.swap_controller {
        args.push(format!("--property=MemorySwapMax={}", policy.session_swap_max).into());
    }
    if environment.cpu_supported() {
        args.push(format!("--property=CPUWeight={CPU_WEIGHT}").into());
    }
    if environment.io_supported() {
        args.push(format!("--property=IOWeight={IO_WEIGHT}").into());
    }
    if environment.pids_supported() {
        args.push(format!("--property=TasksMax={}", policy.session_tasks_max).into());
    }
    args.push("--".into());
    args
}

fn verify_scope(environment: &LimitEnvironment) -> Result<()> {
    let unit = format!("borg-limits-probe-{}", Uuid::new_v4());
    let mut command = ProcessCommand::new("systemd-run");
    command.args(scope_args(environment, &unit));
    command.args([
        "/bin/sh",
        "-c",
        "group=$(cut -d: -f3 /proc/self/cgroup); base=/sys/fs/cgroup$group; printf 'cgroup=%s\\n' \"$group\"; for key in memory.high memory.max memory.swap.max pids.max; do if test -f \"$base/$key\"; then printf '%s=' \"$key\"; cat \"$base/$key\"; fi; done",
    ]);
    configure_systemd_user_bus(&mut command);
    let output = command
        .output()
        .context("failed to start a protected scope probe")?;
    ensure_command_succeeded("systemd-run scope probe", &output)?;
    let values = parse_key_values(&String::from_utf8_lossy(&output.stdout));
    ensure!(
        values
            .get("cgroup")
            .is_some_and(|group| group.contains("/app-borg.slice/")),
        "the protected scope probe was not placed inside {SLICE_NAME}"
    );
    ensure_cgroup_bytes(
        &values,
        "memory.high",
        environment.policy.session_memory_high,
        "MemoryHigh",
    )?;
    ensure_cgroup_bytes(
        &values,
        "memory.max",
        environment.policy.session_memory_max,
        "MemoryMax",
    )?;
    if environment.swap_controller {
        ensure_cgroup_bytes(
            &values,
            "memory.swap.max",
            environment.policy.session_swap_max,
            "MemorySwapMax",
        )?;
    }
    if environment.pids_supported() {
        ensure!(
            values.get("pids.max") == Some(&environment.policy.session_tasks_max.to_string()),
            "the protected scope did not enforce the configured TasksMax"
        );
    }
    Ok(())
}

fn verify_slice_properties(environment: &LimitEnvironment, path: &Path) -> Result<()> {
    let properties = unit_properties()?;
    ensure!(
        properties_match(environment, path, &properties),
        "{SLICE_NAME} is loaded but its effective properties do not match the Borg policy"
    );
    Ok(())
}

fn properties_match(
    environment: &LimitEnvironment,
    path: &Path,
    properties: &BTreeMap<String, String>,
) -> bool {
    let policy = &environment.policy;
    let required = [
        ("LoadState", "loaded".to_string()),
        ("MemoryHigh", policy.aggregate_memory_high.to_string()),
        ("MemoryMax", policy.aggregate_memory_max.to_string()),
    ];
    if required
        .iter()
        .any(|(name, expected)| properties.get(*name) != Some(expected))
    {
        return false;
    }
    if !properties
        .get("FragmentPath")
        .is_some_and(|fragment| same_file_path(Path::new(fragment), path))
    {
        return false;
    }
    if environment.swap_controller
        && properties.get("MemorySwapMax") != Some(&policy.aggregate_swap_max.to_string())
    {
        return false;
    }
    if environment.cpu_supported() && properties.get("CPUWeight") != Some(&CPU_WEIGHT.to_string()) {
        return false;
    }
    if environment.io_supported() && properties.get("IOWeight") != Some(&IO_WEIGHT.to_string()) {
        return false;
    }
    if environment.pids_supported()
        && properties.get("TasksMax") != Some(&policy.aggregate_tasks_max.to_string())
    {
        return false;
    }
    true
}

fn unit_properties() -> Result<BTreeMap<String, String>> {
    let output = systemctl_output(&[
        "show",
        SLICE_NAME,
        "--property=LoadState,ActiveState,FragmentPath,ControlGroup,MemoryCurrent,MemoryHigh,MemoryMax,MemorySwapMax,CPUWeight,IOWeight,TasksCurrent,TasksMax",
    ])?;
    Ok(parse_key_values(&String::from_utf8_lossy(&output.stdout)))
}

fn active_session_names() -> Result<Vec<String>> {
    let output = systemctl_output(&[
        "list-units",
        "--type=scope",
        "--state=active",
        "--plain",
        "--no-legend",
        "borg-session-*.scope",
    ])?;
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.split_whitespace().next().map(str::to_string))
        .collect())
}

fn memory_events(control_group: &str) -> Result<BTreeMap<String, u64>> {
    ensure!(
        control_group.starts_with('/') && !control_group.contains(".."),
        "invalid cgroup path"
    );
    let path = Path::new("/sys/fs/cgroup")
        .join(control_group.trim_start_matches('/'))
        .join("memory.events");
    let source = fs::read_to_string(path)?;
    Ok(source
        .lines()
        .filter_map(|line| {
            let (name, value) = line.split_once(' ')?;
            Some((name.to_string(), value.parse().ok()?))
        })
        .collect())
}

fn policy_json(environment: &LimitEnvironment) -> serde_json::Value {
    json!({
        "policy_memory_bytes": environment.policy.total_memory,
        "aggregate": {
            "memory_high_bytes": environment.policy.aggregate_memory_high,
            "memory_max_bytes": environment.policy.aggregate_memory_max,
            "swap_max_bytes": environment.swap_controller.then_some(environment.policy.aggregate_swap_max),
            "tasks_max": environment.pids_supported().then_some(environment.policy.aggregate_tasks_max),
        },
        "per_session": {
            "memory_high_bytes": environment.policy.session_memory_high,
            "memory_max_bytes": environment.policy.session_memory_max,
            "swap_max_bytes": environment.swap_controller.then_some(environment.policy.session_swap_max),
            "tasks_max": environment.pids_supported().then_some(environment.policy.session_tasks_max),
        },
        "cpu_weight": environment.cpu_supported().then_some(CPU_WEIGHT),
        "io_weight": environment.io_supported().then_some(IO_WEIGHT),
        "controllers": environment.controllers,
    })
}

fn managed_unit_path() -> Option<PathBuf> {
    manager_unit_directory()
        .or_else(|| {
            std::env::var_os("XDG_CONFIG_HOME")
                .map(PathBuf::from)
                .or_else(|| {
                    std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config"))
                })
                .map(|root| root.join("systemd/user"))
        })
        .map(|root| root.join(SLICE_NAME))
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

fn read_managed_unit(path: &Path) -> Result<ManagedUnit> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(ManagedUnit::Absent);
        }
        Err(error) => {
            return Err(error).with_context(|| format!("failed to inspect {}", path.display()));
        }
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Ok(ManagedUnit::Foreign);
    }
    let source =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    if source.starts_with(UNIT_MARKER) {
        Ok(ManagedUnit::Borg(source))
    } else {
        Ok(ManagedUnit::Foreign)
    }
}

fn write_atomic(path: &Path, contents: &[u8]) -> Result<()> {
    let parent = path.parent().context("limits unit path has no parent")?;
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

fn restore_unit(path: &Path, previous: Option<&str>, restart_safe: bool) {
    if let Some(previous) = previous {
        let _ = write_atomic(path, previous.as_bytes());
    } else {
        let _ = fs::remove_file(path);
    }
    let _ = systemctl(&["daemon-reload"]);
    if restart_safe && active_session_names().is_ok_and(|sessions| sessions.is_empty()) {
        if previous.is_none() {
            let _ = systemctl(&["stop", SLICE_NAME]);
        } else {
            let _ = systemctl(&["restart", SLICE_NAME]);
        }
    }
}

fn same_file_path(left: &Path, right: &Path) -> bool {
    match (left.canonicalize(), right.canonicalize()) {
        (Ok(left), Ok(right)) => left == right,
        _ => left == right,
    }
}

fn ensure_cgroup_bytes(
    values: &BTreeMap<String, String>,
    key: &str,
    expected: u64,
    label: &str,
) -> Result<()> {
    let actual = values
        .get(key)
        .with_context(|| format!("the protected scope did not expose {label}"))?
        .parse::<u64>()
        .with_context(|| format!("the protected scope returned an invalid {label}"))?;
    ensure!(
        actual.abs_diff(expected) <= 4096,
        "the protected scope did not enforce the configured {label}: expected {expected}, got {actual}"
    );
    Ok(())
}

fn verify_unit_source(source: &str) -> Result<()> {
    let directory =
        tempfile::tempdir().context("failed to create a unit verification directory")?;
    let path = directory.path().join(SLICE_NAME);
    fs::write(&path, source)?;
    let mut command = ProcessCommand::new("systemd-analyze");
    command.args(["--user", "verify"]);
    command.arg(&path);
    configure_systemd_user_bus(&mut command);
    match command.output() {
        Ok(output) if output.status.success() => {}
        Ok(output) => tracing::warn!(
            status = %output.status,
            stderr = %String::from_utf8_lossy(&output.stderr).trim(),
            "systemd-analyze could not verify the generated limits unit; relying on the live scope probe"
        ),
        Err(error) => tracing::warn!(
            %error,
            "systemd-analyze is unavailable; relying on the live scope probe"
        ),
    }
    Ok(())
}

fn systemctl(args: &[&str]) -> Result<()> {
    systemctl_output(args).map(|_| ())
}

fn systemctl_output(args: &[&str]) -> Result<Output> {
    let mut command = ProcessCommand::new("systemctl");
    command.arg("--user").args(args);
    configure_systemd_user_bus(&mut command);
    let output = command.output().context("failed to run systemctl --user")?;
    ensure_command_succeeded(&format!("systemctl --user {}", args.join(" ")), &output)?;
    Ok(output)
}

fn ensure_command_succeeded(label: &str, output: &Output) -> Result<()> {
    ensure!(
        output.status.success(),
        "{label} failed with {}: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(())
}

fn configure_systemd_user_bus(command: &mut ProcessCommand) {
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
            command.env("XDG_RUNTIME_DIR", &runtime_dir);
        }
        if std::env::var_os("DBUS_SESSION_BUS_ADDRESS").is_none() {
            let bus = runtime_dir.join("bus");
            if bus.exists() {
                command.env(
                    "DBUS_SESSION_BUS_ADDRESS",
                    format!("unix:path={}", bus.display()),
                );
            }
        }
    }
}

fn inside_managed_slice() -> Result<bool> {
    let source =
        fs::read_to_string("/proc/self/cgroup").context("failed to read /proc/self/cgroup")?;
    Ok(source
        .lines()
        .filter_map(|line| line.split_once("::").map(|(_, path)| path))
        .any(|path| path.split('/').any(|component| component == SLICE_NAME)))
}

fn environment_bypasses_limits() -> bool {
    std::env::var_os("BORG_LIMITS").is_some_and(|value| value == "0")
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

fn format_bytes(bytes: u64) -> String {
    let gib = bytes as f64 / GIB as f64;
    if (gib - gib.round()).abs() < 0.05 {
        format!("{gib:.0} GiB")
    } else {
        format!("{gib:.1} GiB")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adaptive_policy_keeps_host_headroom_across_machine_sizes() {
        for gib in [4, 8, 16, 32, 64, 128] {
            let policy = LimitPolicy::for_total_memory(gib * GIB).unwrap();
            assert!(policy.aggregate_memory_high <= policy.aggregate_memory_max);
            assert!(policy.aggregate_memory_max < policy.total_memory);
            assert!(policy.session_memory_high <= policy.session_memory_max);
            assert!(policy.session_memory_max <= policy.aggregate_memory_max);
            assert!(policy.total_memory - policy.aggregate_memory_max >= policy.total_memory / 8);
        }
        let policy = LimitPolicy::for_total_memory(64 * GIB).unwrap();
        assert_eq!(policy.aggregate_memory_high, 64 * GIB - 64 * GIB / 5);
        assert_eq!(policy.aggregate_memory_max, 56 * GIB);
        assert_eq!(policy.session_memory_max, 51 * GIB + GIB / 5);
    }

    #[test]
    fn generated_unit_uses_only_delegated_controllers() {
        let environment = LimitEnvironment {
            policy: LimitPolicy::for_total_memory(32 * GIB).unwrap(),
            controllers: ["cpu", "memory", "pids"]
                .into_iter()
                .map(str::to_string)
                .collect(),
            swap_controller: true,
        };
        let source = slice_unit(&environment);
        assert!(source.starts_with(UNIT_MARKER));
        assert!(source.contains("MemoryMax="));
        assert!(source.contains("CPUWeight=80"));
        assert!(source.contains("TasksMax=32768"));
        assert!(!source.contains("IOWeight="));
    }

    #[test]
    fn per_session_scope_is_generous_without_cpu_quota_or_group_kill() {
        let environment = LimitEnvironment {
            policy: LimitPolicy::for_total_memory(64 * GIB).unwrap(),
            controllers: ["cpu", "io", "memory", "pids"]
                .into_iter()
                .map(str::to_string)
                .collect(),
            swap_controller: true,
        };
        let args = scope_args(&environment, "borg-session-test")
            .into_iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        assert!(args.contains(&"--slice=app-borg.slice".to_string()));
        assert!(args.contains(&"--property=TasksMax=8192".to_string()));
        assert!(args.contains(&"--property=IOWeight=50".to_string()));
        assert!(!args.iter().any(|arg| arg.contains("CPUQuota")));
        assert!(!args.iter().any(|arg| arg.contains("OOMPolicy")));
    }

    #[test]
    fn managed_marker_does_not_claim_foreign_units() {
        assert!(!"[Slice]\nMemoryMax=1G\n".starts_with(UNIT_MARKER));
        assert!(
            slice_unit(&LimitEnvironment {
                policy: LimitPolicy::for_total_memory(16 * GIB).unwrap(),
                controllers: ["memory"].into_iter().map(str::to_string).collect(),
                swap_controller: false,
            })
            .starts_with(UNIT_MARKER)
        );
    }

    #[test]
    fn physical_memory_is_quantized_to_avoid_benign_boot_drift() {
        assert_eq!(quantize_memory(63 * GIB + GIB / 2 - 1), 63 * GIB);
        assert_eq!(quantize_memory(63 * GIB + GIB / 2), 64 * GIB);
        assert_eq!(quantize_memory(64 * GIB - 32 * 1024 * 1024), 64 * GIB);
    }

    #[test]
    fn foreign_or_symlinked_units_are_never_claimed_as_borg_managed() {
        let directory = tempfile::tempdir().unwrap();
        let foreign = directory.path().join(SLICE_NAME);
        fs::write(&foreign, "[Slice]\nMemoryMax=1G\n").unwrap();
        assert!(matches!(
            read_managed_unit(&foreign).unwrap(),
            ManagedUnit::Foreign
        ));

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            let target = directory.path().join("target.slice");
            let linked = directory.path().join("linked.slice");
            fs::write(&target, format!("{UNIT_MARKER}\n")).unwrap();
            symlink(&target, &linked).unwrap();
            assert!(matches!(
                read_managed_unit(&linked).unwrap(),
                ManagedUnit::Foreign
            ));
        }
    }
}
