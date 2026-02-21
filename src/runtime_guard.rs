use color_eyre::eyre::{eyre, Context, Result};
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::path::PathBuf;
use std::process;
use std::time::Duration;
use sysinfo::{Pid, System};

const LOCK_FILE_NAME: &str = "rbx-studio-mcp.lock";
const TAKEOVER_WAIT_MS: u64 = 100;
const TAKEOVER_MAX_POLLS: usize = 20;

pub struct InstanceLock {
    path: PathBuf,
    _file: File,
}

impl InstanceLock {
    pub fn acquire() -> Result<Self> {
        let path = std::env::temp_dir().join(LOCK_FILE_NAME);

        if let Some(existing_pid) = read_lock_pid(&path)? {
            if existing_pid != process::id() && is_process_alive(existing_pid) {
                tracing::warn!(
                    "Detected existing MCP instance pid={} from lock; taking over",
                    existing_pid
                );
                terminate_process(existing_pid);
                wait_for_process_exit(existing_pid);
            }
            let _ = fs::remove_file(&path);
        }

        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("Failed to create lock file at {}", path.display()))?;

        writeln!(file, "pid={}", process::id())?;
        file.flush()?;
        file.sync_all().ok();

        Ok(Self { path, _file: file })
    }
}

impl Drop for InstanceLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub fn kill_other_instances() {
    let current_pid = process::id();
    let current_exe_name = std::env::current_exe()
        .ok()
        .and_then(|p| p.file_name().map(|n| n.to_string_lossy().to_ascii_lowercase()))
        .unwrap_or_else(|| "rbx-studio-mcp".to_string());

    let system = System::new_all();
    for (pid, proc_) in system.processes() {
        let pid_u32 = pid.as_u32();
        if pid_u32 == current_pid {
            continue;
        }

        let proc_name = proc_.name().to_string_lossy().to_ascii_lowercase();
        if proc_name == current_exe_name || proc_name == "rbx-studio-mcp.exe" || proc_name == "rbx-studio-mcp" {
            tracing::warn!("Terminating competing MCP instance pid={pid_u32}");
            terminate_process(pid_u32);
        }
    }
}

pub fn spawn_parent_watchdog() {
    let parent_pid = current_parent_pid();
    if let Some(parent_pid) = parent_pid {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(2));
            loop {
                interval.tick().await;
                if !is_process_alive(parent_pid) {
                    tracing::warn!(
                        "Parent process pid={} is no longer running; exiting MCP server",
                        parent_pid
                    );
                    process::exit(0);
                }
            }
        });
    } else {
        tracing::warn!("Parent process could not be determined; parent watchdog disabled");
    }
}

fn read_lock_pid(path: &PathBuf) -> Result<Option<u32>> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };

    let mut content = String::new();
    file.read_to_string(&mut content)?;
    for line in content.lines() {
        if let Some(value) = line.strip_prefix("pid=") {
            let pid = value
                .trim()
                .parse::<u32>()
                .map_err(|e| eyre!("Invalid lock file pid '{}': {e}", value.trim()))?;
            return Ok(Some(pid));
        }
    }
    Ok(None)
}

fn terminate_process(pid: u32) {
    let mut system = System::new_all();
    system.refresh_processes(sysinfo::ProcessesToUpdate::All, true);
    if let Some(process) = system.process(Pid::from_u32(pid)) {
        let _ = process.kill();
    }
}

fn wait_for_process_exit(pid: u32) {
    for _ in 0..TAKEOVER_MAX_POLLS {
        if !is_process_alive(pid) {
            return;
        }
        std::thread::sleep(Duration::from_millis(TAKEOVER_WAIT_MS));
    }
}

fn current_parent_pid() -> Option<u32> {
    let system = System::new_all();
    let current = system.process(Pid::from_u32(process::id()))?;
    current.parent().map(|pid| pid.as_u32())
}

fn is_process_alive(pid: u32) -> bool {
    let system = System::new_all();
    system.process(Pid::from_u32(pid)).is_some()
}
