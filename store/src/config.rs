use std::fmt::Display;

use serde::{Deserialize, Serialize};

/// How to size the database backend.
///
/// - `Profile` — use a predefined instance type: implies fixed vCPU and RAM.
/// - `Custom`  — supply exact RAM (MB) and vCPU count manually.
/// - Absent (`None` in `Config`) — auto-detect from the running host.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum MachineSpec {
    Profile(MachineProfile),
    Custom { ram_mb: u64, cpu_cores: usize },
}

/// Predefined instance profiles with fixed vCPU and RAM.
/// They only exist to provide convenient default values — the actual
/// DB tuning is derived from the resolved `ram_mb` and `cpu_cores`.
///
/// | Profile  | vCPU | RAM    |
/// |----------|------|--------|
/// | Nano     | 2    | 512 MB |
/// | Micro    | 2    | 1 GB   |
/// | Small    | 2    | 2 GB   |
/// | Medium   | 2    | 4 GB   |
/// | Large    | 2    | 8 GB   |
/// | XLarge   | 4    | 16 GB  |
/// | XXLarge  | 8    | 32 GB  |
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MachineProfile {
    Nano,
    Micro,
    Small,
    Medium,
    Large,
    XLarge,
    #[serde(rename = "2xlarge")]
    XXLarge,
}

impl MachineProfile {
    /// Canonical RAM for this profile in megabytes.
    pub const fn ram_mb(self) -> u64 {
        match self {
            Self::Nano    =>   512,
            Self::Micro   =>  1024,
            Self::Small   =>  2048,
            Self::Medium  =>  4096,
            Self::Large   =>  8192,
            Self::XLarge  => 16384,
            Self::XXLarge => 32768,
        }
    }

    /// vCPU count for this profile.
    pub const fn cpu_cores(self) -> usize {
        match self {
            Self::Nano    => 2,
            Self::Micro   => 2,
            Self::Small   => 2,
            Self::Medium  => 2,
            Self::Large   => 2,
            Self::XLarge  => 4,
            Self::XXLarge => 8,
        }
    }
}

impl Display for MachineProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Nano    => write!(f, "nano"),
            Self::Micro   => write!(f, "micro"),
            Self::Small   => write!(f, "small"),
            Self::Medium  => write!(f, "medium"),
            Self::Large   => write!(f, "large"),
            Self::XLarge  => write!(f, "xlarge"),
            Self::XXLarge => write!(f, "2xlarge"),
        }
    }
}

/// Resolved machine parameters ready to be consumed by database backends.
/// Database tuning is computed directly from these two values.
pub struct ResolvedSpec {
    pub ram_mb: u64,
    pub cpu_cores: usize,
}

/// Resolve the final DB sizing parameters from a [`MachineSpec`]:
///
/// - `Profile(p)` → use the profile's canonical RAM and vCPU.
/// - `Custom { ram_mb, cpu_cores }` → use the supplied values directly.
/// - `None` → auto-detect total RAM and available CPU cores from the host.
pub fn resolve_spec(spec: Option<MachineSpec>) -> ResolvedSpec {
    match spec {
        Some(MachineSpec::Profile(p)) => ResolvedSpec {
            ram_mb: p.ram_mb(),
            cpu_cores: p.cpu_cores(),
        },
        Some(MachineSpec::Custom { ram_mb, cpu_cores }) => ResolvedSpec {
            ram_mb,
            cpu_cores,
        },
        None => ResolvedSpec {
            ram_mb: detect_total_memory_mb().unwrap_or(4096),
            cpu_cores: detect_cpu_cores(),
        },
    }
}

pub fn detect_total_memory_mb() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        use std::fs;
        let meminfo = fs::read_to_string("/proc/meminfo").ok()?;
        for line in meminfo.lines() {
            if let Some(rest) = line.strip_prefix("MemTotal:") {
                let kb_str = rest.split_whitespace().next()?;
                let kb: u64 = kb_str.parse().ok()?;
                return Some(kb / 1024);
            }
        }
        None
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

pub fn detect_cpu_cores() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}
