use std::fmt::Display;

use serde::{Deserialize, Serialize};


#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct Config {
    pub ram_mb: Option<u64>,
    pub cpu_cores: Option<usize>,
    pub profile: Option<MachineProfile>,
    pub durability: bool
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "snake_case")]
pub enum MachineProfile {
    Nano,
    Micro,
    Small,
    Medium,
    Large,
    XLarge,
}

impl Display for MachineProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MachineProfile::Nano => write!(f, "nano"),
            MachineProfile::Micro => write!(f, "micro"),
            MachineProfile::Small => write!(f, "small"),
            MachineProfile::Medium => write!(f, "medium"),
            MachineProfile::Large => write!(f, "large"),
            MachineProfile::XLarge => write!(f, "x_large"),
        }
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

pub fn resolve_profile(ram_mb: u64) -> MachineProfile {
    match ram_mb {
        0..=512 => MachineProfile::Nano,
        513..=1024 => MachineProfile::Micro,
        1025..=2048 => MachineProfile::Small,
        2049..=4096 => MachineProfile::Medium,
        4097..=8192 => MachineProfile::Large,
        _ => MachineProfile::XLarge,
    }
}