use std::net::IpAddr;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlindowConfig {
    pub enabled: bool,
    pub requests_per_window: u64,
    pub window_secs: u64,
    pub key_prefix: String,
    pub script_path: std::path::PathBuf,
    #[serde(default)]
    pub trusted_proxies: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
pub struct TrustedNet {
    addr: IpAddr,
    prefix_len: u8,
}

impl TrustedNet {
    fn contains(&self, ip: IpAddr) -> bool {
        match (self.addr, ip) {
            (IpAddr::V4(net), IpAddr::V4(addr)) => {
                let shift = 32 - self.prefix_len.min(32);
                u32::from(net) >> shift == u32::from(addr) >> shift
            }
            (IpAddr::V6(net), IpAddr::V6(addr)) => {
                let shift = 128 - self.prefix_len.min(128);
                u128::from(net) >> shift == u128::from(addr) >> shift
            }
            _ => false,
        }
    }
}

pub fn parse_trusted_proxies(entries: &[String]) -> anyhow::Result<Vec<TrustedNet>> {
    entries
        .iter()
        .map(|e| {
            if let Some((ip, prefix)) = e.split_once('/') {
                let addr = IpAddr::from_str(ip.trim())
                    .map_err(|err| anyhow::anyhow!("bad trusted_proxy {e:?}: {err}"))?;
                let max = if addr.is_ipv4() { 32 } else { 128 };
                let prefix_len: u8 = prefix
                    .trim()
                    .parse()
                    .map_err(|_| anyhow::anyhow!("bad trusted_proxy {e:?}: invalid prefix"))?;
                if prefix_len > max {
                    anyhow::bail!("bad trusted_proxy {e:?}: prefix > {max}");
                }
                Ok(TrustedNet { addr, prefix_len })
            } else {
                let addr = IpAddr::from_str(e.trim())
                    .map_err(|err| anyhow::anyhow!("bad trusted_proxy {e:?}: {err}"))?;
                let prefix_len = if addr.is_ipv4() { 32 } else { 128 };
                Ok(TrustedNet { addr, prefix_len })
            }
        })
        .collect()
}

pub fn proxy_is_trusted(nets: &[TrustedNet], peer: IpAddr) -> bool {
    nets.iter().any(|n| n.contains(peer))
}
