use std::time::Duration;

pub struct WalConfig{
    pub flush_interval:Duration,
    pub max_log_size:u64,
}


impl WalConfig {
    pub fn new(flush_interval: Duration, max_log_size: u64) -> Self {
        WalConfig { flush_interval, max_log_size }
    }
}