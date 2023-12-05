use std::time::Duration;

struct WalConfig{
    flush_interval:Duration,
    max_log_size:u64,
}


impl WalConfig {
    fn new(flush_interval: Duration, max_log_size: u64) -> Self {
        WalConfig { flush_interval, max_log_size }
    }
}