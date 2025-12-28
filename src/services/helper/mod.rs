pub mod custom_deserialize;
pub mod macaddr;
pub mod pool_task;
pub mod rate_limiter;
pub mod validation;

pub fn round(x: f64, decimals: u32) -> f64 {
    let y = 10i32.pow(decimals) as f64;
    (x * y).round() / y
}

pub fn determine_thread_num() -> usize {
    let cpu_cores = num_cpus::get();
    std::cmp::max(1, cpu_cores - 1)
}
