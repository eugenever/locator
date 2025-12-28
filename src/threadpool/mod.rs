use std::time::Duration;

use rusty_pool::{Builder, ThreadPool};

pub fn create_threadpool(
    core_size: usize,
    max_size: usize,
    name: String,
    keep_alive: u64,
) -> Result<ThreadPool, anyhow::Error> {
    let pool = Builder::new()
        .core_size(core_size)
        .max_size(max_size)
        .keep_alive(Duration::from_secs(keep_alive))
        .name(name)
        .build();
    pool.start_core_threads();
    Ok(pool)
}
