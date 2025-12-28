#![allow(dead_code)]

use anyhow::{Context, Result};
use log::{debug, warn};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, Semaphore};
use tokio::time::sleep;

use crate::CONFIG;

#[derive(Debug, Clone)]
pub struct RateLimitersApp {
    pub yandex_lbs: Arc<Semaphore>,
    pub gh_matching: Arc<Semaphore>,
}

pub fn crate_rate_limiters_app() -> RateLimitersApp {
    RateLimitersApp {
        yandex_lbs: Arc::new(Semaphore::new(CONFIG.yandex_lbs.rate_limit)),
        gh_matching: Arc::new(Semaphore::new(CONFIG.graphhopper.rate_limit_matching)),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    /// Maximum requests per second (RPS)
    #[serde(rename = "maxRequestsPerSecond")]
    pub max_requests_per_second: u32,
    /// Initial retry delay in seconds
    #[serde(rename = "initialRetryDelaySeconds")]
    pub initial_retry_delay_seconds: u64,
    /// Maximum retry delay in seconds
    #[serde(rename = "maxRetryDelaySeconds")]
    pub max_retry_delay_seconds: u64,
    /// Maximum number of retry attempts
    #[serde(rename = "maxRetryAttempts")]
    pub max_retry_attempts: u32,
    /// Backoff multiplier for exponential backoff
    #[serde(rename = "backoffMultiplier")]
    pub backoff_multiplier: f64,
    /// Enable jitter to avoid thundering herd
    #[serde(rename = "enableJitter")]
    pub enable_jitter: bool,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            max_requests_per_second: 50,
            initial_retry_delay_seconds: 1,
            max_retry_delay_seconds: 60, // 1 minute max
            max_retry_attempts: 5,
            backoff_multiplier: 2.0,
            enable_jitter: true,
        }
    }
}

#[derive(Debug)]
struct RateLimitState {
    requests: Vec<Instant>,
    last_rate_limit: Option<Instant>,
    consecutive_rate_limits: u32,
}

impl RateLimitState {
    fn new() -> Self {
        Self {
            requests: Vec::new(),
            last_rate_limit: None,
            consecutive_rate_limits: 0,
        }
    }

    fn cleanup_old_requests(&mut self, window: Duration) {
        let cutoff = Instant::now() - window;
        self.requests.retain(|&request_time| request_time > cutoff);
    }

    fn can_make_request(&self, max_requests: u32) -> bool {
        self.requests.len() < max_requests as usize
    }

    fn record_request(&mut self) {
        self.requests.push(Instant::now());
    }

    fn record_rate_limit(&mut self) {
        self.last_rate_limit = Some(Instant::now());
        self.consecutive_rate_limits += 1;
    }

    fn reset_rate_limit_count(&mut self) {
        self.consecutive_rate_limits = 0;
    }
}

#[derive(Debug, Clone)]
pub struct RateLimiter {
    config: RateLimitConfig,
    state: Arc<Mutex<RateLimitState>>,
}

impl RateLimiter {
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            config,
            state: Arc::new(Mutex::new(RateLimitState::new())),
        }
    }

    /// Wait if necessary to respect rate limits before making a request
    pub async fn acquire_permit(&self) -> Result<()> {
        let mut state = self.state.lock().await;

        // Clean up old requests outside the current window
        let window = Duration::from_secs(1);
        state.cleanup_old_requests(window);

        // Check if we can make a request
        if !state.can_make_request(self.config.max_requests_per_second) {
            let wait_time = self.calculate_wait_time(&state);
            drop(state); // Release lock while waiting

            debug!(
                "Rate limit reached, waiting {:?} before next request",
                wait_time
            );
            sleep(wait_time).await;

            // Re-acquire lock and clean up again
            let mut state = self.state.lock().await;
            state.cleanup_old_requests(window);
            state.record_request();
            debug!(
                "Rate limiter: {} requests in current window",
                state.requests.len()
            );
        } else {
            // Record the request
            state.record_request();
            debug!(
                "Rate limiter: {} requests in current window",
                state.requests.len()
            );
        }

        Ok(())
    }

    /// Handle a rate limit response from the API
    pub async fn handle_rate_limit_response(
        &self,
        retry_after: Option<Duration>,
    ) -> Result<Duration> {
        let mut state = self.state.lock().await;
        state.record_rate_limit();

        let delay = if let Some(retry_after) = retry_after {
            // Use server-provided retry-after if available
            retry_after
        } else {
            // Calculate exponential backoff delay
            self.calculate_backoff_delay(state.consecutive_rate_limits)
        };

        warn!(
            "Rate limited by API (attempt {}), backing off for {:?}",
            state.consecutive_rate_limits, delay
        );

        Ok(delay)
    }

    /// Reset rate limit state after a successful request
    pub async fn reset_rate_limit_state(&self) {
        let mut state = self.state.lock().await;
        state.reset_rate_limit_count();
    }

    /// Check if we should retry after a rate limit
    pub async fn should_retry(&self) -> bool {
        let state = self.state.lock().await;
        state.consecutive_rate_limits <= self.config.max_retry_attempts
    }

    fn calculate_wait_time(&self, state: &RateLimitState) -> Duration {
        if let Some(oldest_request) = state.requests.first() {
            let elapsed = oldest_request.elapsed();
            let window = Duration::from_secs(1);

            if elapsed < window {
                window - elapsed
            } else {
                Duration::from_millis(1) // Small delay for cleanup
            }
        } else {
            Duration::from_millis(1)
        }
    }

    fn calculate_backoff_delay(&self, attempt: u32) -> Duration {
        let base_delay = Duration::from_secs(self.config.initial_retry_delay_seconds);
        let multiplier = self
            .config
            .backoff_multiplier
            .powi(attempt.saturating_sub(1) as i32);
        let delay_secs = (base_delay.as_secs_f64() * multiplier) as u64;

        let delay = Duration::from_secs(delay_secs.min(self.config.max_retry_delay_seconds));

        if self.config.enable_jitter {
            self.add_jitter(delay)
        } else {
            delay
        }
    }

    fn add_jitter(&self, delay: Duration) -> Duration {
        // Simple jitter using system time microseconds
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        let jitter_factor = 0.8 + (now.subsec_micros() % 400) as f64 / 1000.0; // 0.8 to 1.2
        let jittered_secs = delay.as_secs_f64() * jitter_factor;
        Duration::from_secs_f64(jittered_secs)
    }

    /// Get current rate limit statistics
    pub async fn get_stats(&self) -> RateLimitStats {
        let state = self.state.lock().await;
        let window = Duration::from_secs(1);
        let now = Instant::now();

        // Count requests in current window
        let current_requests = state
            .requests
            .iter()
            .filter(|&&req_time| now.duration_since(req_time) < window)
            .count();

        RateLimitStats {
            current_requests: current_requests as u32,
            max_requests_per_second: self.config.max_requests_per_second,
            consecutive_rate_limits: state.consecutive_rate_limits,
            last_rate_limit: state.last_rate_limit,
            requests_remaining: self
                .config
                .max_requests_per_second
                .saturating_sub(current_requests as u32),
        }
    }
}

#[derive(Debug)]
pub struct RateLimitStats {
    pub current_requests: u32,
    pub max_requests_per_second: u32,
    pub consecutive_rate_limits: u32,
    pub last_rate_limit: Option<Instant>,
    pub requests_remaining: u32,
}

/// Extract retry-after duration from HTTP response headers
pub fn parse_retry_after_header(retry_after: Option<&str>) -> Option<Duration> {
    retry_after.and_then(|value| {
        // Try parsing as seconds (most common)
        if let Ok(seconds) = value.parse::<u64>() {
            Some(Duration::from_secs(seconds))
        } else {
            // Try parsing as HTTP date (less common)
            // For now, just return None for date format
            // Could implement HTTP date parsing if needed
            None
        }
    })
}

/// Wrapper for HTTP requests with automatic rate limiting and retry
pub struct RateLimitedClient {
    client: reqwest::Client,
    rate_limiter: RateLimiter,
}

impl RateLimitedClient {
    pub fn new(client: reqwest::Client, config: RateLimitConfig) -> Self {
        Self {
            client,
            rate_limiter: RateLimiter::new(config),
        }
    }

    pub async fn execute_with_retry<F, T>(&self, request_fn: F) -> Result<T>
    where
        F: Fn() -> reqwest::RequestBuilder,
        T: for<'de> serde::Deserialize<'de>,
    {
        loop {
            // Wait for rate limit permit
            self.rate_limiter.acquire_permit().await?;

            // Execute the request
            let response = request_fn()
                .send()
                .await
                .context("Failed to send HTTP request")?;

            match response.status() {
                status if status.is_success() => {
                    // Reset rate limit state on success
                    self.rate_limiter.reset_rate_limit_state().await;

                    let result = response
                        .json::<T>()
                        .await
                        .context("Failed to parse response JSON")?;
                    return Ok(result);
                }
                status if status == 429 => {
                    // Rate limited - check if we should retry
                    if !self.rate_limiter.should_retry().await {
                        return Err(anyhow::anyhow!(
                            "Maximum retry attempts exceeded for rate limiting"
                        ));
                    }

                    // Parse retry-after header
                    let retry_after = response
                        .headers()
                        .get("retry-after")
                        .and_then(|h| h.to_str().ok())
                        .and_then(|s| parse_retry_after_header(Some(s)));

                    // Handle rate limit and get delay
                    let delay = self
                        .rate_limiter
                        .handle_rate_limit_response(retry_after)
                        .await?;

                    // Wait before retrying
                    sleep(delay).await;
                    continue;
                }
                status => {
                    let error_text = response
                        .text()
                        .await
                        .unwrap_or_else(|_| "Unknown error".to_string());
                    return Err(anyhow::anyhow!(
                        "HTTP request failed with status {}: {}",
                        status,
                        error_text
                    ));
                }
            }
        }
    }

    pub async fn get_rate_limit_stats(&self) -> RateLimitStats {
        self.rate_limiter.get_stats().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_rate_limiter_basic() {
        let config = RateLimitConfig {
            max_requests_per_second: 2,
            ..Default::default()
        };
        let limiter = RateLimiter::new(config);

        // First two requests should be immediate
        limiter.acquire_permit().await.unwrap();
        limiter.acquire_permit().await.unwrap();

        // Third request should be delayed
        let start = Instant::now();
        limiter.acquire_permit().await.unwrap();
        let elapsed = start.elapsed();

        // Should have waited some time
        assert!(elapsed > Duration::from_millis(50));
    }

    #[tokio::test]
    async fn test_backoff_calculation() {
        let config = RateLimitConfig {
            initial_retry_delay_seconds: 1,
            backoff_multiplier: 2.0,
            enable_jitter: false,
            ..Default::default()
        };
        let limiter = RateLimiter::new(config);

        let delay1 = limiter.calculate_backoff_delay(1);
        let delay2 = limiter.calculate_backoff_delay(2);
        let delay3 = limiter.calculate_backoff_delay(3);

        assert_eq!(delay1, Duration::from_secs(1));
        assert_eq!(delay2, Duration::from_secs(2));
        assert_eq!(delay3, Duration::from_secs(4));
    }

    #[test]
    fn test_retry_after_parsing() {
        assert_eq!(
            parse_retry_after_header(Some("60")),
            Some(Duration::from_secs(60))
        );
        assert_eq!(
            parse_retry_after_header(Some("0")),
            Some(Duration::from_secs(0))
        );
        assert_eq!(parse_retry_after_header(Some("invalid")), None);
        assert_eq!(parse_retry_after_header(None), None);
    }
}
