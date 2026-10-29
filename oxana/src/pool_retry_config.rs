use std::time::Duration;

use crate::OxanaError;

/// Connection-acquisition retries before claimed-job transitions.
///
/// Configure with [`crate::StorageBuilder::claimed_connection_retries`].
/// Heartbeats, ordinary pool access, and commands already sent are not retried
/// by this policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PoolRetryConfig {
    /// Maximum time for the entire checkout, including the first attempt and
    /// backoff. `None` uses two seconds or twice the longest configured pool
    /// timeout, whichever is greater. Unset pool timeouts do not extend it.
    pub retry_window: Option<Duration>,
    /// Initial backoff ceiling, at least one millisecond. Each delay is randomly
    /// selected between half and all of its ceiling.
    pub initial_backoff: Duration,
    /// Maximum backoff ceiling. The ceiling doubles after each failure.
    pub max_backoff: Duration,
}

impl Default for PoolRetryConfig {
    fn default() -> Self {
        Self {
            retry_window: None,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(1),
        }
    }
}

impl PoolRetryConfig {
    pub(crate) fn validate(self) -> Result<(), OxanaError> {
        if let Some(window) = self.retry_window
            && (window.is_zero() || std::time::Instant::now().checked_add(window).is_none())
        {
            return Err(OxanaError::ConfigError(
                "pool retry window must be positive and fit a monotonic deadline".into(),
            ));
        }
        if self.initial_backoff < Duration::from_millis(1)
            || self.max_backoff < self.initial_backoff
            || std::time::Instant::now()
                .checked_add(self.max_backoff)
                .is_none()
        {
            return Err(OxanaError::ConfigError(
                "pool retry backoff must satisfy 1ms <= initial_backoff <= max_backoff and fit a monotonic deadline".into(),
            ));
        }
        Ok(())
    }
}
