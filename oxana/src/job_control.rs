use std::time::Duration;

/// Worker error that ends a job immediately, regardless of `max_retries`.
///
/// The job is moved straight to the dead queue on this run, with the error
/// recorded on the job so the reason shows in the dashboard. Use it for
/// failures that retrying cannot fix (invalid input, a permanent refusal).
///
/// Return it directly (`Err(Terminal("reason".into()).into())` with
/// [`BoxError`](crate::BoxError)) or wrap it as the
/// [`source`](std::error::Error::source) of your own error type: Oxana looks
/// for it anywhere in the error's source chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Terminal(pub String);

impl std::fmt::Display for Terminal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Terminal {}

/// Worker error that postpones a job without spending a retry.
///
/// The same job (same id, same retry count, current state) is scheduled to
/// run again after the given duration. Like [`Terminal`], it may be returned
/// directly or found anywhere in the error's source chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Snooze(pub Duration);

impl std::fmt::Display for Snooze {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "job snoozed for {:?}", self.0)
    }
}

impl std::error::Error for Snooze {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JobControl {
    Terminal,
    Snooze(Duration),
}

/// Finds the first [`Terminal`] or [`Snooze`] in the error's source chain.
pub(crate) fn job_control(error: &(dyn std::error::Error + 'static)) -> Option<JobControl> {
    let mut current = Some(error);
    while let Some(error) = current {
        if error.is::<Terminal>() {
            return Some(JobControl::Terminal);
        }
        if let Some(Snooze(delay)) = error.downcast_ref::<Snooze>() {
            return Some(JobControl::Snooze(*delay));
        }
        current = error.source();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct Wrapper(Box<dyn std::error::Error + Send + Sync>);

    impl std::fmt::Display for Wrapper {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("wrapper")
        }
    }

    impl std::error::Error for Wrapper {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(self.0.as_ref())
        }
    }

    #[test]
    fn plain_errors_have_no_control() {
        let error = std::io::Error::other("boom");
        assert_eq!(job_control(&error), None);
    }

    #[test]
    fn terminal_is_found_directly_and_in_source_chain() {
        assert_eq!(
            job_control(&Terminal("bad".into())),
            Some(JobControl::Terminal)
        );
        let wrapped = Wrapper(Box::new(Terminal("bad".into())));
        assert_eq!(job_control(&wrapped), Some(JobControl::Terminal));
    }

    #[test]
    fn snooze_carries_its_delay() {
        let wrapped = Wrapper(Box::new(Snooze(Duration::from_secs(30))));
        assert_eq!(
            job_control(&wrapped),
            Some(JobControl::Snooze(Duration::from_secs(30)))
        );
    }

    #[test]
    fn terminal_display_is_the_reason() {
        assert_eq!(Terminal("invalid recipient".into()).to_string(), "invalid recipient");
    }
}
