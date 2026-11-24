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
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct Terminal(pub String);

/// Worker error that postpones a job without spending a retry.
///
/// The same job (same id, same retry count, current state) is scheduled to
/// run again after the given duration. Like [`Terminal`], it may be returned
/// directly or found anywhere in the error's source chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("job snoozed for {0:?}")]
pub struct Snooze(pub Duration);

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

    #[derive(Debug, thiserror::Error)]
    #[error("wrapper")]
    struct Wrapper(#[source] Box<dyn std::error::Error + Send + Sync>);

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
        assert_eq!(
            Terminal("invalid recipient".into()).to_string(),
            "invalid recipient"
        );
    }
}
