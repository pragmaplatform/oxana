use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::OxanaError;
use crate::worker::Job;

pub type JobId = String;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JobEnvelope {
    pub id: JobId,
    pub job: JobData,
    pub queue: String,
    pub meta: JobMeta,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JobData {
    pub name: String,
    pub args: serde_json::Value,
}

fn default_resurrect() -> bool {
    true
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct JobMeta {
    pub id: JobId,
    pub retries: u32,
    pub unique: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_conflict: Option<JobConflictStrategy>,
    pub created_at: i64,
    #[serde(default)]
    pub scheduled_at: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub state: Option<serde_json::Value>,
    #[serde(default = "default_resurrect")]
    pub resurrect: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub throttle_cost: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum JobConflictStrategy {
    #[default]
    Skip,
    Replace,
}

impl JobEnvelope {
    pub(crate) fn new<T: Job + 'static>(queue: String, job: T) -> Result<Self, OxanaError> {
        let job_name = T::name().to_string();
        let unique_id = job.unique_id();
        let unique = unique_id.is_some();
        let resurrect = T::should_resurrect();
        let id = match unique_id {
            Some(id) => format!("{}/{}", job_name, id),
            None => Uuid::new_v4().to_string(),
        };
        Ok(Self {
            id: id.clone(),
            queue,
            job: JobData {
                name: job_name,
                args: serde_json::to_value(&job)?,
            },
            meta: JobMeta {
                id,
                retries: 0,
                unique,
                on_conflict: if unique {
                    Some(job.on_conflict())
                } else {
                    None
                },
                created_at: chrono::Utc::now().timestamp_micros(),
                scheduled_at: chrono::Utc::now().timestamp_micros(),
                started_at: None,
                state: None,
                resurrect,
                error: None,
                throttle_cost: job.throttle_cost(),
            },
        })
    }

    pub(crate) fn new_scheduled<T: Job + 'static>(
        queue: String,
        job: T,
        scheduled_at: DateTime<Utc>,
    ) -> Result<Self, OxanaError> {
        Ok(Self::new(queue, job)?.with_scheduled_at(scheduled_at))
    }

    pub(crate) fn new_cron<T: Job + 'static>(
        queue: String,
        occurrence_id: String,
        job: &T,
        scheduled_at: i64,
        resurrect: bool,
    ) -> Result<(Self, bool), OxanaError> {
        let now = chrono::Utc::now().timestamp_micros();
        let name = T::name().to_string();
        let on_conflict = job.on_conflict();
        if matches!(on_conflict, JobConflictStrategy::Replace) {
            return Err(OxanaError::GenericError(format!(
                "{name}: cron jobs do not support `on_conflict = Replace`; use `Skip`"
            )));
        }
        let (id, on_conflict, declared_unique) = match job.unique_id() {
            Some(unique_id) => (format!("{name}/{unique_id}"), on_conflict, true),
            None => (occurrence_id, JobConflictStrategy::Skip, false),
        };
        Ok((
            Self {
                id: id.clone(),
                queue,
                job: JobData {
                    name,
                    args: serde_json::to_value(job)?,
                },
                meta: JobMeta {
                    id,
                    retries: 0,
                    unique: true,
                    on_conflict: Some(on_conflict),
                    created_at: now,
                    scheduled_at: scheduled_at.max(now),
                    started_at: None,
                    state: None,
                    resurrect,
                    error: None,
                    throttle_cost: None,
                },
            },
            declared_unique,
        ))
    }

    pub(crate) fn with_scheduled_at(mut self, scheduled_at: DateTime<Utc>) -> Self {
        let now = chrono::Utc::now().timestamp_micros();
        self.meta.scheduled_at = scheduled_at.timestamp_micros().max(now);
        self
    }

    pub(crate) fn with_error(mut self, error: String) -> Self {
        self.meta.error = Some(error);
        self
    }

    pub(crate) fn with_retries_incremented(self, error: String) -> Self {
        Self {
            id: self.id.clone(),
            queue: self.queue,
            job: self.job,
            meta: JobMeta {
                id: self.id,
                retries: self.meta.retries + 1,
                unique: self.meta.unique,
                on_conflict: self.meta.on_conflict,
                created_at: self.meta.created_at,
                scheduled_at: self.meta.scheduled_at,
                started_at: None,
                state: self.meta.state,
                resurrect: self.meta.resurrect,
                error: Some(error),
                throttle_cost: self.meta.throttle_cost,
            },
        }
    }
}

impl JobMeta {
    pub fn created_at_secs(&self) -> i64 {
        self.created_at / 1000000
    }

    pub fn effective_scheduled_at_micros(&self) -> i64 {
        if self.scheduled_at > 0 {
            self.scheduled_at
        } else {
            self.created_at
        }
    }

    pub fn latency_micros(&self) -> i64 {
        let reference = self
            .started_at
            .unwrap_or_else(|| chrono::Utc::now().timestamp_micros());
        (reference - self.effective_scheduled_at_micros()).max(0)
    }

    pub fn latency_millis(&self) -> i64 {
        self.latency_micros() / 1000
    }

    pub fn scheduled_at(&self) -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp_micros(self.scheduled_at).unwrap_or_else(Utc::now)
    }

    pub fn created_at(&self) -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp_micros(self.created_at).unwrap_or_else(Utc::now)
    }

    pub fn started_at(&self) -> Option<DateTime<Utc>> {
        self.started_at
            .and_then(DateTime::<Utc>::from_timestamp_micros)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_meta(scheduled_at: i64, started_at: Option<i64>) -> JobMeta {
        JobMeta {
            id: "test".to_string(),
            retries: 0,
            unique: false,
            on_conflict: None,
            created_at: scheduled_at,
            scheduled_at,
            started_at,
            state: None,
            resurrect: true,
            error: None,
            throttle_cost: None,
        }
    }

    #[test]
    fn test_started_at_none() {
        let meta = make_meta(1_000_000, None);
        assert!(meta.started_at().is_none());
    }

    #[test]
    fn test_started_at_some() {
        let scheduled = 1_700_000_000_000_000i64;
        let started = scheduled + 5_500_000;
        let meta = make_meta(scheduled, Some(started));

        assert_eq!(
            meta.started_at().map(|t| t.timestamp_micros()),
            Some(started)
        );
    }

    #[test]
    fn test_latency_uses_started_at_when_available() {
        let scheduled = 1_700_000_000_000_000i64;
        let started = scheduled + 5_500_000;
        let meta = make_meta(scheduled, Some(started));

        assert_eq!(meta.latency_micros(), 5_500_000);
        assert_eq!(meta.latency_millis(), 5_500);
    }

    #[test]
    fn test_latency_falls_back_to_now_without_started_at() {
        let scheduled = chrono::Utc::now().timestamp_micros() - 2_000_000;
        let meta = make_meta(scheduled, None);

        let latency = meta.latency_micros();
        assert!(latency >= 2_000_000);
        assert!(latency < 3_000_000);
    }

    #[test]
    fn test_latency_clamped_to_zero() {
        let scheduled = 1_700_000_000_000_000i64;
        let started = scheduled - 100;
        let meta = make_meta(scheduled, Some(started));

        assert_eq!(meta.latency_micros(), 0);
    }

    #[test]
    fn test_with_retries_incremented_resets_started_at() {
        let envelope = JobEnvelope {
            id: "test".to_string(),
            queue: "default".to_string(),
            job: JobData {
                name: "TestJob".to_string(),
                args: serde_json::json!({}),
            },
            meta: JobMeta {
                id: "test".to_string(),
                retries: 0,
                unique: false,
                on_conflict: None,
                created_at: 1_000_000,
                scheduled_at: 1_000_000,
                started_at: Some(2_000_000),
                state: None,
                resurrect: true,
                error: None,
                throttle_cost: None,
            },
        };

        let retried = envelope.with_retries_incremented("something went wrong".to_string());
        assert_eq!(retried.meta.retries, 1);
        assert!(retried.meta.started_at.is_none());
        assert_eq!(retried.meta.error.as_deref(), Some("something went wrong"));
    }

    #[test]
    fn test_cannot_schedule_in_past() {
        let envelope = JobEnvelope {
            id: "test".to_string(),
            queue: "default".to_string(),
            job: JobData {
                name: "TestJob".to_string(),
                args: serde_json::json!({}),
            },
            meta: JobMeta {
                id: "test".to_string(),
                retries: 0,
                unique: false,
                on_conflict: None,
                created_at: 1_000_000,
                scheduled_at: 1_000_000,
                started_at: None,
                state: None,
                resurrect: true,
                error: None,
                throttle_cost: None,
            },
        };
        let past = Utc::now() - chrono::Duration::hours(1);

        let envelope = envelope.with_scheduled_at(past);

        let drift_micros = Utc::now().timestamp_micros() - envelope.meta.scheduled_at;
        assert!(drift_micros >= 0);
        assert!(drift_micros < 1_000_000);
    }

    #[test]
    fn test_cannot_schedule_cron_in_past() {
        #[derive(Serialize)]
        struct CronJob {}

        impl Job for CronJob {}

        let past = Utc::now().timestamp_micros() - 3_600_000_000;
        let envelope = JobEnvelope::new_cron(
            "default".to_string(),
            "cron-job-1".to_string(),
            &CronJob {},
            past,
            true,
        )
        .unwrap()
        .0;

        let drift_micros = Utc::now().timestamp_micros() - envelope.meta.scheduled_at;
        assert!(drift_micros >= 0);
        assert!(drift_micros < 1_000_000);
    }

    #[test]
    fn cron_envelope_rejects_replace_conflict() {
        #[derive(Serialize)]
        struct ReplaceCronJob {}

        impl Job for ReplaceCronJob {
            fn on_conflict(&self) -> JobConflictStrategy {
                JobConflictStrategy::Replace
            }
        }

        let error = JobEnvelope::new_cron(
            "default".to_string(),
            "CronJob-123".to_string(),
            &ReplaceCronJob {},
            Utc::now().timestamp_micros(),
            true,
        )
        .expect_err("replace conflict should be rejected for cron jobs");

        assert!(
            error
                .to_string()
                .contains("cron jobs do not support `on_conflict = Replace`; use `Skip`")
        );
    }

    #[test]
    fn test_serde_backward_compatibility() {
        let json = r#"{
            "id": "test",
            "retries": 0,
            "unique": false,
            "on_conflict": null,
            "created_at": 1000000,
            "scheduled_at": 1000000,
            "state": null,
            "resurrect": true
        }"#;

        let meta: JobMeta =
            serde_json::from_str(json).expect("should deserialize without started_at");
        assert!(meta.started_at.is_none());
        assert!(meta.error.is_none());
    }
}
