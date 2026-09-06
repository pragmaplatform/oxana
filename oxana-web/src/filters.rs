#[askama::filter_fn]
pub fn relative_time(ts: &i64, _env: &dyn askama::Values) -> askama::Result<String> {
    let now = chrono::Utc::now().timestamp();
    let diff_secs = now - ts;
    Ok(format_relative(diff_secs))
}

fn format_relative(diff_secs: i64) -> String {
    if diff_secs.abs() < 15 {
        return "now".to_string();
    }

    let abs = diff_secs.abs();
    let (prefix, suffix) = if diff_secs > 0 {
        ("", " ago")
    } else {
        ("in ", "")
    };

    if abs < 60 {
        format!("{prefix}{abs}s{suffix}")
    } else if abs < 3600 {
        format!("{prefix}{}m{suffix}", abs / 60)
    } else {
        let h = abs / 3600;
        let m = (abs % 3600) / 60;
        if m == 0 {
            format!("{prefix}{h}h{suffix}")
        } else {
            format!("{prefix}{h}h{m}m{suffix}")
        }
    }
}

#[askama::filter_fn]
pub fn relative_time_micros(ts: &i64, _env: &dyn askama::Values) -> askama::Result<String> {
    let now_micros = chrono::Utc::now().timestamp_micros();
    let diff_secs = (now_micros - ts) / 1_000_000;
    Ok(format_relative(diff_secs))
}

#[askama::filter_fn]
pub fn format_latency(secs: &f64, _env: &dyn askama::Values) -> askama::Result<String> {
    if *secs > 3600.0 {
        Ok(format!("{:.1}h", secs / 3600.0))
    } else if *secs > 600.0 {
        Ok(format!("{:.1}m", secs / 60.0))
    } else {
        Ok(format!("{secs:.1}s"))
    }
}

fn format_duration_from_ms(ms: f64) -> String {
    if ms >= 3_600_000.0 {
        format!("{:.1}h", ms / 3_600_000.0)
    } else if ms >= 60_000.0 {
        format!("{:.1}m", ms / 60_000.0)
    } else if ms >= 1_000.0 {
        format!("{:.2}s", ms / 1_000.0)
    } else {
        format!("{ms:.0}ms")
    }
}

#[askama::filter_fn]
pub fn format_duration_ms(val: &u64, _env: &dyn askama::Values) -> askama::Result<String> {
    Ok(format_duration_from_ms(*val as f64))
}

#[askama::filter_fn]
pub fn format_duration_ms_f64(val: f64, _env: &dyn askama::Values) -> askama::Result<String> {
    Ok(format_duration_from_ms(val))
}

fn format_duration_from_secs(secs: f64) -> String {
    if secs >= 3600.0 {
        format!("{:.1}h", secs / 3600.0)
    } else if secs >= 60.0 {
        format!("{:.1}m", secs / 60.0)
    } else {
        format!("{secs:.1}s")
    }
}

#[askama::filter_fn]
pub fn format_rate_per_minute(val: &f64, _env: &dyn askama::Values) -> askama::Result<String> {
    Ok(format!("{val:.1}/min"))
}

#[askama::filter_fn]
pub fn format_signed_rate_per_minute(
    val: &f64,
    _env: &dyn askama::Values,
) -> askama::Result<String> {
    Ok(format!("{val:+.1}/min"))
}

#[askama::filter_fn]
pub fn format_eta_s(val: &Option<f64>, _env: &dyn askama::Values) -> askama::Result<String> {
    Ok(val
        .map(format_duration_from_secs)
        .unwrap_or_else(|| "—".to_string()))
}

#[askama::filter_fn]
pub fn pretty_json(val: &serde_json::Value, _env: &dyn askama::Values) -> askama::Result<String> {
    Ok(serde_json::to_string_pretty(val).unwrap_or_else(|_| val.to_string()))
}

#[askama::filter_fn]
pub fn relative_time_micros_opt(
    ts: &Option<i64>,
    _env: &dyn askama::Values,
) -> askama::Result<String> {
    let Some(ts) = ts else {
        return Ok("—".to_string());
    };

    let now_micros = chrono::Utc::now().timestamp_micros();
    let diff_secs = (now_micros - ts) / 1_000_000;
    Ok(format_relative(diff_secs))
}

fn format_with_commas(mut n: u64) -> String {
    if n == 0 {
        return "0".to_string();
    }

    let mut groups = Vec::new();
    while n > 0 {
        groups.push(n % 1000);
        n /= 1000;
    }

    let mut result = groups.last().map_or_else(String::new, |g| g.to_string());
    for g in groups.iter().rev().skip(1) {
        result.push_str(&format!(",{g:03}"));
    }
    result
}

#[askama::filter_fn]
pub fn format_number(val: &usize, _env: &dyn askama::Values) -> askama::Result<String> {
    Ok(format_with_commas(*val as u64))
}

#[askama::filter_fn]
pub fn format_number_u64(val: &u64, _env: &dyn askama::Values) -> askama::Result<String> {
    Ok(format_with_commas(*val))
}

#[askama::filter_fn]
pub fn format_number_i64(val: &i64, _env: &dyn askama::Values) -> askama::Result<String> {
    let (prefix, abs) = if *val < 0 {
        ("-", val.unsigned_abs())
    } else {
        ("", *val as u64)
    };
    Ok(format!("{prefix}{}", format_with_commas(abs)))
}

#[askama::filter_fn]
pub fn has_args(val: &serde_json::Value, _env: &dyn askama::Values) -> askama::Result<bool> {
    let empty = match val {
        serde_json::Value::Null => true,
        serde_json::Value::Object(m) => m.is_empty(),
        _ => false,
    };
    Ok(!empty)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimpleArgPill {
    pub key: String,
    pub value: String,
}

fn simple_arg_value(val: &serde_json::Value) -> Option<String> {
    match val {
        serde_json::Value::Null => Some("null".to_string()),
        serde_json::Value::Bool(value) => Some(value.to_string()),
        serde_json::Value::Number(value) => Some(value.to_string()),
        serde_json::Value::String(value) => Some(if value.is_empty() {
            "\"\"".to_string()
        } else {
            value.clone()
        }),
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => None,
    }
}

fn simple_args_for(val: &serde_json::Value) -> Vec<SimpleArgPill> {
    let serde_json::Value::Object(args) = val else {
        return Vec::new();
    };

    args.iter()
        .filter_map(|(key, value)| {
            simple_arg_value(value).map(|value| SimpleArgPill {
                key: key.clone(),
                value,
            })
        })
        .collect()
}

#[askama::filter_fn]
pub fn simple_args(
    val: &serde_json::Value,
    _env: &dyn askama::Values,
) -> askama::Result<Vec<SimpleArgPill>> {
    Ok(simple_args_for(val))
}

fn should_show_args_json(val: &serde_json::Value) -> bool {
    match val {
        serde_json::Value::Null => false,
        serde_json::Value::Object(args) if args.is_empty() => false,
        serde_json::Value::Object(args) => {
            args.values().any(|value| simple_arg_value(value).is_none())
        }
        serde_json::Value::Array(_)
        | serde_json::Value::Bool(_)
        | serde_json::Value::Number(_)
        | serde_json::Value::String(_) => true,
    }
}

#[askama::filter_fn]
pub fn show_args_json(val: &serde_json::Value, _env: &dyn askama::Values) -> askama::Result<bool> {
    Ok(should_show_args_json(val))
}

fn progress_parts(val: &serde_json::Value) -> Option<oxana::JobProgress> {
    serde_json::from_value(val.clone()).ok()
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ProgressView {
    pub summary: String,
    pub percent: String,
    pub note: String,
    pub eta: String,
}

impl ProgressView {
    fn from_state(
        state: &serde_json::Value,
        started_at_micros: Option<i64>,
        now_micros: i64,
    ) -> Option<Self> {
        let progress = progress_parts(state)?;
        if progress.total <= 0 {
            return None;
        }

        let percent =
            ((progress.cursor.max(0) as f64 / progress.total as f64) * 100.0).clamp(0.0, 100.0);
        let eta = progress_eta_s(&progress, started_at_micros, now_micros)
            .map(format_duration_from_secs)
            .unwrap_or_else(|| "—".to_string());

        Some(Self {
            summary: format!(
                "{} / {} ({percent:.0}%)",
                format_with_commas(progress.cursor.max(0) as u64),
                format_with_commas(progress.total as u64),
            ),
            percent: format!("{percent:.0}"),
            note: progress.note.unwrap_or_default(),
            eta,
        })
    }
}

fn progress_eta_s(
    progress: &oxana::JobProgress,
    started_at_micros: Option<i64>,
    now_micros: i64,
) -> Option<f64> {
    let started_at_micros = started_at_micros?;
    if progress.total <= 0 || progress.cursor <= 0 || started_at_micros <= 0 {
        return None;
    }

    let remaining = progress.total.saturating_sub(progress.cursor).max(0);
    if remaining == 0 {
        return Some(0.0);
    }

    let elapsed_s = (now_micros - started_at_micros) as f64 / 1_000_000.0;
    if elapsed_s <= 0.0 {
        return None;
    }

    let cursor = progress.cursor.max(0) as f64;
    Some(elapsed_s * (remaining as f64 / cursor))
}

#[askama::filter_fn]
pub fn job_progress(
    state: &serde_json::Value,
    _env: &dyn askama::Values,
    started_at_micros: &Option<i64>,
) -> askama::Result<Option<ProgressView>> {
    Ok(ProgressView::from_state(
        state,
        *started_at_micros,
        chrono::Utc::now().timestamp_micros(),
    ))
}

#[cfg(test)]
mod tests {
    use super::{
        ProgressView, SimpleArgPill, progress_parts, should_show_args_json, simple_args_for,
    };
    use serde_json::json;

    fn progress_eta_s(state: &serde_json::Value, started_at: Option<i64>, now: i64) -> Option<f64> {
        super::progress_eta_s(&progress_parts(state)?, started_at, now)
    }

    fn should_show_progress(state: &serde_json::Value) -> bool {
        ProgressView::from_state(state, None, 0).is_some()
    }

    #[test]
    fn progress_view_formats_object_and_tuple_states_identically() {
        let expected = ProgressView {
            summary: "1,250 / 5,000 (25%)".to_string(),
            percent: "25".to_string(),
            note: "Importing".to_string(),
            eta: "30.0s".to_string(),
        };
        for state in [
            json!({"cursor": 1250, "total": 5000, "note": "Importing"}),
            json!([1250, 5000, "Importing"]),
        ] {
            assert_eq!(
                ProgressView::from_state(&state, Some(1_000_000), 11_000_000).as_ref(),
                Some(&expected)
            );
        }
    }

    #[test]
    fn progress_view_clamps_percent_and_preserves_unknown_eta() {
        let negative = ProgressView::from_state(&json!([-1, 10]), None, 0).unwrap();
        assert_eq!(negative.summary, "0 / 10 (0%)");
        assert_eq!(negative.percent, "0");
        assert_eq!(negative.eta, "—");
        assert!(negative.note.is_empty());

        let complete =
            ProgressView::from_state(&json!([15, 10]), Some(1_000_000), 11_000_000).unwrap();
        assert_eq!(complete.summary, "15 / 10 (100%)");
        assert_eq!(complete.percent, "100");
        assert_eq!(complete.eta, "0.0s");
    }

    #[test]
    fn simple_args_include_top_level_scalar_values() {
        let value = json!({
            "game_id": 12345,
            "league": "nba",
            "dry_run": true,
            "missing": null,
        });

        assert_eq!(
            simple_args_for(&value),
            vec![
                SimpleArgPill {
                    key: "game_id".to_string(),
                    value: "12345".to_string(),
                },
                SimpleArgPill {
                    key: "league".to_string(),
                    value: "nba".to_string(),
                },
                SimpleArgPill {
                    key: "dry_run".to_string(),
                    value: "true".to_string(),
                },
                SimpleArgPill {
                    key: "missing".to_string(),
                    value: "null".to_string(),
                },
            ]
        );
    }

    #[test]
    fn simple_args_include_scalar_values_from_mixed_args() {
        let value = json!({
            "game_id": 12345,
            "metadata": { "season": 2026 },
        });

        assert_eq!(
            simple_args_for(&value),
            vec![SimpleArgPill {
                key: "game_id".to_string(),
                value: "12345".to_string(),
            }]
        );
    }

    #[test]
    fn simple_args_require_an_object() {
        assert!(simple_args_for(&json!(12345)).is_empty());
        assert!(simple_args_for(&json!(["game_id", 12345])).is_empty());
    }

    #[test]
    fn args_json_only_shows_when_pills_do_not_cover_everything() {
        assert!(!should_show_args_json(&json!({})));
        assert!(!should_show_args_json(&json!({
            "game_id": 12345,
            "dry_run": false,
        })));
        assert!(should_show_args_json(&json!({
            "game_id": 12345,
            "metadata": { "season": 2026 },
        })));
        assert!(should_show_args_json(&json!(12345)));
    }

    #[test]
    fn progress_parts_recognizes_update_progress_state() {
        let value = json!({
            "cursor": 25,
            "total": 100,
            "note": "importing users"
        });

        assert_eq!(
            progress_parts(&value),
            Some(oxana::JobProgress {
                cursor: 25,
                total: 100,
                note: Some("importing users".to_string()),
            })
        );
        assert_eq!(
            progress_parts(&json!(42)),
            Some(oxana::JobProgress::from(42))
        );
        assert_eq!(
            progress_parts(&json!([25, 100])),
            Some(oxana::JobProgress {
                cursor: 25,
                total: 100,
                note: None,
            })
        );
    }

    #[test]
    fn progress_display_requires_total() {
        assert!(!should_show_progress(&json!(42)));
        assert!(!should_show_progress(&json!({
            "cursor": 42,
            "total": 0
        })));
        assert!(should_show_progress(&json!([1, 100])));
        assert!(should_show_progress(&json!({
            "cursor": 42,
            "total": 100
        })));
    }

    #[test]
    fn progress_eta_uses_started_time_and_progress_rate() {
        let state = json!({
            "cursor": 25,
            "total": 100
        });

        assert_eq!(
            progress_eta_s(&state, Some(9_000_000), 19_000_000),
            Some(30.0)
        );
    }

    #[test]
    fn progress_eta_ignores_time_spent_waiting_in_queue() {
        let state = json!({
            "cursor": 25,
            "total": 100
        });

        assert_eq!(
            progress_eta_s(&state, Some(43_201_000_000), 43_211_000_000),
            Some(30.0)
        );
    }

    #[test]
    fn progress_eta_is_unknown_without_rate() {
        assert_eq!(
            progress_eta_s(
                &json!({
                    "cursor": 0,
                    "total": 100
                }),
                Some(1_000_000),
                11_000_000
            ),
            None
        );
        assert_eq!(progress_eta_s(&json!([25, 100]), None, 11_000_000), None);
    }

    #[test]
    fn progress_eta_is_zero_when_complete() {
        assert_eq!(
            progress_eta_s(&json!([100, 100]), Some(1_000_000), 11_000_000),
            Some(0.0)
        );
        assert_eq!(
            progress_eta_s(&json!([125, 100]), Some(1_000_000), 11_000_000),
            Some(0.0)
        );
    }
}
