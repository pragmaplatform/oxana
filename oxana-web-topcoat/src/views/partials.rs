use crate::filters;
use topcoat::{
    context::Cx,
    view::{Unescaped, View, component, view},
};

#[component]
pub(super) async fn tabs(cx: &Cx, base_path: &str, active_tab: &str) -> topcoat::Result<impl View> {
    Ok(view! {
        cx =>
        <nav class="flex gap-1 mb-8 border-b border-gray-800">
            <a
                href=(if base_path.is_empty() { "/" } else { base_path })
                class=(format!(
                    "px-4 py-2 text-sm font-medium {}",
                    if active_tab.is_empty() {
                        "text-white border-b-2 border-blue-500"
                    } else {
                        "text-gray-400 border-b-2 border-transparent hover:text-gray-200 hover:border-gray-600"
                    },
                ))
            >
                "Overview"
            </a>
            <a
                href=(format!("{}/busy", base_path))
                class=(format!(
                    "px-4 py-2 text-sm font-medium {}",
                    if active_tab == "/busy" {
                        "text-white border-b-2 border-blue-500"
                    } else {
                        "text-gray-400 border-b-2 border-transparent hover:text-gray-200 hover:border-gray-600"
                    },
                ))
            >
                "Busy"
            </a>
            <a
                href=(format!("{}/queues", base_path))
                class=(format!(
                    "px-4 py-2 text-sm font-medium {}",
                    if active_tab == "/queues" {
                        "text-white border-b-2 border-blue-500"
                    } else {
                        "text-gray-400 border-b-2 border-transparent hover:text-gray-200 hover:border-gray-600"
                    },
                ))
            >
                "Queues"
            </a>
            <a
                href=(format!("{}/metrics", base_path))
                class=(format!(
                    "px-4 py-2 text-sm font-medium {}",
                    if active_tab == "/metrics" {
                        "text-white border-b-2 border-blue-500"
                    } else {
                        "text-gray-400 border-b-2 border-transparent hover:text-gray-200 hover:border-gray-600"
                    },
                ))
            >
                "Metrics"
            </a>
            <a
                href=(format!("{}/cron", base_path))
                class=(format!(
                    "px-4 py-2 text-sm font-medium {}",
                    if active_tab == "/cron" {
                        "text-white border-b-2 border-blue-500"
                    } else {
                        "text-gray-400 border-b-2 border-transparent hover:text-gray-200 hover:border-gray-600"
                    },
                ))
            >
                "Cron"
            </a>
            <a
                href=(format!("{}/on-demand", base_path))
                class=(format!(
                    "px-4 py-2 text-sm font-medium {}",
                    if active_tab == "/on-demand" {
                        "text-white border-b-2 border-blue-500"
                    } else {
                        "text-gray-400 border-b-2 border-transparent hover:text-gray-200 hover:border-gray-600"
                    },
                ))
            >
                "On-Demand"
            </a>
            <a
                href=(format!("{}/scheduled", base_path))
                class=(format!(
                    "px-4 py-2 text-sm font-medium {}",
                    if active_tab == "/scheduled" {
                        "text-white border-b-2 border-blue-500"
                    } else {
                        "text-gray-400 border-b-2 border-transparent hover:text-gray-200 hover:border-gray-600"
                    },
                ))
            >
                "Scheduled"
            </a>
            <a
                href=(format!("{}/retries", base_path))
                class=(format!(
                    "px-4 py-2 text-sm font-medium {}",
                    if active_tab == "/retries" {
                        "text-white border-b-2 border-blue-500"
                    } else {
                        "text-gray-400 border-b-2 border-transparent hover:text-gray-200 hover:border-gray-600"
                    },
                ))
            >
                "Retries"
            </a>
            <a
                href=(format!("{}/dead", base_path))
                class=(format!(
                    "px-4 py-2 text-sm font-medium {}",
                    if active_tab == "/dead" {
                        "text-white border-b-2 border-blue-500"
                    } else {
                        "text-gray-400 border-b-2 border-transparent hover:text-gray-200 hover:border-gray-600"
                    },
                ))
            >
                "Dead"
            </a>
        </nav>
    })
}

#[component]
pub(super) async fn stats_cards(
    cx: &Cx,
    base_path: &str,
    stats: &oxana::Stats,
) -> topcoat::Result<impl View> {
    Ok(view! {
        cx =>
        " "
        <div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-4 mb-10">
            <a
                href=(format!("{}/busy", base_path))
                class="bg-gray-900 rounded-lg p-4 border border-gray-800 hover:border-gray-600 transition-colors block"
            >
                <div class="text-xs text-gray-400 uppercase tracking-wide mb-1">
                    "Busy"
                </div>
                <div class="text-2xl font-semibold text-purple-400">
                    (stats.processing.len())
                </div>
            </a>
            <a
                href=(format!("{}/queues", base_path))
                class="bg-gray-900 rounded-lg p-4 border border-gray-800 hover:border-gray-600 transition-colors block"
            >
                <div class="text-xs text-gray-400 uppercase tracking-wide mb-1">
                    "Enqueued"
                </div>
                <div class="text-2xl font-semibold text-blue-400">
                    (filters::format_number(&stats.global.enqueued))
                </div>
            </a>
            <a
                href=(format!("{}/retries", base_path))
                class="bg-gray-900 rounded-lg p-4 border border-gray-800 hover:border-gray-600 transition-colors block"
            >
                <div class="text-xs text-gray-400 uppercase tracking-wide mb-1">
                    "Retries"
                </div>
                <div class="text-2xl font-semibold text-orange-400">
                    (filters::format_number(&stats.global.retries))
                </div>
            </a>
            <a
                href=(format!("{}/scheduled", base_path))
                class="bg-gray-900 rounded-lg p-4 border border-gray-800 hover:border-gray-600 transition-colors block"
            >
                <div class="text-xs text-gray-400 uppercase tracking-wide mb-1">
                    "Scheduled"
                </div>
                <div class="text-2xl font-semibold text-yellow-400">
                    (filters::format_number(&stats.global.scheduled))
                </div>
            </a>
            <a
                href=(format!("{}/dead", base_path))
                class="bg-gray-900 rounded-lg p-4 border border-gray-800 hover:border-gray-600 transition-colors block"
            >
                <div class="text-xs text-gray-400 uppercase tracking-wide mb-1">
                    "Dead"
                </div>
                <div class="text-2xl font-semibold text-red-400">
                    (filters::format_number(&stats.global.dead))
                </div>
            </a>
            <div class="bg-gray-900 rounded-lg p-4 border border-gray-800">
                <div class="text-xs text-gray-400 uppercase tracking-wide mb-1">
                    "Latency"
                </div>
                <div class="text-2xl font-semibold text-yellow-400">
                    (filters::format_latency(&stats.global.latency_s_max))
                </div>
            </div>
        </div>
    })
}

#[component]
pub(super) async fn argument_pills(
    cx: &Cx,
    arg_pills: &[filters::SimpleArgPill],
) -> topcoat::Result<impl View> {
    Ok(view! {
        cx =>
        if !arg_pills.is_empty() {
            <div class="mt-3 flex flex-wrap gap-2">
                for arg in arg_pills.iter() {
                    <span
                        class="inline-flex max-w-full overflow-hidden rounded-full border border-gray-700 bg-gray-800/70 align-middle text-xs"
                        title=(format!("{}: {}", arg.key, arg.value))
                        aria-label=(format!("Argument {} {}", arg.key, arg.value))
                    >
                        <span
                            class="shrink-0 border-r border-gray-700 px-2 py-1 text-gray-400"
                        >
                            (&arg.key)
                        </span>
                        <span
                            class="min-w-0 truncate px-2 py-1 font-mono text-gray-100"
                        >
                            (&arg.value)
                        </span>
                    </span>
                }
            </div>
        }
    })
}

#[component]
pub(super) async fn job_progress(
    cx: &Cx,
    progress: &filters::ProgressView,
) -> topcoat::Result<impl View> {
    Ok(view! {
        cx =>
        <div class="mt-3 rounded border border-cyan-900/60 bg-cyan-950/20 p-3">
            <div class="flex items-start justify-between gap-3">
                <div>
                    <div class="text-xs font-medium text-cyan-200">"Progress"</div>
                    <div class="mt-1 text-xs text-gray-400">(&progress.summary)</div>
                </div>
                <div class="shrink-0 text-right">
                    <div class="text-sm font-semibold text-cyan-200">
                        (&progress.percent)
                        "%"
                    </div>
                    <div class="mt-1 text-[11px] text-cyan-200/70">
                        "ETA "
                        (&progress.eta)
                    </div>
                </div>
            </div>
            <div class="mt-3 h-2 overflow-hidden rounded-full bg-gray-800">
                <div
                    class="h-full rounded-full bg-cyan-400 transition-all"
                    style=(format!("width: {}%", progress.percent))
                ></div>
            </div>
            if !progress.note.is_empty() {
                <div class="mt-2 text-xs text-gray-300">(&progress.note)</div>
            }
        </div>
    })
}

#[component]
pub(super) async fn chart_head(cx: &Cx, tooltip_min_width: usize) -> topcoat::Result<impl View> {
    Ok(view! {
        cx =>
        " "
        <link
            rel="stylesheet"
            href="https://cdn.jsdelivr.net/npm/uplot@1.6.32/dist/uPlot.min.css"
        >
        <script
            defer=(true)
            src="https://cdn.jsdelivr.net/npm/uplot@1.6.32/dist/uPlot.iife.min.js"
        ></script>
        <style>
            (Unescaped::new_unchecked(include_str!("../assets/charts.css")))
            (Unescaped::new_unchecked(
                format!(":root {{ --chart-tooltip-min-width: {tooltip_min_width}rem; }}"),
            ))
        </style>
    })
}

/// JSON in a script element must not be able to close that element.
pub(super) fn chart_json(json: String) -> Unescaped<String> {
    Unescaped::new_unchecked(json.replace('<', "\\u003c"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use topcoat::view::ViewExt;

    #[tokio::test]
    async fn chart_labels_cannot_close_the_json_script() {
        let cx = &Cx::default();
        let data = serde_json::json!({"label": "</script><script>alert('queue')</script>"});
        let payload = data.to_string();
        let rendered = view! {
            cx =>
            <script type="application/json">(chart_json(payload))</script>
        }
        .single()
        .await
        .unwrap()
        .render(cx);
        assert_eq!(rendered.matches("</script>").count(), 1);
        let json = rendered
            .strip_prefix("<script type=\"application/json\">")
            .unwrap()
            .strip_suffix("</script>")
            .unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(json).unwrap(),
            data
        );
    }
}
