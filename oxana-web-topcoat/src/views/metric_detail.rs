use super::partials;
use crate::{filters, models::*};
use topcoat::{
    context::Cx,
    view::{Unescaped, View, view},
};

impl MetricDetailTemplate {
    pub(crate) fn into_view(self, cx: &Cx) -> impl View + '_ {
        view! {
            cx =>
            let base_path = &self.base_path;
            let active_tab = self.active_tab;
            let metrics = &self.metrics;
            <!DOCTYPE html>
            <html lang="en">
                <head>
                    <meta charset="UTF-8">
                    <meta
                        name="viewport"
                        content="width=device-width, initial-scale=1.0"
                    >
                    <title>
                        "Metric: "
                        (&metrics.identity.worker)
                        " - Oxana"
                    </title>
                    <script src="https://cdn.tailwindcss.com"></script>
                    let tooltip_min_width = 10;
                    partials::chart_head(tooltip_min_width: tooltip_min_width)
                </head>
                <body class="bg-gray-950 text-gray-100 min-h-screen">
                    <div class="max-w-[88rem] mx-auto px-4 sm:px-6 lg:px-8 py-8">
                        <div class="mb-6">
                            <h1 class="text-2xl font-bold tracking-tight">
                                "Oxana Dashboard"
                            </h1>
                        </div>
                        partials::tabs(base_path: base_path, active_tab: active_tab)
                        <div
                            class="flex flex-col lg:flex-row lg:items-start lg:justify-between gap-4 mb-6"
                        >
                            <div>
                                <a
                                    href=(format!(
                                        "{}/metrics?minutes={}",
                                        base_path,
                                        metrics.minutes,
                                    ))
                                    class="text-sm text-gray-400 hover:text-gray-200"
                                >
                                    "← Metrics"
                                </a>
                                <h2 class="mt-2 text-lg font-semibold font-mono break-all">
                                    (&metrics.identity.worker)
                                </h2>
                            </div>
                            <form
                                method="GET"
                                action=(format!("{}/metrics/job", base_path))
                                class="flex items-center gap-2 text-sm"
                            >
                                <input
                                    type="hidden"
                                    name="worker"
                                    value=(&metrics.identity.worker)
                                >
                                <label for="minutes" class="text-gray-400">"Window"</label>
                                <select
                                    id="minutes"
                                    name="minutes"
                                    class="bg-gray-900 border border-gray-800 rounded px-3 py-1.5 text-gray-200"
                                    onchange="this.form.submit()"
                                >
                                    <option
                                        value="60"
                                        if metrics.minutes == 60 {
                                            selected=(true)
                                        }
                                    >
                                        "60 minutes"
                                    </option>
                                    <option
                                        value="120"
                                        if metrics.minutes == 120 {
                                            selected=(true)
                                        }
                                    >
                                        "2 hours"
                                    </option>
                                    <option
                                        value="240"
                                        if metrics.minutes == 240 {
                                            selected=(true)
                                        }
                                    >
                                        "4 hours"
                                    </option>
                                    <option
                                        value="480"
                                        if metrics.minutes == 480 {
                                            selected=(true)
                                        }
                                    >
                                        "8 hours"
                                    </option>
                                    <option
                                        value="1440"
                                        if metrics.minutes == 1440 {
                                            selected=(true)
                                        }
                                    >
                                        "24 hours"
                                    </option>
                                </select>
                            </form>
                        </div>
                        <div class="grid grid-cols-2 sm:grid-cols-5 gap-4 mb-8">
                            <div
                                class="bg-gray-900 rounded-lg p-4 border border-gray-800"
                            >
                                <div
                                    class="text-xs text-gray-400 uppercase tracking-wide mb-1"
                                >
                                    "Processed"
                                </div>
                                <div class="text-2xl font-semibold text-green-400">
                                    (filters::format_number_u64(&metrics.totals.processed))
                                </div>
                            </div>
                            <div
                                class="bg-gray-900 rounded-lg p-4 border border-gray-800"
                            >
                                <div
                                    class="text-xs text-gray-400 uppercase tracking-wide mb-1"
                                >
                                    "Failed"
                                </div>
                                <div class="text-2xl font-semibold text-red-400">
                                    (filters::format_number_u64(&metrics.totals.failed))
                                </div>
                            </div>
                            <div
                                class="bg-gray-900 rounded-lg p-4 border border-gray-800"
                            >
                                <div
                                    class="text-xs text-gray-400 uppercase tracking-wide mb-1"
                                >
                                    "Succeeded"
                                </div>
                                <div class="text-2xl font-semibold text-emerald-400">
                                    (filters::format_number_u64(&metrics.totals.succeeded))
                                </div>
                            </div>
                            <div
                                class="bg-gray-900 rounded-lg p-4 border border-gray-800"
                            >
                                <div
                                    class="text-xs text-gray-400 uppercase tracking-wide mb-1"
                                >
                                    "Total Time"
                                </div>
                                <div class="text-2xl font-semibold text-blue-400">
                                    (filters::format_duration_ms(&metrics.totals.execution_ms))
                                </div>
                            </div>
                            <div
                                class="bg-gray-900 rounded-lg p-4 border border-gray-800"
                            >
                                <div
                                    class="text-xs text-gray-400 uppercase tracking-wide mb-1"
                                >
                                    "Avg Time"
                                </div>
                                <div class="text-2xl font-semibold text-purple-400">
                                    (filters::format_duration_ms_f64(
                                        metrics.totals.average_execution_ms(),
                                    ))
                                </div>
                            </div>
                        </div>
                        <section class="space-y-6 mb-10">
                            <div>
                                <h2 class="text-lg font-semibold mb-4">"Average Time"</h2>
                                <div
                                    class="relative bg-gray-900 border border-gray-800 rounded-lg p-4"
                                >
                                    <div id="average-chart" class="w-full h-[300px]"></div>
                                </div>
                            </div>
                            <div>
                                <div
                                    class="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-2 mb-4"
                                >
                                    <h2 class="text-lg font-semibold">"Total Executions"</h2>
                                    <div
                                        class="flex flex-wrap gap-x-4 gap-y-2 text-xs text-gray-400"
                                    >
                                        <span class="inline-flex items-center gap-2">
                                            <span class="h-2 w-2 rounded-full bg-emerald-400"></span>
                                            "Succeeded"
                                        </span>
                                        <span class="inline-flex items-center gap-2">
                                            <span class="h-2 w-2 rounded-full bg-red-500"></span>
                                            "Failed"
                                        </span>
                                        <span class="inline-flex items-center gap-2">
                                            <span
                                                class="h-2 w-2 rounded-full bg-black ring-1 ring-gray-500"
                                            ></span>
                                            "Panicked"
                                        </span>
                                    </div>
                                </div>
                                <div
                                    class="relative bg-gray-900 border border-gray-800 rounded-lg p-4"
                                >
                                    <div id="executions-chart" class="w-full h-[300px]"></div>
                                </div>
                            </div>
                        </section>
                        <section>
                            <h2 class="text-lg font-semibold mb-4">"Histogram"</h2>
                            <div
                                class="bg-gray-900 border border-gray-800 rounded-lg p-4"
                            >
                                <div class="space-y-2">
                                    for bucket in metrics.histogram.iter() {
                                        <div
                                            class="grid grid-cols-[4.5rem_1fr_5rem] items-center gap-3 text-sm"
                                        >
                                            <div class="text-gray-400 font-mono text-xs">
                                                (&bucket.label)
                                            </div>
                                            <div
                                                class="h-5 bg-gray-950 rounded border border-gray-800 overflow-hidden"
                                            >
                                                <div
                                                    class="h-full bg-blue-500/70"
                                                    style=(format!(
                                                        "width: {}",
                                                        self.histogram_width(&bucket.count),
                                                    ))
                                                ></div>
                                            </div>
                                            <div class="text-right text-gray-300 font-mono text-xs">
                                                (filters::format_number_u64(&bucket.count))
                                            </div>
                                        </div>
                                    }
                                </div>
                            </div>
                        </section>
                    </div>
                    <script>
                        (Unescaped::new_unchecked(include_str!("../assets/charts.js")))
                    </script>
                    <script type="application/json" id="metric-detail-data-0">
                        (partials::chart_json(self.detail_average_chart_data_json()))
                    </script>
                    <script type="application/json" id="metric-detail-data-1">
                        (partials::chart_json(self.detail_total_chart_data_json()))
                    </script>
                    <script>
                        (Unescaped::new_unchecked(
                            include_str!("../assets/metric_detail.js"),
                        ))
                    </script>
                </body>
            </html>
        }
    }
}
