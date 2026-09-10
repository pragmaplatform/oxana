use super::partials;
use crate::{filters, models::*};
use topcoat::{
    context::Cx,
    view::{Unescaped, View, view},
};

impl MetricsTemplate {
    pub(crate) fn into_view(self, cx: &Cx) -> impl View + '_ {
        view! {
            cx =>
            let base_path = &self.base_path;
            let active_tab = self.active_tab;
            let metrics = &self.metrics;
            let table_workers = &self.table_workers;
            let sort = &self.sort;
            let dir = &self.dir;
            <!DOCTYPE html>
            <html lang="en">
                <head>
                    <meta charset="UTF-8">
                    <meta
                        name="viewport"
                        content="width=device-width, initial-scale=1.0"
                    >
                    <title>"Metrics - Oxana"</title>
                    <script src="https://cdn.tailwindcss.com"></script>
                    let tooltip_min_width = 9;
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
                            class="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 mb-6"
                        >
                            <h2 class="text-lg font-semibold">"Metrics"</h2>
                            <form
                                method="GET"
                                action=(format!("{}/metrics", base_path))
                                class="flex items-center gap-2 text-sm"
                            >
                                if self.has_sort() {
                                    <input type="hidden" name="sort" value=(sort)>
                                    <input type="hidden" name="dir" value=(dir)>
                                }
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
                                <div class="flex items-center justify-between mb-4">
                                    <h2 class="text-lg font-semibold">"Total Time"</h2>
                                </div>
                                <div
                                    class="relative bg-gray-900 border border-gray-800 rounded-lg p-4"
                                >
                                    <div id="execution-chart" class="w-full h-[300px]"></div>
                                </div>
                            </div>
                            <div>
                                <div class="flex items-center justify-between mb-4">
                                    <h2 class="text-lg font-semibold">"Total Processed"</h2>
                                </div>
                                <div
                                    class="relative bg-gray-900 border border-gray-800 rounded-lg p-4"
                                >
                                    <div id="processed-chart" class="w-full h-[300px]"></div>
                                </div>
                            </div>
                        </section>
                        <section>
                            <h2 class="text-lg font-semibold mb-4">"Workers"</h2>
                            if table_workers.is_empty() {
                                <div
                                    class="bg-gray-900 border border-gray-800 rounded-lg p-6 text-center text-gray-500"
                                >
                                    " No metrics "
                                </div>
                            } else {
                                <div class="overflow-x-auto">
                                    <table class="w-full text-sm">
                                        <thead>
                                            <tr
                                                class="border-b border-gray-800 text-left text-xs text-gray-400 uppercase tracking-wide"
                                            >
                                                <th class="pb-3 pr-4">"Worker"</th>
                                                <th class="pb-3 pr-4 text-right">
                                                    <a
                                                        href=(self.sort_href("processed"))
                                                        class="hover:text-gray-200"
                                                    >
                                                        "Processed"
                                                        (self.sort_arrow("processed"))
                                                    </a>
                                                </th>
                                                <th class="pb-3 pr-4 text-right">
                                                    <a
                                                        href=(self.sort_href("succeeded"))
                                                        class="hover:text-gray-200"
                                                    >
                                                        "Succeeded"
                                                        (self.sort_arrow("succeeded"))
                                                    </a>
                                                </th>
                                                <th class="pb-3 pr-4 text-right">
                                                    <a
                                                        href=(self.sort_href("failed"))
                                                        class="hover:text-gray-200"
                                                    >
                                                        "Failed"
                                                        (self.sort_arrow("failed"))
                                                    </a>
                                                </th>
                                                <th class="pb-3 pr-4 text-right">
                                                    <a
                                                        href=(self.sort_href("total_time"))
                                                        class="hover:text-gray-200"
                                                    >
                                                        "Total Time"
                                                        (self.sort_arrow("total_time"))
                                                    </a>
                                                </th>
                                                <th class="pb-3 text-right">
                                                    <a
                                                        href=(self.sort_href("avg_time"))
                                                        class="hover:text-gray-200"
                                                    >
                                                        "Avg Time"
                                                        (self.sort_arrow("avg_time"))
                                                    </a>
                                                </th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            for row in table_workers.iter() {
                                                <tr
                                                    class="border-b border-gray-800/50 hover:bg-gray-900/50"
                                                >
                                                    <td class="py-3 pr-4 font-mono text-sm">
                                                        <a
                                                            href=(format!(
                                                                "{}/metrics/job?worker={}&minutes={}",
                                                                base_path,
                                                                urlencoding::encode(&row.identity.worker),
                                                                metrics.minutes,
                                                            ))
                                                            title=(&row.identity.worker)
                                                            class="text-blue-400 hover:text-blue-300 hover:underline"
                                                        >
                                                            (self.metric_identity_label(&row.identity))
                                                        </a>
                                                    </td>
                                                    <td class="py-3 pr-4 text-right text-green-400">
                                                        (filters::format_number_u64(&row.totals.processed))
                                                    </td>
                                                    <td class="py-3 pr-4 text-right text-emerald-400">
                                                        (filters::format_number_u64(&row.totals.succeeded))
                                                    </td>
                                                    <td class="py-3 pr-4 text-right text-red-400">
                                                        (filters::format_number_u64(&row.totals.failed))
                                                    </td>
                                                    <td class="py-3 pr-4 text-right text-blue-400">
                                                        (filters::format_duration_ms(&row.totals.execution_ms))
                                                    </td>
                                                    <td class="py-3 text-right text-purple-400">
                                                        (filters::format_duration_ms_f64(
                                                            row.totals.average_execution_ms(),
                                                        ))
                                                    </td>
                                                </tr>
                                            }
                                        </tbody>
                                    </table>
                                </div>
                            }
                        </section>
                    </div>
                    <script>
                        (Unescaped::new_unchecked(include_str!("../assets/charts.js")))
                    </script>
                    <script type="application/json" id="metrics-data-0">
                        (partials::chart_json(self.execution_chart_data_json()))
                    </script>
                    <script type="application/json" id="metrics-data-1">
                        (partials::chart_json(self.processed_chart_data_json()))
                    </script>
                    <script>
                        (Unescaped::new_unchecked(include_str!("../assets/metrics.js")))
                    </script>
                </body>
            </html>
        }
    }
}
