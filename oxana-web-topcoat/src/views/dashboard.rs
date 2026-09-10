use super::partials;
use crate::{filters, models::*};
use topcoat::{
    context::Cx,
    view::{View, view},
};

impl DashboardTemplate {
    pub(crate) fn into_view(self, cx: &Cx) -> impl View + '_ {
        view! {
            cx =>
            let base_path = &self.base_path;
            let active_tab = self.active_tab;
            let stats = &self.stats;
            <!DOCTYPE html>
            <html lang="en">
                <head>
                    <meta charset="UTF-8">
                    <meta
                        name="viewport"
                        content="width=device-width, initial-scale=1.0"
                    >
                    <title>"Oxana Dashboard"</title>
                    <script src="https://cdn.tailwindcss.com"></script>
                </head>
                <body class="bg-gray-950 text-gray-100 min-h-screen">
                    <div class="max-w-[88rem] mx-auto px-4 sm:px-6 lg:px-8 py-8">
                        <div class="mb-6">
                            <h1 class="text-2xl font-bold tracking-tight">
                                "Oxana Dashboard"
                            </h1>
                        </div>
                        partials::tabs(base_path: base_path, active_tab: active_tab)
                        partials::stats_cards(base_path: base_path, stats: stats)
                        <section class="mb-10">
                            <h2 class="text-lg font-semibold mb-4">
                                "Active Processes ("
                                (stats.processes.len())
                                ")"
                            </h2>
                            if stats.processes.is_empty() {
                                <div
                                    class="bg-gray-900 border border-gray-800 rounded-lg p-6 text-center text-gray-500"
                                >
                                    " No active processes "
                                </div>
                            } else {
                                <div class="overflow-x-auto">
                                    <table class="w-full text-sm">
                                        <thead>
                                            <tr
                                                class="border-b border-gray-800 text-left text-xs text-gray-400 uppercase tracking-wide"
                                            >
                                                <th class="pb-3 pr-4">"Hostname"</th>
                                                <th class="pb-3 pr-4">"PID"</th>
                                                <th class="pb-3 pr-4 text-right">"Busy"</th>
                                                <th class="pb-3 pr-4">"Started"</th>
                                                <th class="pb-3">"Last Heartbeat"</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            for process in stats.processes.iter() {
                                                <tr
                                                    class="border-b border-gray-800/50 hover:bg-gray-900/50"
                                                >
                                                    <td class="py-3 pr-4 font-mono text-sm">
                                                        (&process.hostname)
                                                    </td>
                                                    <td class="py-3 pr-4 font-mono">(&process.pid)</td>
                                                    <td class="py-3 pr-4 text-right text-purple-400">
                                                        (self.busy_for_process(process))
                                                    </td>
                                                    <td class="py-3 pr-4 text-gray-400">
                                                        (filters::relative_time(&process.started_at))
                                                    </td>
                                                    <td class="py-3 text-gray-400">
                                                        (filters::relative_time(&process.heartbeat_at))
                                                    </td>
                                                </tr>
                                            }
                                        </tbody>
                                    </table>
                                </div>
                            }
                        </section>
                        <section>
                            <h2 class="text-lg font-semibold mb-4">
                                "Queues ("
                                (stats.queues.len())
                                ")"
                            </h2>
                            if stats.queues.is_empty() {
                                <div
                                    class="bg-gray-900 border border-gray-800 rounded-lg p-6 text-center text-gray-500"
                                >
                                    " No queues "
                                </div>
                            } else {
                                <div class="overflow-x-auto">
                                    <table class="w-full text-sm">
                                        <thead>
                                            <tr
                                                class="border-b border-gray-800 text-left text-xs text-gray-400 uppercase tracking-wide"
                                            >
                                                <th class="pb-3 pr-4">"Queue"</th>
                                                <th class="pb-3 pr-4 text-right">"Concurrency"</th>
                                                <th class="pb-3 pr-4 text-right">"State"</th>
                                                <th class="pb-3 pr-4 text-right">"Busy"</th>
                                                <th class="pb-3 pr-4 text-right">"Enqueued"</th>
                                                <th class="pb-3 text-right">"Latency"</th>
                                            </tr>
                                        </thead>
                                        <tbody>
                                            for queue in stats.queues.iter() {
                                                <tr
                                                    class="border-b border-gray-800/50 hover:bg-gray-900/50"
                                                >
                                                    <td class="py-3 pr-4 font-mono text-sm">
                                                        if queue.queues.is_empty() {
                                                            <a
                                                                href=(format!(
                                                                    "{}/queues/{}",
                                                                    base_path,
                                                                    urlencoding::encode(&queue.key),
                                                                ))
                                                                class="text-blue-400 hover:text-blue-300 hover:underline"
                                                            >
                                                                (&queue.key)
                                                            </a>
                                                        } else {
                                                            (&queue.key)
                                                        }
                                                    </td>
                                                    <td class="py-3 pr-4 text-right text-gray-300">
                                                        (self.concurrency_for(&queue.key))
                                                        if self.has_concurrency_override_for(&queue.key) {
                                                            " "
                                                            <span class="text-gray-500">
                                                                "("
                                                                <span class="line-through">
                                                                    (self.default_concurrency_for(&queue.key))
                                                                </span>
                                                                ")"
                                                            </span>
                                                        }
                                                    </td>
                                                    <td
                                                        class=(format!(
                                                            "py-3 pr-4 text-right {}",
                                                            self.state_class_for(&queue.key),
                                                        ))
                                                    >
                                                        (self.state_for(&queue.key))
                                                    </td>
                                                    <td class="py-3 pr-4 text-right text-purple-400">
                                                        (self.busy_for(&queue.key))
                                                    </td>
                                                    <td class="py-3 pr-4 text-right text-blue-400">
                                                        (&queue.enqueued)
                                                    </td>
                                                    <td class="py-3 text-right text-purple-400">
                                                        (filters::format_latency(&queue.latency_s))
                                                    </td>
                                                </tr>
                                                for dq in queue.queues.iter() {
                                                    if dq.enqueued > 0 {
                                                        let dq_key = self.dynamic_queue_key(&queue.key, &dq.suffix);
                                                        <tr
                                                            class="border-b border-gray-800/50 hover:bg-gray-900/50 text-gray-500"
                                                        >
                                                            <td class="py-2 pr-4 pl-10 font-mono text-xs">
                                                                "↳ "
                                                                <a
                                                                    href=(format!(
                                                                        "{}/queues/{}%23{}",
                                                                        base_path,
                                                                        queue.key,
                                                                        urlencoding::encode(&dq.suffix),
                                                                    ))
                                                                    class="text-blue-400 hover:text-blue-300 hover:underline"
                                                                >
                                                                    (&dq.suffix)
                                                                </a>
                                                            </td>
                                                            <td class="py-2 pr-4 text-right text-xs text-gray-400">
                                                                (self.concurrency_for(&dq_key))
                                                                if self.has_concurrency_override_for(&dq_key) {
                                                                    " "
                                                                    <span class="text-gray-600">
                                                                        "("
                                                                        <span class="line-through">
                                                                            (self.default_concurrency_for(&dq_key))
                                                                        </span>
                                                                        ")"
                                                                    </span>
                                                                }
                                                            </td>
                                                            <td
                                                                class=(format!(
                                                                    "py-2 pr-4 text-right text-xs {}",
                                                                    self.state_class_for(&dq_key),
                                                                ))
                                                            >
                                                                (self.state_for(&dq_key))
                                                            </td>
                                                            <td class="py-2 pr-4 text-right text-xs text-purple-400"></td>
                                                            <td class="py-2 pr-4 text-right text-xs text-blue-400">
                                                                (&dq.enqueued)
                                                            </td>
                                                            <td class="py-2 text-right text-xs text-purple-400">
                                                                (filters::format_latency(&dq.latency_s))
                                                            </td>
                                                        </tr>
                                                    }
                                                }
                                            }
                                        </tbody>
                                    </table>
                                </div>
                            }
                        </section>
                    </div>
                </body>
            </html>
        }
    }
}
