use super::partials;
use crate::{filters, models::*};
use topcoat::{
    context::Cx,
    view::{Unescaped, View, view},
};

impl BusyTemplate {
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
                    <title>"Oxana Dashboard - Busy"</title>
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
                        <section class="mb-10">
                            <h2 class="text-lg font-semibold mb-4">"Active Queues"</h2>
                            if stats.queues.is_empty() {
                                <div
                                    class="bg-gray-900 border border-gray-800 rounded-lg p-6 text-center text-gray-500"
                                >
                                    " No active queues "
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
                                                let busy = self.busy_for(&queue.key);
                                                if queue.enqueued > 0 || busy > 0 {
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
                                                            (busy)
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
                                            }
                                        </tbody>
                                    </table>
                                </div>
                            }
                        </section>
                        <section class="mb-10">
                            <h2 class="text-lg font-semibold mb-4">
                                "Currently Processing ("
                                (stats.processing.len())
                                ")"
                            </h2>
                            if stats.processing.is_empty() {
                                <div
                                    class="bg-gray-900 border border-gray-800 rounded-lg p-6 text-center text-gray-500"
                                >
                                    " No jobs currently being processed "
                                </div>
                            } else {
                                <div class="space-y-3">
                                    for item in stats.processing.iter() {
                                        <div
                                            class="bg-gray-900 border border-gray-800 rounded-lg p-4"
                                        >
                                            <div class="flex items-start justify-between mb-2">
                                                <div class="flex items-center gap-2">
                                                    <span
                                                        class="inline-block w-2 h-2 rounded-full bg-green-400 animate-pulse"
                                                    ></span>
                                                    <span class="font-semibold text-sm">
                                                        (&item.job_envelope.job.name)
                                                    </span>
                                                </div>
                                                <a
                                                    href=(format!(
                                                        "{}/jobs/{}",
                                                        base_path,
                                                        urlencoding::encode(&item.job_envelope.id),
                                                    ))
                                                    class="text-xs text-gray-500 hover:text-blue-300 hover:underline font-mono"
                                                >
                                                    (&item.job_envelope.id)
                                                </a>
                                            </div>
                                            <div
                                                class="grid grid-cols-1 sm:grid-cols-5 gap-2 text-xs text-gray-400 mt-2"
                                            >
                                                <div>
                                                    <span class="text-gray-500">"Process:"</span>
                                                    <span class="ml-1 text-gray-300 font-mono">
                                                        (&item.process_id)
                                                    </span>
                                                </div>
                                                <div>
                                                    <span class="text-gray-500">"Queue:"</span>
                                                    <a
                                                        href=(format!(
                                                            "{}/queues/{}",
                                                            base_path,
                                                            urlencoding::encode(&item.job_envelope.queue),
                                                        ))
                                                        class="ml-1 text-blue-400 hover:text-blue-300 hover:underline"
                                                    >
                                                        (&item.job_envelope.queue)
                                                    </a>
                                                </div>
                                                <div>
                                                    <span class="text-gray-500">"Started:"</span>
                                                    <span class="ml-1 text-gray-300">
                                                        (filters::relative_time_micros_opt(
                                                            &item.job_envelope.meta.started_at,
                                                        ))
                                                    </span>
                                                </div>
                                                <div>
                                                    <span class="text-gray-500">"Scheduled:"</span>
                                                    <span class="ml-1 text-gray-300">
                                                        (filters::relative_time_micros(
                                                            &item.job_envelope.meta.scheduled_at,
                                                        ))
                                                    </span>
                                                </div>
                                                <div>
                                                    <span class="text-gray-500">"Retries:"</span>
                                                    <span class="ml-1 text-gray-300">
                                                        (&item.job_envelope.meta.retries)
                                                    </span>
                                                </div>
                                            </div>
                                            let arg_pills = filters::simple_args(
                                                &item.job_envelope.job.args,
                                            );
                                            partials::argument_pills(arg_pills: &arg_pills)
                                            if filters::show_args_json(&item.job_envelope.job.args) {
                                                <div class="mt-3">
                                                    <details>
                                                        <summary
                                                            class="text-xs text-gray-500 cursor-pointer hover:text-gray-300"
                                                        >
                                                            "Arguments"
                                                        </summary>
                                                        <pre
                                                            class="mt-2 text-xs bg-gray-950 rounded p-3 overflow-x-auto text-gray-300 border border-gray-800"
                                                        >
                                                            (filters::pretty_json(&item.job_envelope.job.args))
                                                        </pre>
                                                    </details>
                                                </div>
                                            }
                                            match &item.job_envelope.meta.state {
                                                Some(state) => {
                                                    if filters::has_args(state) {
                                                        let progress_started_at = item.job_envelope.meta.started_at;
                                                        if let Some(progress) = filters::job_progress(
                                                            state,
                                                            &progress_started_at,
                                                        ) {
                                                            partials::job_progress(progress: &progress)
                                                        } else {
                                                            <div class="mt-3">
                                                                <details>
                                                                    <summary
                                                                        class="text-xs text-gray-500 cursor-pointer hover:text-gray-300"
                                                                    >
                                                                        "State"
                                                                    </summary>
                                                                    <pre
                                                                        class="mt-2 text-xs bg-gray-950 rounded p-3 overflow-x-auto text-gray-300 border border-gray-800"
                                                                    >
                                                                        (filters::pretty_json(state))
                                                                    </pre>
                                                                </details>
                                                            </div>
                                                        }
                                                    }
                                                }
                                                None => {

                                                }
                                            }
                                            <div class="mt-3 flex justify-end">
                                                <form
                                                    method="POST"
                                                    action=(format!(
                                                        "{}/queues/{}/jobs/{}/delete",
                                                        base_path,
                                                        urlencoding::encode(&item.job_envelope.queue),
                                                        urlencoding::encode(&item.job_envelope.id),
                                                    ))
                                                    onsubmit=(format!(
                                                        "return confirmDeleteJob('{}')",
                                                        item.job_envelope.id,
                                                    ))
                                                >
                                                    <button
                                                        type="submit"
                                                        class="px-2.5 py-1 rounded bg-red-900/50 border border-red-800 hover:bg-red-800 text-red-300 text-xs transition-colors"
                                                    >
                                                        "Delete Job"
                                                    </button>
                                                </form>
                                            </div>
                                        </div>
                                    }
                                </div>
                            }
                        </section>
                    </div>
                    <script>
                        (Unescaped::new_unchecked(include_str!("../assets/busy.js")))
                    </script>
                </body>
            </html>
        }
    }
}
