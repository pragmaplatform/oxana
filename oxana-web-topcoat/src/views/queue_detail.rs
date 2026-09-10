use super::partials;
use crate::{filters, models::*};
use topcoat::{
    context::Cx,
    view::{Unescaped, View, view},
};

impl QueueDetailTemplate {
    pub(crate) fn into_view(self, cx: &Cx) -> impl View + '_ {
        view! {
            cx =>
            let base_path = &self.base_path;
            let active_tab = self.active_tab;
            let queue_key = &self.queue_key;
            let queue_stats = &self.queue_stats;
            let active_jobs = &self.active_jobs;
            let busy = self.busy;
            let page = &self.page;
            <!DOCTYPE html>
            <html lang="en">
                <head>
                    <meta charset="UTF-8">
                    <meta
                        name="viewport"
                        content="width=device-width, initial-scale=1.0"
                    >
                    <title>
                        "Queue: "
                        (queue_key)
                        " - Oxana"
                    </title>
                    <script src="https://cdn.tailwindcss.com"></script>
                </head>
                <body class="bg-gray-950 text-gray-100 min-h-screen">
                    <div class="max-w-[88rem] mx-auto px-4 sm:px-6 lg:px-8 py-8">
                        <div class="flex items-center justify-between mb-6">
                            <h1 class="text-2xl font-bold tracking-tight">
                                "Oxana Dashboard"
                            </h1>
                        </div>
                        partials::tabs(base_path: base_path, active_tab: active_tab)
                        <div
                            class="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 mb-6"
                        >
                            <div class="flex flex-wrap items-center gap-3">
                                <h2 class="text-lg font-semibold font-mono">
                                    (queue_key)
                                </h2>
                                <span
                                    class=(format!(
                                        "px-2.5 py-1 rounded border text-xs font-medium {}",
                                        self.state_class(),
                                    ))
                                >
                                    (self.state_label())
                                </span>
                                <span
                                    class="px-2.5 py-1 rounded border border-gray-800 bg-gray-900 text-gray-300 text-xs"
                                >
                                    " Concurrency: "
                                    (self.concurrency_label())
                                    if self.has_concurrency_override() {
                                        <span class="text-gray-500">
                                            "("
                                            <span class="line-through">
                                                (self.concurrency_default_label())
                                            </span>
                                            ")"
                                        </span>
                                    }
                                </span>
                            </div>
                            <div class="flex flex-wrap items-center gap-2">
                                if self.can_change_concurrency() {
                                    <form
                                        method="POST"
                                        action=(self.queue_action_path("concurrency"))
                                        onsubmit="return promptConcurrency(this)"
                                        data-queue-key=(queue_key)
                                        data-current-concurrency=(self.concurrency_label())
                                        data-default-concurrency=(self.concurrency_default_label())
                                    >
                                        <input type="hidden" name="concurrency" value="">
                                        <button
                                            type="submit"
                                            class="px-3 py-1.5 rounded bg-gray-800 border border-gray-700 hover:bg-gray-700 text-gray-300 text-sm transition-colors"
                                        >
                                            "Change concurrency"
                                        </button>
                                    </form>
                                }
                                if self.is_paused() {
                                    <form
                                        method="POST"
                                        action=(format!(
                                            "{}/queues/{}/unpause",
                                            base_path,
                                            urlencoding::encode(queue_key),
                                        ))
                                    >
                                        <button
                                            type="submit"
                                            class="px-3 py-1.5 rounded bg-green-900/50 border border-green-800 hover:bg-green-800 text-green-300 text-sm transition-colors"
                                        >
                                            "Unpause"
                                        </button>
                                    </form>
                                } else {
                                    <form
                                        method="POST"
                                        action=(format!(
                                            "{}/queues/{}/pause",
                                            base_path,
                                            urlencoding::encode(queue_key),
                                        ))
                                    >
                                        <button
                                            type="submit"
                                            class="px-3 py-1.5 rounded bg-yellow-900/50 border border-yellow-800 hover:bg-yellow-800 text-yellow-300 text-sm transition-colors"
                                        >
                                            "Pause"
                                        </button>
                                    </form>
                                }
                                <form
                                    method="POST"
                                    action=(format!(
                                        "{}/queues/{}/wipe",
                                        base_path,
                                        urlencoding::encode(queue_key),
                                    ))
                                    onsubmit=(format!("return confirmWipe('{}')", queue_key))
                                >
                                    <button
                                        type="submit"
                                        class="px-3 py-1.5 rounded bg-red-900/50 border border-red-800 hover:bg-red-800 text-red-300 text-sm transition-colors"
                                    >
                                        "Wipe"
                                    </button>
                                </form>
                            </div>
                        </div>
                        match &queue_stats {
                            Some(qs) => {
                                <div
                                    class="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-6 gap-4 mb-8"
                                >
                                    <div
                                        class="bg-gray-900 rounded-lg p-4 border border-gray-800"
                                    >
                                        <div
                                            class="text-xs text-gray-400 uppercase tracking-wide mb-1"
                                        >
                                            "Enqueued"
                                        </div>
                                        <div class="text-2xl font-semibold text-blue-400">
                                            (filters::format_number(&qs.enqueued))
                                        </div>
                                    </div>
                                    <div
                                        class="bg-gray-900 rounded-lg p-4 border border-gray-800"
                                    >
                                        <div
                                            class="text-xs text-gray-400 uppercase tracking-wide mb-1"
                                        >
                                            "Busy"
                                        </div>
                                        <div class="text-2xl font-semibold text-purple-400">
                                            (filters::format_number(&busy))
                                        </div>
                                    </div>
                                    <div
                                        class="bg-gray-900 rounded-lg p-4 border border-gray-800"
                                    >
                                        <div
                                            class="text-xs text-gray-400 uppercase tracking-wide mb-1"
                                        >
                                            "Processed"
                                        </div>
                                        <div class="text-2xl font-semibold text-green-400">
                                            (filters::format_number_i64(&qs.processed))
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
                                            (filters::format_number_i64(&qs.succeeded))
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
                                            (filters::format_number_i64(&qs.failed))
                                        </div>
                                    </div>
                                    <div
                                        class="bg-gray-900 rounded-lg p-4 border border-gray-800"
                                    >
                                        <div
                                            class="text-xs text-gray-400 uppercase tracking-wide mb-1"
                                        >
                                            "Panicked"
                                        </div>
                                        <div class="text-2xl font-semibold text-orange-400">
                                            (filters::format_number_i64(&qs.panicked))
                                        </div>
                                    </div>
                                    <div
                                        class="bg-gray-900 rounded-lg p-4 border border-gray-800"
                                    >
                                        <div
                                            class="text-xs text-gray-400 uppercase tracking-wide mb-1"
                                        >
                                            "Latency"
                                        </div>
                                        <div class="text-2xl font-semibold text-yellow-400">
                                            (filters::format_latency(&qs.latency_s))
                                        </div>
                                    </div>
                                    <div
                                        class="bg-gray-900 rounded-lg p-4 border border-gray-800"
                                    >
                                        <div
                                            class="text-xs text-gray-400 uppercase tracking-wide mb-1"
                                        >
                                            "Processed/min"
                                        </div>
                                        <div class="text-2xl font-semibold text-cyan-400">
                                            (filters::format_rate_per_minute(
                                                &qs.rate.processed_per_minute,
                                            ))
                                        </div>
                                    </div>
                                    <div
                                        class="bg-gray-900 rounded-lg p-4 border border-gray-800"
                                    >
                                        <div
                                            class="text-xs text-gray-400 uppercase tracking-wide mb-1"
                                        >
                                            "Succeeded/min"
                                        </div>
                                        <div class="text-2xl font-semibold text-emerald-400">
                                            (filters::format_rate_per_minute(
                                                &qs.rate.succeeded_per_minute,
                                            ))
                                        </div>
                                    </div>
                                    <div
                                        class="bg-gray-900 rounded-lg p-4 border border-gray-800"
                                    >
                                        <div
                                            class="text-xs text-gray-400 uppercase tracking-wide mb-1"
                                        >
                                            "Failed/min"
                                        </div>
                                        <div class="text-2xl font-semibold text-red-400">
                                            (filters::format_rate_per_minute(&qs.rate.failed_per_minute))
                                        </div>
                                    </div>
                                    <div
                                        class="bg-gray-900 rounded-lg p-4 border border-gray-800"
                                    >
                                        <div
                                            class="text-xs text-gray-400 uppercase tracking-wide mb-1"
                                        >
                                            "Growth/min"
                                        </div>
                                        <div class="text-2xl font-semibold text-orange-400">
                                            (filters::format_signed_rate_per_minute(
                                                &qs.rate.growth_per_minute,
                                            ))
                                        </div>
                                    </div>
                                    <div
                                        class="bg-gray-900 rounded-lg p-4 border border-gray-800"
                                    >
                                        <div
                                            class="text-xs text-gray-400 uppercase tracking-wide mb-1"
                                        >
                                            "ETA"
                                        </div>
                                        <div class="text-2xl font-semibold text-yellow-400">
                                            (filters::format_eta_s(&qs.rate.eta_s))
                                        </div>
                                    </div>
                                </div>
                            }
                            None => {

                            }
                        }
                        if !active_jobs.is_empty() {
                            <section class="mb-8">
                                <h2 class="text-lg font-semibold mb-4">
                                    "Currently Processing ("
                                    (active_jobs.len())
                                    ")"
                                </h2>
                                <div class="space-y-3">
                                    for item in active_jobs.iter() {
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
                                                class="grid grid-cols-1 sm:grid-cols-4 gap-2 text-xs text-gray-400 mt-2"
                                            >
                                                <div>
                                                    <span class="text-gray-500">"Process:"</span>
                                                    <span class="ml-1 text-gray-300 font-mono">
                                                        (&item.process_id)
                                                    </span>
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
                                                <details class="mt-3">
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
                                                            <details class="mt-3">
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
                                                        urlencoding::encode(queue_key),
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
                            </section>
                        }
                        <div class="flex items-center justify-between mb-4">
                            <div>
                                <h2 class="text-lg font-semibold">"Enqueued"</h2>
                                <div class="mt-1 text-sm text-gray-400">
                                    if page.total > 0 {
                                        (page.range_start())
                                        "–"
                                        (page.range_end())
                                        " of "
                                        (&page.total)
                                        " jobs "
                                    } else {
                                        " 0 jobs "
                                    }
                                </div>
                            </div>
                            <div class="flex items-center gap-2 text-sm">
                                if page.number > 1 {
                                    <a
                                        href=(format!("?page={}", page.number - 1))
                                        class="px-3 py-1 rounded bg-gray-800 border border-gray-700 hover:bg-gray-700 text-gray-300"
                                    >
                                        "← Prev"
                                    </a>
                                } else {
                                    <span
                                        class="px-3 py-1 rounded bg-gray-900 border border-gray-800 text-gray-600 cursor-not-allowed"
                                    >
                                        "← Prev"
                                    </span>
                                }
                                if page.has_next {
                                    <a
                                        href=(format!("?page={}", page.number + 1))
                                        class="px-3 py-1 rounded bg-gray-800 border border-gray-700 hover:bg-gray-700 text-gray-300"
                                    >
                                        "Next →"
                                    </a>
                                } else {
                                    <span
                                        class="px-3 py-1 rounded bg-gray-900 border border-gray-800 text-gray-600 cursor-not-allowed"
                                    >
                                        "Next →"
                                    </span>
                                }
                            </div>
                        </div>
                        if page.jobs.is_empty() {
                            <div
                                class="bg-gray-900 border border-gray-800 rounded-lg p-6 text-center text-gray-500"
                            >
                                " No jobs in this queue "
                            </div>
                        } else {
                            <div class="space-y-3">
                                for job in page.jobs.iter() {
                                    <div
                                        class="bg-gray-900 border border-gray-800 rounded-lg p-4"
                                    >
                                        <div class="flex items-start justify-between mb-2">
                                            <span class="font-semibold text-sm">(&job.job.name)</span>
                                            <a
                                                href=(format!(
                                                    "{}/jobs/{}",
                                                    base_path,
                                                    urlencoding::encode(&job.id),
                                                ))
                                                class="text-xs text-gray-500 hover:text-blue-300 hover:underline font-mono"
                                            >
                                                (&job.id)
                                            </a>
                                        </div>
                                        <div
                                            class="grid grid-cols-1 sm:grid-cols-3 gap-2 text-xs text-gray-400 mt-2"
                                        >
                                            <div>
                                                <span class="text-gray-500">"Retries:"</span>
                                                <span class="ml-1 text-gray-300">(&job.meta.retries)</span>
                                            </div>
                                            <div>
                                                <span class="text-gray-500">"Created:"</span>
                                                <span class="ml-1 text-gray-300">
                                                    (filters::relative_time_micros(&job.meta.created_at))
                                                </span>
                                            </div>
                                            <div>
                                                <span class="text-gray-500">"Scheduled:"</span>
                                                <span class="ml-1 text-gray-300">
                                                    (filters::relative_time_micros(&job.meta.scheduled_at))
                                                </span>
                                            </div>
                                        </div>
                                        let arg_pills = filters::simple_args(&job.job.args);
                                        partials::argument_pills(arg_pills: &arg_pills)
                                        if filters::show_args_json(&job.job.args) {
                                            <details class="mt-3" open=(true)>
                                                <summary
                                                    class="text-xs text-gray-500 cursor-pointer hover:text-gray-300"
                                                >
                                                    "Arguments"
                                                </summary>
                                                <pre
                                                    class="mt-2 text-xs bg-gray-950 rounded p-3 overflow-x-auto text-gray-300 border border-gray-800"
                                                >
                                                    (filters::pretty_json(&job.job.args))
                                                </pre>
                                            </details>
                                        }
                                        match &job.meta.state {
                                            Some(state) => {
                                                if filters::has_args(state) {
                                                    let progress_started_at = job.meta.started_at;
                                                    if let Some(progress) = filters::job_progress(
                                                        state,
                                                        &progress_started_at,
                                                    ) {
                                                        partials::job_progress(progress: &progress)
                                                    } else {
                                                        <details class="mt-3">
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
                                                    urlencoding::encode(queue_key),
                                                    urlencoding::encode(&job.id),
                                                ))
                                                onsubmit=(format!("return confirmDeleteJob('{}')", job.id))
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
                            <div class="flex items-center justify-between mt-6 text-sm">
                                <span class="text-gray-500">
                                    (page.range_start())
                                    "–"
                                    (page.range_end())
                                    " of "
                                    (&page.total)
                                </span>
                                <div class="flex items-center gap-2">
                                    if page.number > 1 {
                                        <a
                                            href=(format!("?page={}", page.number - 1))
                                            class="px-3 py-1 rounded bg-gray-800 border border-gray-700 hover:bg-gray-700 text-gray-300"
                                        >
                                            "← Prev"
                                        </a>
                                    }
                                    if page.has_next {
                                        <a
                                            href=(format!("?page={}", page.number + 1))
                                            class="px-3 py-1 rounded bg-gray-800 border border-gray-700 hover:bg-gray-700 text-gray-300"
                                        >
                                            "Next →"
                                        </a>
                                    }
                                </div>
                            </div>
                        }
                    </div>
                    <script>
                        (Unescaped::new_unchecked(
                            include_str!("../assets/queue_detail.js"),
                        ))
                    </script>
                </body>
            </html>
        }
    }
}
