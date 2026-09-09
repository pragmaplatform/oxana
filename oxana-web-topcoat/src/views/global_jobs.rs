use super::partials;
use crate::{filters, models::*};
use topcoat::{
    context::Cx,
    view::{Unescaped, View, view},
};

impl GlobalJobsTemplate {
    pub(crate) fn into_view(self, cx: &Cx) -> impl View + '_ {
        view! {
            cx =>
            let base_path = &self.base_path;
            let active_tab = self.active_tab;
            let kind = self.kind;
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
                        (kind.title())
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
                        <div class="flex items-center justify-between mb-4">
                            <h2 class="text-lg font-semibold">
                                if page.total > 0 {
                                    (page.range_start())
                                    "–"
                                    (page.range_end())
                                    " of "
                                    (&page.total)
                                    " "
                                    (kind.label())
                                } else {
                                    " 0 "
                                    (kind.label())
                                }
                            </h2>
                            <div class="flex items-center gap-2 text-sm">
                                if kind.is_dead() {
                                    if page.total > 0 {
                                        <form
                                            method="POST"
                                            action=(format!("{}/dead/revive_all", base_path))
                                            onsubmit="return confirmReviveAllDead()"
                                        >
                                            <button
                                                type="submit"
                                                class="px-3 py-1 rounded bg-green-900/50 border border-green-800 hover:bg-green-800 text-green-300 transition-colors"
                                            >
                                                "Revive All"
                                            </button>
                                        </form>
                                        <form
                                            method="POST"
                                            action=(format!("{}/dead/wipe", base_path))
                                            onsubmit="return confirmWipeDead()"
                                        >
                                            <button
                                                type="submit"
                                                class="px-3 py-1 rounded bg-red-900/50 border border-red-800 hover:bg-red-800 text-red-300 transition-colors"
                                            >
                                                "Wipe"
                                            </button>
                                        </form>
                                    }
                                }
                                if kind.is_retries() {
                                    if page.total > 0 {
                                        <form
                                            method="POST"
                                            action=(format!("{}/retries/retry_all_now", base_path))
                                            onsubmit="return confirmRetryAllNow()"
                                        >
                                            <button
                                                type="submit"
                                                class="px-3 py-1 rounded bg-orange-900/50 border border-orange-800 hover:bg-orange-800 text-orange-300 transition-colors"
                                            >
                                                "Retry All Now"
                                            </button>
                                        </form>
                                    }
                                }
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
                                (kind.empty_label())
                            </div>
                        } else {
                            <div class="space-y-3">
                                for job in page.jobs.iter() {
                                    <div
                                        class=(format!(
                                            "bg-gray-900 border {} rounded-lg p-4",
                                            kind.border_class(),
                                        ))
                                    >
                                        <div class="flex items-start justify-between mb-2">
                                            <div class="flex items-center gap-2">
                                                <span
                                                    class=(format!(
                                                        "inline-block w-2 h-2 rounded-full {}",
                                                        kind.dot_class(),
                                                    ))
                                                ></span>
                                                <span class="font-semibold text-sm">(&job.job.name)</span>
                                            </div>
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
                                            class="grid grid-cols-1 sm:grid-cols-4 gap-2 text-xs text-gray-400 mt-2"
                                        >
                                            <div>
                                                <span class="text-gray-500">"Queue:"</span>
                                                <a
                                                    href=(format!(
                                                        "{}/queues/{}",
                                                        base_path,
                                                        urlencoding::encode(&job.queue),
                                                    ))
                                                    class="ml-1 text-blue-400 hover:text-blue-300 hover:underline"
                                                >
                                                    (&job.queue)
                                                </a>
                                            </div>
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
                                        match &job.meta.error {
                                            Some(error) => {
                                                <details class="mt-3" open=(true)>
                                                    <summary
                                                        class="text-xs text-red-400 cursor-pointer hover:text-red-300"
                                                    >
                                                        "Error"
                                                    </summary>
                                                    if error.lines().count() > 10 {
                                                        <pre
                                                            class="mt-2 text-xs bg-gray-950 rounded p-3 overflow-x-auto text-red-300 border border-red-900/50 error-truncated"
                                                        >
                                                            (error.lines().take(10).collect::<Vec<_>>().join("\n"))
                                                        </pre>
                                                        <pre
                                                            class="mt-2 text-xs bg-gray-950 rounded p-3 overflow-x-auto text-red-300 border border-red-900/50 error-full"
                                                            style="display:none"
                                                        >
                                                            (error)
                                                        </pre>
                                                        <button
                                                            onclick="toggleFullError(this)"
                                                            class="mt-1 text-xs text-red-400 hover:text-red-300 cursor-pointer underline"
                                                        >
                                                            "View full error"
                                                        </button>
                                                    } else {
                                                        <pre
                                                            class="mt-2 text-xs bg-gray-950 rounded p-3 overflow-x-auto text-red-300 border border-red-900/50"
                                                        >
                                                            (error)
                                                        </pre>
                                                    }
                                                </details>
                                            }
                                            None => {

                                            }
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
                                        if kind.is_dead() {
                                            <div class="mt-3 flex justify-end gap-2">
                                                <button
                                                    onclick="copyErrorInfo(this)"
                                                    class="px-2.5 py-1 rounded bg-gray-800 border border-gray-700 hover:bg-gray-700 text-gray-300 text-xs transition-colors"
                                                    data-job-name=(&job.job.name)
                                                    data-queue=(&job.queue)
                                                    data-created=(filters::relative_time_micros(
                                                        &job.meta.created_at,
                                                    ))
                                                    data-args=(job.job.args.to_string())
                                                    data-error=(job.meta.error.as_deref().unwrap_or(""))
                                                >
                                                    "Copy Error Info"
                                                </button>
                                                <form
                                                    method="POST"
                                                    action=(format!("{}/enqueue", base_path))
                                                    onsubmit=(format!(
                                                        "return confirm('Revive job {}?')",
                                                        job.job.name,
                                                    ))
                                                >
                                                    <input type="hidden" name="queue" value=(&job.queue)>
                                                    <input type="hidden" name="name" value=(&job.job.name)>
                                                    <input
                                                        type="hidden"
                                                        name="args"
                                                        value=(job.job.args.to_string())
                                                    >
                                                    match &job.meta.state {
                                                        Some(state) => {
                                                            <input
                                                                type="hidden"
                                                                name="state"
                                                                value=(state.to_string())
                                                            >
                                                        }
                                                        None => {

                                                        }
                                                    }
                                                    <input type="hidden" name="redirect" value="/dead">
                                                    <button
                                                        type="submit"
                                                        class="px-2.5 py-1 rounded bg-green-900/50 border border-green-800 hover:bg-green-800 text-green-300 text-xs transition-colors"
                                                    >
                                                        "Revive"
                                                    </button>
                                                </form>
                                            </div>
                                        }
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
                            include_str!("../assets/global_jobs.js"),
                        ))
                    </script>
                </body>
            </html>
        }
    }
}
