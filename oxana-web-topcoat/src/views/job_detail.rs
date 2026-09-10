use super::partials;
use crate::{filters, models::*};
use topcoat::{
    context::Cx,
    view::{View, view},
};

impl JobDetailTemplate {
    pub(crate) fn into_view(self, cx: &Cx) -> impl View + '_ {
        view! {
            cx =>
            let base_path = &self.base_path;
            let active_tab = self.active_tab;
            let job_id = &self.job_id;
            let job = &self.job;
            let is_dead = self.is_dead;
            <!DOCTYPE html>
            <html lang="en">
                <head>
                    <meta charset="UTF-8">
                    <meta
                        name="viewport"
                        content="width=device-width, initial-scale=1.0"
                    >
                    <title>
                        "Job: "
                        (job_id)
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
                        match &job {
                            Some(job) => {
                                <div class="flex items-center justify-between mb-4">
                                    <div>
                                        <h2 class="text-lg font-semibold">
                                            <a
                                                href=(format!(
                                                    "{}/metrics/job?worker={}",
                                                    base_path,
                                                    urlencoding::encode(&job.job.name),
                                                ))
                                                class="text-blue-400 hover:text-blue-300 hover:underline"
                                            >
                                                (&job.job.name)
                                            </a>
                                        </h2>
                                        <div
                                            class="mt-1 text-xs text-gray-500 font-mono break-all"
                                        >
                                            (&job.id)
                                        </div>
                                    </div>
                                    if is_dead {
                                        <span
                                            class="px-2 py-1 rounded bg-red-900/50 border border-red-800 text-red-300 text-xs"
                                        >
                                            "Dead"
                                        </span>
                                    }
                                </div>
                                <div
                                    class="bg-gray-900 border border-gray-800 rounded-lg p-4"
                                >
                                    <div
                                        class="grid grid-cols-1 sm:grid-cols-4 gap-2 text-xs text-gray-400"
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
                                        <div>
                                            <span class="text-gray-500">"Started:"</span>
                                            <span class="ml-1 text-gray-300">
                                                (filters::relative_time_micros_opt(&job.meta.started_at))
                                            </span>
                                        </div>
                                        <div>
                                            <span class="text-gray-500">"Unique:"</span>
                                            <span class="ml-1 text-gray-300">(&job.meta.unique)</span>
                                        </div>
                                        <div>
                                            <span class="text-gray-500">"Resurrect:"</span>
                                            <span class="ml-1 text-gray-300">
                                                (&job.meta.resurrect)
                                            </span>
                                        </div>
                                        <div>
                                            <span class="text-gray-500">"Throttle cost:"</span>
                                            <span class="ml-1 text-gray-300">
                                                match &job.meta.throttle_cost {
                                                    Some(cost) => {
                                                        (cost)
                                                    }
                                                    None => {
                                                        "— "
                                                    }
                                                }
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
                                            let progress_started_at = job.meta.started_at;
                                            if let Some(progress) = filters::job_progress(
                                                state,
                                                &progress_started_at,
                                            ) {
                                                partials::job_progress(progress: &progress)
                                            } else {
                                                <details class="mt-3" open=(true)>
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
                                        None => {

                                        }
                                    }
                                    match &job.meta.error {
                                        Some(error) => {
                                            <details class="mt-3" open=(true)>
                                                <summary
                                                    class="text-xs text-red-400 cursor-pointer hover:text-red-300"
                                                >
                                                    "Error"
                                                </summary>
                                                <pre
                                                    class="mt-2 text-xs bg-gray-950 rounded p-3 overflow-x-auto text-red-300 border border-red-900/50"
                                                >
                                                    (error)
                                                </pre>
                                            </details>
                                        }
                                        None => {

                                        }
                                    }
                                </div>
                            }
                            None => {
                                <div
                                    class="bg-gray-900 border border-gray-800 rounded-lg p-6 text-center text-gray-500"
                                >
                                    " No job found for "
                                    <span class="font-mono">(job_id)</span>
                                </div>
                            }
                        }
                    </div>
                </body>
            </html>
        }
    }
}
