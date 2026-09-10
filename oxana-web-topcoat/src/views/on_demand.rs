use super::partials;
use crate::models::*;
use topcoat::{
    context::Cx,
    view::{Unescaped, View, view},
};

impl OnDemandTemplate {
    pub(crate) fn into_view(self, cx: &Cx) -> impl View + '_ {
        view! {
            cx =>
            let base_path = &self.base_path;
            let active_tab = self.active_tab;
            let rows = &self.rows;
            let queues = &self.queues;
            let total = self.total;
            let scheduled = self.scheduled;
            let invalid_json = self.invalid_json;
            <!DOCTYPE html>
            <html lang="en">
                <head>
                    <meta charset="UTF-8">
                    <meta
                        name="viewport"
                        content="width=device-width, initial-scale=1.0"
                    >
                    <title>"On-Demand - Oxana"</title>
                    <script src="https://cdn.tailwindcss.com"></script>
                    <style>
                        (Unescaped::new_unchecked(
                            include_str!("../assets/on_demand.css"),
                        ))
                    </style>
                </head>
                <body class="bg-gray-950 text-gray-100 min-h-screen">
                    <div class="max-w-[88rem] mx-auto px-4 sm:px-6 lg:px-8 py-8">
                        <div class="mb-6">
                            <h1 class="text-2xl font-bold tracking-tight">
                                "Oxana Dashboard"
                            </h1>
                        </div>
                        partials::tabs(base_path: base_path, active_tab: active_tab)
                        if scheduled {
                            <div
                                data-auto-dismiss-notice=(true)
                                class="mb-4 rounded border border-green-800 bg-green-950/40 px-4 py-3 text-sm text-green-300"
                            >
                                " Job scheduled. "
                            </div>
                        }
                        if invalid_json {
                            <div
                                data-auto-dismiss-notice=(true)
                                class="mb-4 rounded border border-red-800 bg-red-950/40 px-4 py-3 text-sm text-red-300"
                            >
                                " Invalid JSON. "
                            </div>
                        }
                        <h2 class="text-lg font-semibold mb-4">
                            "On-Demand Jobs ("
                            (total)
                            ")"
                        </h2>
                        if rows.is_empty() {
                            <div
                                class="bg-gray-900 border border-gray-800 rounded-lg p-6 text-center text-gray-500"
                            >
                                " No on-demand jobs registered "
                            </div>
                        } else if queues.is_empty() {
                            <div
                                class="bg-gray-900 border border-gray-800 rounded-lg p-6 text-center text-gray-500"
                            >
                                " No static queues registered "
                            </div>
                        } else {
                            <div class="w-full text-sm">
                                <div
                                    class="grid grid-cols-[minmax(16rem,1fr)_auto] gap-4 border-b border-gray-800 pb-3 text-left text-xs text-gray-400 uppercase tracking-wide"
                                >
                                    <div>"Worker"</div>
                                    <div>"Action"</div>
                                </div>
                                <div>
                                    for (row_index, row) in rows.iter().enumerate() {
                                        match &row {
                                            OnDemandRow::Group { name, depth: _ } => {
                                                <div class="border-b border-gray-800/50">
                                                    <div
                                                        class="py-2 pt-4 font-mono text-xs text-gray-500 font-semibold"
                                                        style=(format!("padding-left: {}px", row.indent_px()))
                                                    >
                                                        (name)
                                                    </div>
                                                </div>
                                            }
                                            OnDemandRow::Job { view: job, depth: _ } => {
                                                <details class="border-b border-gray-800/50">
                                                    <summary
                                                        class="grid cursor-pointer list-none grid-cols-[minmax(16rem,1fr)_auto] gap-4 py-3 hover:bg-gray-900/50"
                                                    >
                                                        <span
                                                            class="font-mono text-sm"
                                                            style=(format!("padding-left: {}px", row.indent_px()))
                                                        >
                                                            <span class="text-gray-600">"↳ "</span>
                                                            (&job.short_name)
                                                        </span>
                                                        <span
                                                            class="inline-flex w-fit select-none rounded bg-green-900/50 border border-green-800 px-2.5 py-1 text-xs text-green-300 hover:bg-green-800 transition-colors"
                                                        >
                                                            "Enqueue"
                                                        </span>
                                                    </summary>
                                                    <form
                                                        method="POST"
                                                        action=(format!("{}/on-demand/enqueue", base_path))
                                                        class="w-full pb-6 pt-3"
                                                    >
                                                        <input type="hidden" name="name" value=(&job.name)>
                                                        <label
                                                            class="block text-xs text-gray-500 mb-1"
                                                            for=(format!("queue-{}", (row_index + 1)))
                                                        >
                                                            "Queue"
                                                        </label>
                                                        <select
                                                            id=(format!("queue-{}", (row_index + 1)))
                                                            name="queue"
                                                            class="w-full sm:w-72 mb-3 rounded bg-gray-950 border border-gray-800 px-3 py-2 text-sm text-gray-200"
                                                        >
                                                            for queue in queues.iter() {
                                                                <option
                                                                    value=(&queue.key)
                                                                    if queue.selected {
                                                                        selected=(true)
                                                                    }
                                                                >
                                                                    (&queue.key)
                                                                </option>
                                                            }
                                                        </select>
                                                        <label
                                                            class="block text-xs text-gray-500 mb-1"
                                                            for=(format!("args-{}", (row_index + 1)))
                                                        >
                                                            "Arguments"
                                                        </label>
                                                        <textarea
                                                            id=(format!("args-{}", (row_index + 1)))
                                                            name="args"
                                                            rows="10"
                                                            spellcheck="false"
                                                            class="w-full rounded bg-gray-950 border border-gray-800 px-3 py-2 font-mono text-xs text-gray-200"
                                                        >
                                                            (&job.args_template_json)
                                                        </textarea>
                                                        <div class="mt-3 flex justify-end">
                                                            <button
                                                                type="submit"
                                                                class="rounded bg-blue-900/50 border border-blue-800 px-3 py-1.5 text-xs text-blue-300 hover:bg-blue-800 transition-colors"
                                                            >
                                                                "Enqueue Job"
                                                            </button>
                                                        </div>
                                                    </form>
                                                </details>
                                            }
                                        }
                                    }
                                </div>
                            </div>
                        }
                    </div>
                    <script>
                        (Unescaped::new_unchecked(include_str!("../assets/notices.js")))
                    </script>
                </body>
            </html>
        }
    }
}
