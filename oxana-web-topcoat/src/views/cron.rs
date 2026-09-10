use super::partials;
use crate::{filters, models::*};
use topcoat::{
    context::Cx,
    view::{Unescaped, View, view},
};

impl CronTemplate {
    pub(crate) fn into_view(self, cx: &Cx) -> impl View + '_ {
        view! {
            cx =>
            let base_path = &self.base_path;
            let active_tab = self.active_tab;
            let rows = &self.rows;
            let total = self.total;
            let enqueued = self.enqueued;
            <!DOCTYPE html>
            <html lang="en">
                <head>
                    <meta charset="UTF-8">
                    <meta
                        name="viewport"
                        content="width=device-width, initial-scale=1.0"
                    >
                    <title>"Cron - Oxana"</title>
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
                        if enqueued {
                            <div
                                data-auto-dismiss-notice=(true)
                                class="mb-4 rounded border border-green-800 bg-green-950/40 px-4 py-3 text-sm text-green-300"
                            >
                                " Cron job enqueued. "
                            </div>
                        }
                        <h2 class="text-lg font-semibold mb-4">
                            "Cron Workers ("
                            (total)
                            ")"
                        </h2>
                        if rows.is_empty() {
                            <div
                                class="bg-gray-900 border border-gray-800 rounded-lg p-6 text-center text-gray-500"
                            >
                                " No cron workers registered "
                            </div>
                        } else {
                            <div class="overflow-x-auto">
                                <table class="w-full text-sm">
                                    <thead>
                                        <tr
                                            class="border-b border-gray-800 text-left text-xs text-gray-400 uppercase tracking-wide"
                                        >
                                            <th class="pb-3 pr-4">"Worker"</th>
                                            <th class="pb-3 pr-4">"Queue"</th>
                                            <th class="pb-3 pr-4">"Schedule"</th>
                                            <th class="pb-3">"Next Run"</th>
                                            <th class="pb-3 text-right">"Action"</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        for row in rows {
                                            match &row {
                                                CronRow::Group { name, depth: _ } => {
                                                    <tr class="border-b border-gray-800/50">
                                                        <td
                                                            colspan="5"
                                                            class="py-2 pt-4 font-mono text-xs text-gray-500 font-semibold"
                                                            style=(format!("padding-left: {}px", row.indent_px()))
                                                        >
                                                            (name)
                                                        </td>
                                                    </tr>
                                                }
                                                CronRow::Worker { view: cw, depth: _ } => {
                                                    <tr
                                                        class="border-b border-gray-800/50 hover:bg-gray-900/50"
                                                    >
                                                        <td
                                                            class="py-3 pr-4 font-mono text-sm"
                                                            style=(format!("padding-left: {}px", row.indent_px()))
                                                        >
                                                            <span class="text-gray-600">"↳ "</span>
                                                            (&cw.short_name)
                                                        </td>
                                                        <td class="py-3 pr-4 font-mono text-sm text-gray-400">
                                                            (&cw.queue_key)
                                                        </td>
                                                        <td class="py-3 pr-4 font-mono text-sm text-yellow-400">
                                                            (&cw.schedule)
                                                        </td>
                                                        <td class="py-3 text-gray-300">
                                                            (filters::relative_time_micros_opt(&cw.next_run_micros))
                                                        </td>
                                                        <td class="py-3 text-right">
                                                            <form
                                                                method="POST"
                                                                action=(format!("{}/cron/enqueue", base_path))
                                                                class="inline"
                                                            >
                                                                <input type="hidden" name="name" value=(&cw.name)>
                                                                <button
                                                                    type="submit"
                                                                    class="whitespace-nowrap rounded bg-green-900/50 border border-green-800 px-2.5 py-1 text-xs text-green-300 hover:bg-green-800 transition-colors"
                                                                >
                                                                    "Enqueue now"
                                                                </button>
                                                            </form>
                                                        </td>
                                                    </tr>
                                                }
                                            }
                                        }
                                    </tbody>
                                </table>
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
