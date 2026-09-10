window.addEventListener('DOMContentLoaded', function () {
    if (typeof uPlot === 'undefined') {
        return;
    }

    function formatSeconds(value) {
        if (value >= 60) {
            return (value / 60).toFixed(1) + 'm';
        }
        return value.toFixed(1) + 's';
    }

    var execution = JSON.parse(document.getElementById('metrics-data-0').textContent);
    oxanaCharts.renderChart('execution-chart', execution, formatSeconds, function (idx) {
        return oxanaCharts.seriesTooltipRows(execution, idx, formatSeconds);
    });
    var processed = JSON.parse(document.getElementById('metrics-data-1').textContent);
    oxanaCharts.renderStackedChart('processed-chart', processed, 'Total processed by worker', function (idx, total) {
        var rows = oxanaCharts.seriesTooltipRows(processed, idx, oxanaCharts.formatCount);
        if (total !== 0) {
            rows.push({ label: 'Total', value: oxanaCharts.formatCount(total) });
        }
        return rows;
    });
});
