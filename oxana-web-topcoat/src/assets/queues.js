window.addEventListener('DOMContentLoaded', function () {
    if (typeof uPlot === 'undefined') {
        return;
    }

    var payload = JSON.parse(document.getElementById('queues-data-0').textContent);
    oxanaCharts.renderChart('queue-length-chart', payload, oxanaCharts.formatCount, function (idx) {
        return oxanaCharts.seriesTooltipRows(payload, idx, oxanaCharts.formatCount);
    });
});
