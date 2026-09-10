window.addEventListener('DOMContentLoaded', function () {
    if (typeof uPlot === 'undefined') {
        return;
    }

    function formatMillis(value) {
        if (value >= 1000) {
            return (value / 1000).toFixed(2) + 's';
        }
        return value.toFixed(0) + 'ms';
    }

    var average = JSON.parse(document.getElementById('metric-detail-data-0').textContent);
    oxanaCharts.renderChart('average-chart', {
        timestamps: average[0],
        series: [{ label: 'Avg', data: average[1], color: '#c084fc', fill: 'rgba(192, 132, 252, 0.10)' }]
    }, formatMillis, function (idx) {
        return [{ label: 'Avg', value: formatMillis(average[1][idx]) }];
    });
    var executions = JSON.parse(document.getElementById('metric-detail-data-1').textContent);
    oxanaCharts.renderStackedChart('executions-chart', {
        timestamps: executions[0],
        series: [
            { label: 'Succeeded', data: executions[1], color: '#34d399' },
            { label: 'Failed', data: executions[2], color: '#ef4444' },
            { label: 'Panicked', data: executions[3], color: '#000000', border: '#6b7280' }
        ]
    }, 'Total executions by minute', function (idx, total) {
        return [
            { label: 'Succeeded', value: oxanaCharts.formatCount(executions[1][idx] || 0) },
            { label: 'Failed', value: oxanaCharts.formatCount(executions[2][idx] || 0) },
            { label: 'Panicked', value: oxanaCharts.formatCount(executions[3][idx] || 0) },
            { label: 'Total', value: oxanaCharts.formatCount(total) }
        ];
    });
});
