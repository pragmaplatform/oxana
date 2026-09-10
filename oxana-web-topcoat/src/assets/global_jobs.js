function toggleFullError(btn) {
    var container = btn.parentElement;
    var truncated = container.querySelector('.error-truncated');
    var full = container.querySelector('.error-full');
    if (full.style.display === 'none') {
        truncated.style.display = 'none';
        full.style.display = '';
        btn.textContent = 'Show less';
    } else {
        truncated.style.display = '';
        full.style.display = 'none';
        btn.textContent = 'View full error';
    }
}
function copyErrorInfo(btn) {
    var info = [
        'Job: ' + btn.dataset.jobName,
        'Queue: ' + btn.dataset.queue,
        'Created: ' + btn.dataset.created,
        'Arguments: ' + btn.dataset.args,
        'Error: ' + btn.dataset.error
    ].join('\n');
    navigator.clipboard.writeText(info).then(function() {
        btn.textContent = 'Copied!';
        setTimeout(function() { btn.textContent = 'Copy Error Info'; }, 1500);
    }, function() {
        btn.textContent = 'Copy failed';
        setTimeout(function() { btn.textContent = 'Copy Error Info'; }, 1500);
    });
}
function confirmWipeDead() {
    var input = prompt('Type WIPE to permanently remove all dead jobs.');
    return input === 'WIPE';
}
function confirmReviveAllDead() {
    return confirm('Revive all dead jobs now?');
}
function confirmRetryAllNow() {
    return confirm('Retry all pending retry jobs now?');
}
