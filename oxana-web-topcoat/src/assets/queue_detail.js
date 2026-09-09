function promptConcurrency(form) {
    var current = form.dataset.currentConcurrency || '';
    var defaultConcurrency = form.dataset.defaultConcurrency || '';
    var queueKey = form.dataset.queueKey || 'queue';
    var promptText = 'Set concurrency for ' + queueKey;

    if (defaultConcurrency) {
        promptText += ' (default ' + defaultConcurrency + ')';
    }

    var input = prompt(promptText, current);
    if (input === null) {
        return false;
    }

    input = input.trim();
    if (!/^[1-9][0-9]*$/.test(input)) {
        alert('Concurrency must be a positive integer.');
        return false;
    }

    form.elements.concurrency.value = input;
    return true;
}

function confirmWipe(queueKey) {
    var input = prompt('Type the queue name to confirm wipe: ' + queueKey);
    return input === queueKey;
}
function confirmDeleteJob(jobId) {
    return confirm('Are you sure you want to delete job ' + jobId + '?');
}
