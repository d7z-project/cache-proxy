// ── Status modal data loading ──

function cancelStatusRequests() {
    ['summary', 'disk', 'events'].forEach(function(key) {
        if (statusState.controllers[key]) {
            statusState.controllers[key].abort();
            statusState.controllers[key] = null;
        }
    });
}

function fetchStatus(name, path, onSuccess, onFailure) {
    if (statusState.cache[name]) {
        if (onSuccess) onSuccess(statusState.cache[name]);
        return;
    }
    if (statusState.controllers[name]) {
        return;
    }
    var controller = new AbortController();
    statusState.controllers[name] = controller;
    fetch(path, { signal: controller.signal }).then(function(resp) {
        if (!resp.ok) {
            throw new Error(resp.statusText || String(resp.status));
        }
        return resp.json();
    }).then(function(data) {
        statusState.controllers[name] = null;
        statusState.cache[name] = data;
        if (onSuccess) onSuccess(data);
    }).catch(function(err) {
        statusState.controllers[name] = null;
        if (err && err.name === 'AbortError') {
            return;
        }
        if (onFailure) onFailure(err);
    });
}

// ── Formatting helpers ──

function formatRelative(seconds) {
    var t = window.I18N;
    if (seconds < 5) return t.just_now || 'just now';
    if (seconds < 60) return (t.s_ago || '%ds ago').replace('%d', String(seconds));
    var minutes = Math.round(seconds / 60);
    if (seconds < 3600) return (t.m_ago || '%dm ago').replace('%d', String(minutes));
    var hours = Math.round(seconds / 3600);
    return (t.h_ago || '%dh ago').replace('%d', String(hours));
}

function formatDateTime(value) {
    if (!value) return '';
    var date = new Date(value);
    if (isNaN(date.getTime())) return value;
    var pad = function(n) { return String(n).padStart(2, '0'); };
    return date.getFullYear() + '/' +
        pad(date.getMonth() + 1) + '/' +
        pad(date.getDate()) + ' ' +
        pad(date.getHours()) + ':' +
        pad(date.getMinutes()) + ':' +
        pad(date.getSeconds());
}

function formatDisplayTime(value) {
    if (!value) return { display: '', exact: '' };
    var date = new Date(value);
    if (isNaN(date.getTime())) {
        return { display: value, exact: value };
    }
    var exact = formatDateTime(value);
    var ageSeconds = Math.round((Date.now() - date.getTime()) / 1000);
    if (ageSeconds >= 0 && ageSeconds < 86400) {
        return { display: formatRelative(ageSeconds), exact: exact };
    }
    return { display: exact, exact: exact };
}

function formatDurationMS(value) {
    if (value < 1000) return value + 'ms';
    if (value < 60000) return (value / 1000).toFixed(1).replace(/\.0$/, '') + 's';
    return (value / 60000).toFixed(1).replace(/\.0$/, '') + 'm';
}

function formatShortDuration(seconds) {
    var t = window.I18N;
    if (seconds % 3600 === 0) {
        return String(seconds / 3600) + (t.duration_hours_short || 'h');
    }
    if (seconds % 60 === 0) {
        return String(seconds / 60) + (t.duration_minutes_short || 'm');
    }
    return String(seconds) + (t.duration_seconds_short || 's');
}

function sampleIntervalSeconds(samples) {
    if (!samples || samples.length < 2) {
        return 0;
    }
    var first = new Date(samples[0].at).getTime();
    var second = new Date(samples[1].at).getTime();
    if (isNaN(first) || isNaN(second) || second <= first) {
        return 0;
    }
    return Math.round((second - first) / 1000);
}

function sampleWindowSeconds(samples, intervalSeconds) {
    if (!samples || !samples.length) {
        return 0;
    }
    if (samples.length === 1) {
        return intervalSeconds;
    }
    var first = new Date(samples[0].at).getTime();
    var last = new Date(samples[samples.length - 1].at).getTime();
    if (isNaN(first) || isNaN(last) || last < first) {
        return intervalSeconds;
    }
    return Math.round((last - first) / 1000) + intervalSeconds;
}

function diskWindowNote(samples) {
    var t = window.I18N;
    var summary = statusState.cache.summary;
    var windowSeconds = summary && summary.disk_history_window_seconds ? summary.disk_history_window_seconds : 0;
    var intervalSeconds = summary && summary.disk_sample_interval_seconds ? summary.disk_sample_interval_seconds : 0;
    if ((!windowSeconds || !intervalSeconds) && samples && samples.length) {
        intervalSeconds = intervalSeconds || sampleIntervalSeconds(samples);
        windowSeconds = windowSeconds || sampleWindowSeconds(samples, intervalSeconds);
    }
    if (!windowSeconds || !intervalSeconds) {
        return t.server_status;
    }
    var windowText = formatShortDuration(windowSeconds);
    var intervalText = formatShortDuration(intervalSeconds);
    return (t.disk_window_note || 'Last %s, sampled every %s')
        .replace('%s', windowText)
        .replace('%s', intervalText);
}

function eventRequestLimit() {
    var summary = statusState.cache.summary;
    if (summary && summary.event_limit) {
        return summary.event_limit;
    }
    return 500;
}

function formatBytes(value) {
    var units = ['B', 'KB', 'MB', 'GB', 'TB'];
    var n = value;
    var idx = 0;
    while (n >= 1024 && idx < units.length - 1) {
        n /= 1024;
        idx += 1;
    }
    var digits = n >= 100 || idx === 0 ? 0 : 1;
    return n.toFixed(digits).replace(/\.0$/, '') + ' ' + units[idx];
}

function spinnerHTML() {
    return '<span class="loading-spinner"></span>';
}

function summarySkeletonHTML() {
    return '<div class="status-summary-skeleton">' +
        '<div class="skeleton-card"></div>' +
        '<div class="skeleton-card"></div>' +
        '</div>';
}

function translateResult(result) {
    var t = window.I18N;
    var key = 'result_' + result.toLowerCase();
    return t[key] || result;
}

function translateTaskType(taskType) {
    var t = window.I18N;
    var key = 'task_' + String(taskType || '').toLowerCase();
    return t[key] || taskType;
}

function resultClass(result) {
    var lower = String(result).toLowerCase();
    if (['success', 'ok', 'active', 'up', 'healthy'].indexOf(lower) !== -1) return 'result-ok';
    if (['failed', 'error', 'failure', 'degraded', 'down', 'err'].indexOf(lower) !== -1) return 'result-err';
    if (['aborted', 'timeout', 'cancelled', 'suspect'].indexOf(lower) !== -1) return 'result-warn';
    return '';
}

// ── Modal open/close ──

function openStatusModal() {
    if (!statusState.modal || !statusState.modal.hidden) {
        return;
    }
    statusState.openedAt = Date.now();
    statusState.lastRefresh = Date.now();
    updateRefreshBadge();
    loadStatusSummary();
    switchStatusTab(statusState.activeTab);
    statusState.modal.hidden = false;
    document.body.classList.add('modal-open');
    startAutoRefresh();
}

function closeStatusModal() {
    if (statusState.openedAt && Date.now() - statusState.openedAt < 160) {
        return;
    }
    stopAutoRefresh();
    statusState.modal.hidden = true;
    document.body.classList.remove('modal-open');
    cancelStatusRequests();
    statusState.openedAt = 0;
}

function switchStatusTab(name) {
    statusState.activeTab = name;
    var buttons = document.querySelectorAll('[data-status-tab]');
    for (var i = 0; i < buttons.length; i++) {
        var active = buttons[i].getAttribute('data-status-tab') === name;
        buttons[i].classList.toggle('is-active', active);
        buttons[i].setAttribute('aria-selected', active ? 'true' : 'false');
    }
    var diskPanel = document.getElementById('status-tab-disk');
    var eventsPanel = document.getElementById('status-tab-events');
    var showDisk = name === 'disk';
    diskPanel.hidden = !showDisk;
    eventsPanel.hidden = showDisk;
    diskPanel.classList.toggle('is-active', showDisk);
    eventsPanel.classList.toggle('is-active', !showDisk);
    if (showDisk) {
        loadDiskStatus();
        return;
    }
    loadEventStatus();
}

// ── Auto-refresh ──

function startAutoRefresh() {
    stopAutoRefresh();
    statusState.refreshTimer = setInterval(refreshActiveTab, 30000);
}

function stopAutoRefresh() {
    if (statusState.refreshTimer) {
        clearInterval(statusState.refreshTimer);
        statusState.refreshTimer = null;
    }
}

function refreshActiveTab() {
    var tab = statusState.activeTab;
    var scrollEl = tab === 'disk' ? document.querySelector('.chart-card') : document.querySelector('.table-wrap');
    if (scrollEl) statusState.scrollTops[tab] = scrollEl.scrollTop;

    delete statusState.cache.summary;
    delete statusState.cache[tab];

    ['summary', tab].forEach(function(k) {
        if (statusState.controllers[k]) {
            statusState.controllers[k].abort();
            statusState.controllers[k] = null;
        }
    });

    statusState.lastRefresh = Date.now();
    updateRefreshBadge();
    loadStatusSummary();
    if (tab === 'disk') loadDiskStatus();
    else loadEventStatus();
}

function updateRefreshBadge() {
    var t = window.I18N;
    var badge = document.getElementById('refresh-badge');
    if (!badge) return;
    var stamp = formatDisplayTime(new Date(statusState.lastRefresh).toISOString());
    var text = stamp.display || (t.just_now || 'just now');
    var label = badge.querySelector('.refresh-label');
    if (label) label.textContent = (t.auto_refresh || 'Auto-refresh 30s') + ' \u00b7 ' + text;
    badge.title = stamp.exact || '';
}

function lastRefreshNote() {
    var t = window.I18N;
    var stamp = formatDisplayTime(new Date(statusState.lastRefresh).toISOString());
    return '<span class="last-refresh" title="' + escapeHTML(stamp.exact) + '">' +
        escapeHTML((t.last_refreshed || 'Last refreshed') + ': ' + stamp.display) +
        '</span>';
}

// ── Status summary (card layout) ──

function loadStatusSummary() {
    var target = document.getElementById('status-summary');
    var t = window.I18N;
    if (!statusState.cache.summary) {
        target.innerHTML = summarySkeletonHTML();
    }
    fetchStatus('summary', '/-/status/summary', function(data) {
        var healthy = data.healthy;
        var degraded = data.degraded_objects || 0;
        var healthClass = healthy ? 'summary-card-ok' : 'summary-card-err';
        var healthText = healthy ? t.store_healthy : (t.store_degraded || '%d degraded').replace('%d', String(degraded));

        var cards = [];
        cards.push('<div class="summary-card">' +
            '<div class="summary-card-label">' + escapeHTML(t.summary_store || 'Store') + '</div>' +
            '<div class="summary-card-value ' + healthClass + '">' + escapeHTML(healthText) + '</div>' +
            '</div>');

        if (data.last_sample_at) {
            var lastSample = formatDisplayTime(data.last_sample_at);
            cards.push('<div class="summary-card">' +
                '<div class="summary-card-label">' + escapeHTML(t.last_sample || 'Last sample') + '</div>' +
                '<div class="summary-card-value" title="' + escapeHTML(lastSample.exact) + '">' + escapeHTML(lastSample.display) + '</div>' +
                '<div class="summary-card-sub">' + escapeHTML(lastSample.exact) + '</div>' +
                '</div>');
        }
        target.innerHTML = cards.join('');
        target.classList.remove('is-fade-in');
        void target.offsetWidth;
        target.classList.add('is-fade-in');
    }, function() {
        target.innerHTML = '<div class="status-summary-line">' + escapeHTML(t.status_load_failed || 'Failed to load status') + '</div>';
    });
}

// ── Disk chart ──

function loadDiskStatus() {
    var note = document.getElementById('disk-panel-note');
    var chart = document.getElementById('disk-chart');
    var card = document.querySelector('.chart-card');
    var t = window.I18N;
    if (!statusState.cache.disk) {
        note.innerHTML = spinnerHTML() + escapeHTML(t.loading);
        chart.className = 'disk-chart empty-state';
        chart.textContent = t.loading;
    } else if (card) {
        card.classList.add('is-loading');
    }
    fetchStatus('disk', '/-/status/disk', function(data) {
        var samples = data.samples || [];
        note.innerHTML = escapeHTML(diskWindowNote(samples)) + lastRefreshNote();
        if (!samples.length) {
            chart.className = 'disk-chart empty-state is-fade-in';
            chart.textContent = t.no_data || 'No data';
            if (card) {
                card.classList.remove('is-loading');
            }
            return;
        }
        renderDiskChart(chart, samples);
        if (card) {
            card.classList.remove('is-loading');
        }
        restoreScroll('disk');
    }, function() {
        note.textContent = t.status_load_failed || 'Failed to load status';
        chart.textContent = t.status_load_failed || 'Failed to load status';
        if (card) {
            card.classList.remove('is-loading');
        }
    });
}

function renderDiskChart(target, samples) {
    var t = window.I18N;
    var width = 800;
    var height = 280;
    var padLeft = 60;
    var padRight = 30;
    var padTop = 18;
    var padBottom = 26;

    var values = samples.map(function(item) { return item.total_bytes; });
    var min = Math.min.apply(Math, values);
    var max = Math.max.apply(Math, values);
    if (min === max) {
        min = Math.max(0, min - 1);
        max = max + 1;
    }
    var span = max - min;
    var chartW = width - padLeft - padRight;
    var chartH = height - padTop - padBottom;
    var step = values.length === 1 ? 0 : chartW / (values.length - 1);

    var points = [];
    for (var i = 0; i < values.length; i++) {
        var x = padLeft + step * i;
        var y = padTop + chartH - ((values[i] - min) / span) * chartH;
        points.push(x.toFixed(2) + ',' + y.toFixed(2));
    }

    var fillPoints = points.slice();
    fillPoints.unshift((padLeft).toFixed(2) + ',' + (padTop + chartH).toFixed(2));
    fillPoints.push((width - padRight).toFixed(2) + ',' + (padTop + chartH).toFixed(2));

    var yLabels = '';
    yLabels += '<text x="' + (padLeft - 6) + '" y="' + (padTop + 10) + '" class="chart-axis-label" text-anchor="end">' + escapeHTML(formatBytes(max)) + '</text>';
    yLabels += '<text x="' + (padLeft - 6) + '" y="' + (padTop + chartH) + '" class="chart-axis-label" text-anchor="end">' + escapeHTML(formatBytes(min)) + '</text>';

    var xLabels = '';
    if (samples.length >= 2) {
        var firstTime = formatDateTime(samples[0].at);
        var midTime = formatDateTime(samples[Math.floor(samples.length / 2)].at);
        var lastTime = formatDateTime(samples[samples.length - 1].at);
        xLabels += '<text x="' + padLeft + '" y="' + (height - 5) + '" class="chart-axis-label" text-anchor="start">' + escapeHTML(firstTime) + '</text>';
        if (samples.length > 2) {
            xLabels += '<text x="' + (width / 2) + '" y="' + (height - 5) + '" class="chart-axis-label" text-anchor="middle">' + escapeHTML(midTime) + '</text>';
        }
        xLabels += '<text x="' + (width - padRight) + '" y="' + (height - 5) + '" class="chart-axis-label" text-anchor="end">' + escapeHTML(lastTime) + '</text>';
    }

    var last = samples[samples.length - 1];
    var lastStamp = formatDisplayTime(last.at);
    var cs = getComputedStyle(document.documentElement);
    var gradFrom = cs.getPropertyValue('--chart-fill-from').trim() || 'rgba(30,111,103,.22)';
    var gradTo = cs.getPropertyValue('--chart-fill-to').trim() || 'rgba(30,111,103,.02)';

    target.className = 'disk-chart';
    target.innerHTML = '' +
        '<div class="chart-meta">' +
        '<strong>' + escapeHTML(formatBytes(last.total_bytes)) + '</strong>' +
        '<span title="' + escapeHTML(lastStamp.exact) + '">' + escapeHTML(lastStamp.display) + '</span>' +
        '</div>' +
        '<svg viewBox="0 0 ' + width + ' ' + height + '" aria-label="' + escapeHTML(t.disk_tab || 'Disk') + '" preserveAspectRatio="xMidYMid meet">' +
        '<defs><linearGradient id="chartGrad" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="' + gradFrom + '" /><stop offset="100%" stop-color="' + gradTo + '" /></linearGradient></defs>' +
        '<line class="chart-grid" x1="' + padLeft + '" y1="' + (padTop + chartH) + '" x2="' + (width - padRight) + '" y2="' + (padTop + chartH) + '"></line>' +
        '<polygon class="chart-fill" points="' + fillPoints.join(' ') + '"></polygon>' +
        '<polyline class="chart-line" points="' + points.join(' ') + '"></polyline>' +
        yLabels + xLabels +
        '</svg>';
    target.classList.add('is-fade-in');
}

// ── Events table ──

function loadEventStatus() {
    var note = document.getElementById('events-panel-note');
    var table = document.getElementById('events-table');
    var wrap = document.querySelector('.table-wrap');
    var t = window.I18N;
    if (!statusState.cache.events) {
        note.innerHTML = spinnerHTML() + escapeHTML(t.loading);
        table.className = 'empty-state';
        table.textContent = t.loading;
    } else if (wrap) {
        wrap.classList.add('is-loading');
    }
    fetchStatus('events', '/-/status/events?limit=' + eventRequestLimit(), function(data) {
        var events = data.events || [];
        var eventLimit = statusState.cache.summary && statusState.cache.summary.event_limit ? statusState.cache.summary.event_limit : events.length;
        note.innerHTML = escapeHTML((t.event_limit_note || 'Keeps the latest %d events').replace('%d', String(eventLimit))) + lastRefreshNote();
        renderEventsTable(table, events);
        if (wrap) {
            wrap.classList.remove('is-loading');
        }
        restoreScroll('events');
    }, function() {
        note.textContent = t.status_load_failed || 'Failed to load status';
        table.textContent = t.status_load_failed || 'Failed to load status';
        if (wrap) {
            wrap.classList.remove('is-loading');
        }
    });
}

function renderEventsTable(target, events) {
    var t = window.I18N;
    if (!events.length) {
        target.className = 'empty-state';
        target.textContent = t.no_data || 'No data';
        return;
    }
    var rows = events.slice().reverse().map(function(item) {
        var rClass = resultClass(item.result);
        var rText = translateResult(item.result);
        var started = formatDisplayTime(item.started_at);
        var finished = formatDisplayTime(item.finished_at);
        return '' +
            '<tr>' +
            '<td class="col-clip" title="' + escapeHTML(item.storage) + '">' + escapeHTML(item.storage) + '</td>' +
            '<td class="col-narrow" title="' + escapeHTML(item.task_type) + '">' + escapeHTML(translateTaskType(item.task_type)) + '</td>' +
            '<td title="' + escapeHTML(item.target) + '"><span class="clip-cell">' + escapeHTML(item.target) + '</span></td>' +
            '<td class="col-narrow" title="' + escapeHTML(started.exact) + '">' + escapeHTML(started.display) + '</td>' +
            '<td class="col-narrow" title="' + escapeHTML(finished.exact) + '">' + escapeHTML(finished.display) + '</td>' +
            '<td class="col-narrow">' + escapeHTML(formatDurationMS(item.duration_ms)) + '</td>' +
            '<td class="col-narrow"><span class="result-badge ' + rClass + '">' + escapeHTML(rText) + '</span></td>' +
            '<td title="' + escapeHTML(item.message || '') + '"><span class="clip-cell">' + escapeHTML(item.message || '') + '</span></td>' +
            '</tr>';
    });
    target.className = '';
    target.innerHTML = '' +
        '<table class="status-table">' +
        '<thead><tr>' +
        '<th>' + escapeHTML(t.storage_name || 'Storage') + '</th>' +
        '<th>' + escapeHTML(t.task_type || 'Task') + '</th>' +
        '<th>' + escapeHTML(t.task_target || 'Target') + '</th>' +
        '<th>' + escapeHTML(t.started_at || 'Started') + '</th>' +
        '<th>' + escapeHTML(t.finished_at || 'Finished') + '</th>' +
        '<th>' + escapeHTML(t.duration || 'Duration') + '</th>' +
        '<th>' + escapeHTML(t.result || 'Result') + '</th>' +
        '<th>' + escapeHTML(t.reason || 'Reason') + '</th>' +
        '</tr></thead>' +
        '<tbody>' + rows.join('') + '</tbody>' +
        '</table>';
    target.classList.add('is-fade-in');
}

// ── Scroll preservation ──

function restoreScroll(tab) {
    var el = tab === 'disk' ? document.querySelector('.chart-card') : document.querySelector('.table-wrap');
    if (el && statusState.scrollTops[tab]) {
        el.scrollTop = statusState.scrollTops[tab];
    }
}

function initStatusModal() {
    statusState.modal = document.getElementById('status-modal');
    var backdrop = document.getElementById('status-modal-backdrop');
    if (backdrop) {
        backdrop.addEventListener('click', function() {
            closeStatusModal();
        });
    }
    if (statusState.modal) {
        var dialog = statusState.modal.querySelector('.modal');
        if (dialog) {
            dialog.addEventListener('click', function(evt) {
                evt.stopPropagation();
            });
        }
    }
    document.addEventListener('keydown', function(evt) {
        if (evt.key === 'Escape' && !statusState.modal.hidden) {
            closeStatusModal();
        }
    });
}
