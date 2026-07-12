// ── Status modal data loading ──

function cancelStatusRequests() {
    ['summary', 'disk', 'network', 'events'].forEach(function(key) {
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
    var key = 'result_' + String(result).toLowerCase();
    return t[key] || result;
}

function translateReason(reason) {
    var t = window.I18N;
    var key = 'reason_' + String(reason || '').toLowerCase();
    return t[key] || reason;
}

function translateUpstreamState(state) {
    var t = window.I18N;
    var key = 'upstream_state_' + String(state || '').toLowerCase();
    return t[key] || translateResult(state);
}

function translateTaskType(taskType) {
    var t = window.I18N;
    var key = 'task_' + String(taskType || '').toLowerCase();
    return t[key] || taskType;
}

function resultClass(result) {
    var lower = String(result).toLowerCase();
    var okResults = ['success', 'updated', 'unchanged', 'ok', 'active', 'up', 'healthy', 'closed'];
    if (okResults.indexOf(lower) !== -1) return 'result-ok';
    if (['failed', 'error', 'failure', 'degraded', 'down', 'err', 'open'].indexOf(lower) !== -1) return 'result-err';
    if (['aborted', 'timeout', 'cancelled', 'suspect', 'halfopen'].indexOf(lower) !== -1) return 'result-warn';
    return '';
}

function formatEventMessage(item) {
    if (item.task_type !== 'upstream_state') {
        var taskParts = [];
        if (item.reason_code) {
            taskParts.push(translateReason(item.reason_code));
        }
        if (item.detail) {
            taskParts.push(item.detail);
        }
        if (item.message) {
            taskParts.push(item.message);
        }
        return taskParts.join(' · ');
    }
    var parts = [];
    if (item.reason_code) {
        parts.push(translateReason(item.reason_code));
    }
    if (item.detail) {
        parts.push(item.detail);
    }
    if (item.state_from && item.result) {
        parts.push(translateUpstreamState(item.state_from) + ' -> ' + translateUpstreamState(item.result));
    }
    if (parts.length) {
        return parts.join(' · ');
    }
    return item.message || '';
}

// ── Modal open/close ──

function openStatusModal() {
    if (!statusState.modal || statusState.modal.classList.contains('is-open')) {
        return;
    }
    statusState.openedAt = Date.now();
    statusState.lastRefresh = Date.now();
    updateRefreshBadge();
    loadStatusSummary();
    switchStatusTab(statusState.activeTab);
    document.body.classList.add('modal-open');
    statusState.modal.setAttribute('aria-hidden', 'false');
    statusState.modal.classList.add('is-open');
    startAutoRefresh();
}

function closeStatusModal() {
    if (statusState.openedAt && Date.now() - statusState.openedAt < 160) {
        return;
    }
    stopAutoRefresh();
    statusState.modal.classList.remove('is-open');
    statusState.modal.setAttribute('aria-hidden', 'true');
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
    var networkPanel = document.getElementById('status-tab-network');
    var eventsPanel = document.getElementById('status-tab-events');
    var showDisk = name === 'disk';
    var showNetwork = name === 'network';
    diskPanel.hidden = !showDisk;
    networkPanel.hidden = !showNetwork;
    eventsPanel.hidden = showDisk || showNetwork;
    diskPanel.classList.toggle('is-active', showDisk);
    networkPanel.classList.toggle('is-active', showNetwork);
    eventsPanel.classList.toggle('is-active', !showDisk && !showNetwork);
    if (showDisk) {
        loadDiskStatus();
        return;
    }
    if (showNetwork) {
        loadNetworkStatus();
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
    var scrollEl = tab === 'disk' ? document.querySelector('.chart-card') :
        (tab === 'network' ? document.querySelector('.network-table-wrap') : document.querySelector('.table-wrap'));
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
    else if (tab === 'network') loadNetworkStatus();
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

// ── Network map ──

function loadNetworkStatus() {
    var note = document.getElementById('network-panel-note');
    var map = document.getElementById('network-map');
    var table = document.getElementById('network-table');
    var t = window.I18N;
    if (!statusState.cache.network) {
        note.innerHTML = spinnerHTML() + escapeHTML(t.loading);
        map.className = 'network-map empty-state';
        table.className = 'network-table-wrap empty-state';
        map.textContent = t.loading;
        table.textContent = t.loading;
    }
    fetchStatus('network', '/-/status/network', function(data) {
        var filter = activeNetworkFilter();
        note.innerHTML = renderNetworkSummary(data) + lastRefreshNote();
        renderNetworkMap(map, filterNetworkEdges(data.edges || [], filter), data);
        renderNetworkTable(table, filterNetworkEdges(data.edges || [], filter));
        restoreScroll('network');
    }, function() {
        note.textContent = t.status_load_failed || 'Failed to load status';
        map.className = 'network-map empty-state';
        table.className = 'network-table-wrap empty-state';
        map.textContent = t.status_load_failed || 'Failed to load status';
        table.textContent = t.status_load_failed || 'Failed to load status';
    });
}

function activeNetworkFilter() {
    var active = document.querySelector('[data-network-filter].is-active');
    return active ? active.getAttribute('data-network-filter') : 'all';
}

function filterNetworkEdges(edges, filter) {
    if (filter === 'active') {
        return edges.filter(function(edge) { return edge.active_upstream_requests > 0; });
    }
    if (filter === 'degraded') {
        return edges.filter(function(edge) {
            return edge.state && edge.state !== 'closed' && edge.state !== 'unknown';
        });
    }
    if (filter === 'failed') {
        return edges.filter(function(edge) { return edge.errors > 0 || edge.last_error; });
    }
    return edges;
}

function renderNetworkSummary(data) {
    var t = window.I18N;
    var s = data.summary || {};
    var cards = [
        [t.network_active || 'Active upstream', String(s.active_upstream_requests || 0)],
        [t.network_hit_rate || 'Hit rate', formatPercent(s.hit_rate || 0)],
        [t.network_error_rate || 'Upstream errors', formatPercent(s.upstream_error_rate || 0)],
        [t.network_traffic || 'Upstream traffic', formatBytes(s.upstream_bytes || 0)],
        [t.network_degraded || 'Degraded upstreams', String(s.degraded_upstreams || 0)]
    ];
    return '<div class="network-summary">' + cards.map(function(card) {
        return '<div class="network-metric"><span>' + escapeHTML(card[0]) + '</span><strong>' +
            escapeHTML(card[1]) + '</strong></div>';
    }).join('') + '</div>';
}

function formatPercent(value) {
    return (value * 100).toFixed(value > 0 && value < 0.1 ? 1 : 0).replace(/\.0$/, '') + '%';
}

function renderNetworkMap(target, edges, data) {
    var t = window.I18N;
    if (!edges.length) {
        target.className = 'network-map empty-state';
        target.removeAttribute('data-focus-id');
        target.onclick = null;
        target.onmouseover = null;
        target.onmouseout = null;
        target.textContent = t.no_data || 'No data';
        return;
    }
    var instances = (data.instances || []).filter(function(instance) {
        return edges.some(function(edge) { return edge.instance === instance.name; });
    });
    var upstreamByID = {};
    (data.upstreams || []).forEach(function(upstream) {
        upstreamByID[upstream.id] = upstream;
    });
    var upstreams = [];
    edges.forEach(function(edge) {
        if (upstreamByID[edge.to] && upstreams.indexOf(upstreamByID[edge.to]) === -1) {
            upstreams.push(upstreamByID[edge.to]);
        }
    });
    var width = Math.max(820, target.clientWidth ? target.clientWidth - 28 : 0);
    var height = Math.max(260, Math.max(instances.length, upstreams.length, 1) * 76 + 64);
    var proxy = { x: 86, y: height / 2 };
    var instX = Math.round(width * 0.38);
    var upX = width - 140;
    var instancePos = {};
    var upstreamPos = {};
    instances.forEach(function(instance, idx) {
        instancePos[instance.id] = { x: instX, y: laneY(idx, instances.length, height) };
    });
    upstreams.forEach(function(upstream, idx) {
        upstreamPos[upstream.id] = { x: upX, y: laneY(idx, upstreams.length, height) };
    });
    var maxReq = Math.max.apply(Math, edges.map(function(edge) { return edge.requests || 1; }));
    var edgeSVG = edges.map(function(edge) {
        var a = instancePos[edge.from];
        var b = upstreamPos[edge.to];
        if (!a || !b) return '';
        var width = 1.4 + Math.min(7, ((edge.requests || 1) / maxReq) * 7);
        var cls = 'network-edge state-' + escapeHTML(edge.state || 'unknown') +
            (edge.active_upstream_requests > 0 ? ' is-active' : '');
        var title = edge.instance + ' -> ' + edge.upstream_url + (edge.last_error ? ' (' + edge.last_error + ')' : '');
        return '<path class="' + cls + '" data-edge-from="' + escapeHTML(edge.from) +
            '" data-edge-to="' + escapeHTML(edge.to) + '" d="M' + a.x + ',' + a.y + ' C' + (a.x + 120) + ',' + a.y +
            ' ' + (b.x - 120) + ',' + b.y + ' ' + b.x + ',' + b.y + '" stroke-width="' +
            width.toFixed(1) + '"><title>' + escapeHTML(title) + '</title></path>';
    }).join('');
    var proxyEdges = instances.map(function(instance) {
        var p = instancePos[instance.id];
        return '<path class="network-edge state-closed" data-edge-from="proxy:cache" data-edge-to="' +
            escapeHTML(instance.id) + '" d="M' + proxy.x + ',' + proxy.y + ' C170,' +
            proxy.y + ' 230,' + p.y + ' ' + p.x + ',' + p.y + '" stroke-width="2"></path>';
    }).join('');
    target.className = 'network-map';
    target.innerHTML = '<svg viewBox="0 0 ' + width + ' ' + height + '" preserveAspectRatio="xMidYMid meet">' +
        proxyEdges + edgeSVG +
        networkNode(proxy.x, proxy.y, 'proxy:cache', 'proxy', t.title || 'Cache Proxy', 'closed') +
        instances.map(function(instance) {
            var p = instancePos[instance.id];
            return networkNode(p.x, p.y, instance.id, 'instance', instance.name, 'closed', instance.mode);
        }).join('') +
        upstreams.map(function(upstream) {
            var p = upstreamPos[upstream.id];
            return networkNode(p.x, p.y, upstream.id, 'upstream', upstream.host, upstream.state || 'unknown',
                networkUpstreamNodeSubtitle(upstream));
        }).join('') +
        '</svg>';
    wireNetworkMapFocus(target);
    target.classList.add('is-fade-in');
}

function networkUpstreamNodeSubtitle(upstream) {
    var t = window.I18N;
    var active = upstream.active_upstream_requests || 0;
    if (active > 0) {
        return String(active) + ' ' + (t.network_active_short || 'active');
    }
    var requests = upstream.requests || 0;
    if (requests > 0) {
        return String(requests) + ' ' + (t.requests_short || 'req');
    }
    if (upstream.latency_ms > 0) {
        return Math.round(upstream.latency_ms) + 'ms';
    }
    return translateUpstreamState(upstream.state || 'unknown');
}

function laneY(index, total, height) {
    if (total <= 1) return height / 2;
    var top = 52;
    var bottom = height - 52;
    return top + ((bottom - top) * index / (total - 1));
}

function networkNode(x, y, id, kind, label, state, sub) {
    var displayLabel = compactNetworkLabel(label);
    return '<g class="network-node node-' + escapeHTML(kind) + ' state-' + escapeHTML(state || 'unknown') +
        '" data-node-id="' + escapeHTML(id) + '" transform="translate(' + x + ' ' + y + ')">' +
        '<title>' + escapeHTML(label) + '</title>' +
        '<circle r="22"></circle>' +
        '<text class="node-label" x="0" y="-30" text-anchor="middle">' + escapeHTML(displayLabel) + '</text>' +
        (sub ? '<text class="node-sub" x="0" y="40" text-anchor="middle">' + escapeHTML(sub) + '</text>' : '') +
        '</g>';
}

function compactNetworkLabel(label) {
    label = String(label || '');
    if (label.length <= 28) return label;
    return label.slice(0, 13) + '...' + label.slice(-12);
}

function wireNetworkMapFocus(target) {
    target.onclick = function(evt) {
        var node = evt.target.closest && evt.target.closest('.network-node');
        if (!node) {
            applyNetworkFocus(target, '');
            return;
        }
        var id = node.getAttribute('data-node-id') || '';
        applyNetworkFocus(target, target.getAttribute('data-focus-id') === id ? '' : id);
    };
    target.onmouseover = function(evt) {
        if (target.getAttribute('data-focus-id')) return;
        var node = evt.target.closest && evt.target.closest('.network-node');
        if (node) applyNetworkFocus(target, node.getAttribute('data-node-id') || '');
    };
    target.onmouseout = function(evt) {
        if (target.getAttribute('data-focus-id')) return;
        var node = evt.target.closest && evt.target.closest('.network-node');
        if (node && (!evt.relatedTarget || !node.contains(evt.relatedTarget))) {
            applyNetworkFocus(target, '');
        }
    };
}

function applyNetworkFocus(target, focusID) {
    target.setAttribute('data-focus-id', focusID || '');
    var edges = target.querySelectorAll('.network-edge');
    var nodes = target.querySelectorAll('.network-node');
    if (!focusID) {
        target.classList.remove('has-focus');
        edges.forEach(function(edge) {
            edge.classList.remove('is-focused', 'is-dimmed');
        });
        nodes.forEach(function(node) {
            node.classList.remove('is-focused', 'is-dimmed');
        });
        return;
    }
    target.classList.add('has-focus');
    var connected = {};
    connected[focusID] = true;
    edges.forEach(function(edge) {
        var from = edge.getAttribute('data-edge-from') || '';
        var to = edge.getAttribute('data-edge-to') || '';
        var hit = from === focusID || to === focusID;
        edge.classList.toggle('is-focused', hit);
        edge.classList.toggle('is-dimmed', !hit);
        if (hit) {
            connected[from] = true;
            connected[to] = true;
        }
    });
    nodes.forEach(function(node) {
        var id = node.getAttribute('data-node-id') || '';
        node.classList.toggle('is-focused', !!connected[id]);
        node.classList.toggle('is-dimmed', !connected[id]);
    });
}

function renderNetworkTable(target, edges) {
    var t = window.I18N;
    if (!edges.length) {
        target.className = 'network-table-wrap empty-state';
        target.textContent = t.no_data || 'No data';
        return;
    }
    var rows = edges.slice().sort(function(a, b) {
        return (b.active_upstream_requests || 0) - (a.active_upstream_requests || 0) ||
            (b.requests || 0) - (a.requests || 0);
    }).map(function(edge) {
        var used = formatDisplayTime(edge.last_used_at);
        var title = edge.upstream_url + (edge.last_error ? ' - ' + edge.last_error : '');
        return '<tr title="' + escapeHTML(title) + '">' +
            '<td title="' + escapeHTML(edge.instance) + '">' + escapeHTML(edge.instance) + '</td>' +
            '<td title="' + escapeHTML(edge.upstream_url) + '"><span class="clip-cell">' +
            escapeHTML(edge.upstream_host) + '</span></td>' +
            '<td><span class="result-badge ' + resultClass(edge.state) + '">' +
            escapeHTML(translateUpstreamState(edge.state)) + '</span></td>' +
            '<td>' + escapeHTML(edge.last_status || '') + '</td>' +
            '<td>' + escapeHTML(String(edge.active_upstream_requests || 0)) + '</td>' +
            '<td>' + escapeHTML(String(edge.requests || 0)) + '</td>' +
            '<td>' + escapeHTML(String(edge.errors || 0)) + '</td>' +
            '<td>' + escapeHTML(formatPercent(edge.error_rate || 0)) + '</td>' +
            '<td>' + escapeHTML(formatBytes(edge.response_bytes || 0)) + '</td>' +
            '<td>' + escapeHTML((edge.latency_ms || 0).toFixed(0)) + 'ms</td>' +
            '<td title="' + escapeHTML(used.exact) + '">' + escapeHTML(used.display || '') + '</td>' +
            '</tr>';
    });
    target.className = 'network-table-wrap';
    target.innerHTML = '<table class="status-table">' +
        '<thead><tr>' +
        '<th>' + escapeHTML(t.storage_name || 'Storage') + '</th>' +
        '<th>' + escapeHTML(t.upstream || 'Upstream') + '</th>' +
        '<th>' + escapeHTML(t.status || 'Status') + '</th>' +
        '<th>' + escapeHTML(t.last_status || 'Last status') + '</th>' +
        '<th>' + escapeHTML(t.network_active || 'Active') + '</th>' +
        '<th>' + escapeHTML(t.requests || 'Requests') + '</th>' +
        '<th>' + escapeHTML(t.errors || 'Errors') + '</th>' +
        '<th>' + escapeHTML(t.network_error_rate || 'Error rate') + '</th>' +
        '<th>' + escapeHTML(t.network_traffic || 'Traffic') + '</th>' +
        '<th>' + escapeHTML(t.network_latency || 'Latency') + '</th>' +
        '<th>' + escapeHTML(t.last_used || 'Last used') + '</th>' +
        '</tr></thead><tbody>' + rows.join('') + '</tbody></table>';
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
        renderEventsTable(table, filterEvents(events));
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

function filterEvents(events) {
    var input = document.getElementById('events-search');
    var query = input ? input.value.trim().toLowerCase() : '';
    if (!query) {
        return events;
    }
    return events.filter(function(item) {
        return [
            item.storage,
            item.task_type,
            translateTaskType(item.task_type),
            item.target,
            item.result,
            translateResult(item.result),
            item.message,
            item.reason_code,
            item.detail,
            item.state_from
        ].some(function(value) {
            return String(value || '').toLowerCase().indexOf(query) !== -1;
        });
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
        var rText = item.task_type === 'upstream_state' ? translateUpstreamState(item.result) : translateResult(item.result);
        var started = formatDisplayTime(item.started_at);
        var finished = formatDisplayTime(item.finished_at);
        var message = formatEventMessage(item);
        return '' +
            '<tr>' +
            '<td class="col-clip" title="' + escapeHTML(item.storage) + '">' + escapeHTML(item.storage) + '</td>' +
            '<td class="col-narrow" title="' + escapeHTML(item.task_type) + '">' + escapeHTML(translateTaskType(item.task_type)) + '</td>' +
            '<td title="' + escapeHTML(item.target) + '"><span class="clip-cell">' + escapeHTML(item.target) + '</span></td>' +
            '<td class="col-narrow" title="' + escapeHTML(started.exact) + '">' + escapeHTML(started.display) + '</td>' +
            '<td class="col-narrow" title="' + escapeHTML(finished.exact) + '">' + escapeHTML(finished.display) + '</td>' +
            '<td class="col-narrow">' + escapeHTML(formatDurationMS(item.duration_ms)) + '</td>' +
            '<td class="col-narrow"><span class="result-badge ' + rClass + '">' + escapeHTML(rText) + '</span></td>' +
            '<td title="' + escapeHTML(message) + '"><span class="clip-cell">' + escapeHTML(message) + '</span></td>' +
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
    var el = tab === 'disk' ? document.querySelector('.chart-card') :
        (tab === 'network' ? document.querySelector('.network-table-wrap') : document.querySelector('.table-wrap'));
    if (el && statusState.scrollTops[tab]) {
        el.scrollTop = statusState.scrollTops[tab];
    }
}

function initStatusModal() {
    statusState.modal = document.getElementById('status-modal');
    if (statusState.modal) {
        statusState.modal.removeAttribute('hidden');
        statusState.modal.setAttribute('aria-hidden', 'true');
    }
    var openBtn = document.getElementById('status-btn');
    if (openBtn) {
        openBtn.addEventListener('click', function(evt) {
            evt.preventDefault();
            evt.stopPropagation();
            openStatusModal();
        });
    }
    var backdrop = document.getElementById('status-modal-backdrop');
    if (backdrop) {
        backdrop.addEventListener('click', function(evt) {
            if (evt.target !== backdrop) {
                return;
            }
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
        if (evt.key === 'Escape' && statusState.modal && statusState.modal.classList.contains('is-open')) {
            closeStatusModal();
        }
    });
    var search = document.getElementById('events-search');
    if (search) {
        search.addEventListener('input', function() {
            var table = document.getElementById('events-table');
            var cached = statusState.cache.events;
            if (!table || !cached) {
                return;
            }
            renderEventsTable(table, filterEvents(cached.events || []));
        });
    }
    var networkFilters = document.querySelectorAll('[data-network-filter]');
    for (var i = 0; i < networkFilters.length; i++) {
        networkFilters[i].addEventListener('click', function() {
            for (var j = 0; j < networkFilters.length; j++) {
                networkFilters[j].classList.remove('is-active');
            }
            this.classList.add('is-active');
            var cached = statusState.cache.network;
            if (!cached) {
                return;
            }
            var edges = filterNetworkEdges(cached.edges || [], activeNetworkFilter());
            renderNetworkMap(document.getElementById('network-map'), edges, cached);
            renderNetworkTable(document.getElementById('network-table'), edges);
        });
    }
}
