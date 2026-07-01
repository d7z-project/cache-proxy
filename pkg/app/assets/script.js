var statusState = {
    modal: null,
    activeTab: 'disk',
    controllers: { summary: null, disk: null, events: null },
    cache: { summary: null, disk: null, events: null }
};

function fallbackCopy(txt) {
    var node = document.createElement('textarea');
    node.value = txt;
    node.setAttribute('readonly', '');
    node.style.position = 'fixed';
    node.style.left = '-9999px';
    document.body.appendChild(node);
    node.select();
    var ok = document.execCommand('copy');
    document.body.removeChild(node);
    return ok;
}

function setCopyState(btn, key, cls) {
    var t = window.I18N;
    btn.textContent = t[key] || key;
    btn.classList.add(cls);
    setTimeout(function() {
        btn.textContent = t.copy;
        btn.classList.remove(cls);
    }, 1500);
}

function copyToClipboard(btn) {
    var txt = btn.getAttribute('data-copy') || '';
    var copied = false;
    var done = function(ok) {
        setCopyState(btn, ok ? 'copied' : 'copy_failed', ok ? 'copied' : 'copy-failed');
    };
    if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(txt).then(function() {
            done(true);
        }).catch(function() {
            try {
                copied = fallbackCopy(txt);
            } catch (e) {
                copied = false;
            }
            done(copied);
        });
        return;
    }
    try {
        copied = fallbackCopy(txt);
    } catch (e) {
        copied = false;
    }
    done(copied);
}

function copyURL(btn) { copyToClipboard(btn); }
function copyCode(btn) { copyToClipboard(btn); }

function toggleReleases(btn) {
    var open = btn.classList.toggle('open');
    btn.setAttribute('aria-expanded', open);
}

function sunIcon() {
    return '' +
        '<svg viewBox="0 0 24 24" aria-hidden="true">' +
        '<circle cx="12" cy="12" r="4.2"></circle>' +
        '<path d="M12 2.5v2.3M12 19.2v2.3M4.9 4.9l1.6 1.6M17.5 17.5l1.6 1.6M2.5 12h2.3M19.2 12h2.3M4.9 19.1l1.6-1.6M17.5 6.5l1.6-1.6"></path>' +
        '</svg>';
}

function moonIcon() {
    return '' +
        '<svg viewBox="0 0 24 24" aria-hidden="true">' +
        '<path d="M14.5 2.8c-1 1-1.7 2.4-1.7 4 0 3.2 2.6 5.8 5.8 5.8 1.1 0 2.2-.3 3.1-.9-.4 4.9-4.5 8.8-9.5 8.8-5.3 0-9.6-4.3-9.6-9.6 0-5 3.9-9.1 8.8-9.5.9-.1 2.1.4 3.1 1.4z"></path>' +
        '</svg>';
}

function toggleTheme() {
    var root = document.documentElement;
    var next = root.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
    root.setAttribute('data-theme', next);
    document.cookie = 'theme=' + next + ';path=/;max-age=31536000;samesite=lax';
    updateThemeBtn(next);
}

function toggleLang() {
    var url = new URL(window.location.href);
    var next = url.searchParams.get('lang') === 'zh' ? 'en' : 'zh';
    url.searchParams.set('lang', next);
    window.location.search = url.searchParams.toString();
}

function updateThemeBtn(theme) {
    var btn = document.getElementById('theme-btn');
    if (!btn) return;
    btn.innerHTML = theme === 'dark' ? sunIcon() : moonIcon();
}

function initSearch() {
    var input = document.getElementById('instance-search');
    if (!input) return;

    var cards = Array.prototype.slice.call(document.querySelectorAll('.grid .card')).map(function(card) {
        return {
            node: card,
            mode: card.getAttribute('data-mode') || '',
            status: card.getAttribute('data-status') || '',
            haystack: (card.textContent || '').toLowerCase()
        };
    });
    var chips = Array.prototype.slice.call(document.querySelectorAll('.filter-chip'));
    var empty = document.getElementById('search-empty');
    var active = { mode: '', status: '' };
    var allowed = { mode: { '': true }, status: { '': true } };

    for (var i = 0; i < chips.length; i++) {
        var chip = chips[i];
        var group = chip.getAttribute('data-filter-group');
        var value = chip.getAttribute('data-filter-value') || '';
        if (!allowed[group]) {
            allowed[group] = { '': true };
        }
        allowed[group][value] = true;
    }

    function readStateFromParams() {
        var params = new URL(window.location.href).searchParams;
        var query = params.get('q') || '';
        var mode = params.get('mode') || '';
        var status = params.get('status') || '';
        input.value = query;
        active.mode = allowed.mode[mode] ? mode : '';
        active.status = allowed.status[status] ? status : '';
    }

    function syncParams() {
        var url = new URL(window.location.href);
        var query = input.value.trim();
        if (query) {
            url.searchParams.set('q', query);
        } else {
            url.searchParams.delete('q');
        }
        if (active.mode) {
            url.searchParams.set('mode', active.mode);
        } else {
            url.searchParams.delete('mode');
        }
        if (active.status) {
            url.searchParams.set('status', active.status);
        } else {
            url.searchParams.delete('status');
        }
        history.replaceState(null, '', url.toString());
    }

    function applyChipState() {
        for (var i = 0; i < chips.length; i++) {
            var chip = chips[i];
            var group = chip.getAttribute('data-filter-group');
            var value = chip.getAttribute('data-filter-value') || '';
            chip.classList.toggle('is-active', active[group] === value);
        }
    }

    function matchesForCard(card, query, modeValue, statusValue) {
        var matchQuery = !query || card.haystack.indexOf(query) !== -1;
        var matchMode = !modeValue || card.mode === modeValue;
        var matchStatus = !statusValue || card.status === statusValue;
        return matchQuery && matchMode && matchStatus;
    }

    function updateChipStates(query) {
        for (var i = 0; i < chips.length; i++) {
            var chip = chips[i];
            var group = chip.getAttribute('data-filter-group');
            var value = chip.getAttribute('data-filter-value') || '';
            var count = 0;

            for (var j = 0; j < cards.length; j++) {
                var card = cards[j];
                var modeValue = group === 'mode' ? value : active.mode;
                var statusValue = group === 'status' ? value : active.status;
                if (matchesForCard(card, query, modeValue, statusValue)) {
                    count += 1;
                }
            }

            var countNode = chip.querySelector('.chip-count');
            if (countNode) {
                countNode.textContent = value ? '(' + count + ')' : '';
            }

            var disabled = value && count === 0;
            chip.disabled = disabled;
            chip.classList.toggle('is-disabled', disabled);
        }
    }

    function filterCards(syncURL) {
        var query = input.value.trim().toLowerCase();
        var visible = 0;
        for (var i = 0; i < cards.length; i++) {
            var card = cards[i];
            var match = matchesForCard(card, query, active.mode, active.status);
            card.node.hidden = !match;
            if (match) {
                visible += 1;
            }
        }
        updateChipStates(query);
        if (empty) empty.hidden = !(visible === 0 && (query || active.mode || active.status));
        if (syncURL) {
            syncParams();
        }
    }

    for (var j = 0; j < chips.length; j++) {
        chips[j].addEventListener('click', function() {
            if (this.disabled) return;
            var group = this.getAttribute('data-filter-group');
            var value = this.getAttribute('data-filter-value') || '';
            active[group] = value;
            applyChipState();
            filterCards(true);
        });
    }

    input.addEventListener('input', function() {
        filterCards(true);
    });
    window.addEventListener('popstate', function() {
        readStateFromParams();
        applyChipState();
        filterCards(false);
    });

    readStateFromParams();
    applyChipState();
    filterCards(false);
}

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
        onSuccess(statusState.cache[name]);
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
        onSuccess(data);
    }).catch(function(err) {
        statusState.controllers[name] = null;
        if (err && err.name === 'AbortError') {
            return;
        }
        onFailure(err);
    });
}

function formatRelativeMinutes(seconds) {
    var t = window.I18N;
    var minutes = Math.round(seconds / 60);
    var hours = Math.round(seconds / 3600);
    if (seconds < 60) return t.just_now || 'just now';
    if (seconds < 3600) return (t.m_ago || '%dm ago').replace('%d', String(minutes));
    return (t.h_ago || '%dh ago').replace('%d', String(hours));
}

function formatDateTime(value) {
    if (!value) return '';
    var date = new Date(value);
    if (isNaN(date.getTime())) return value;
    var locale = document.documentElement.lang === 'zh' ? 'zh-CN' : 'en-US';
    return new Intl.DateTimeFormat(locale, {
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit'
    }).format(date);
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

function openStatusModal() {
    statusState.modal.hidden = false;
    document.body.classList.add('modal-open');
    loadStatusSummary();
    switchStatusTab(statusState.activeTab);
}

function closeStatusModal() {
    statusState.modal.hidden = true;
    document.body.classList.remove('modal-open');
    cancelStatusRequests();
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

function loadStatusSummary() {
    var target = document.getElementById('status-summary');
    var t = window.I18N;
    target.innerHTML = '<div class="status-summary-line">' + t.loading + '</div>';
    fetchStatus('summary', '/-/status/summary', function(data) {
        var lines = [];
        var statusText = data.healthy ? t.store_healthy : (t.store_degraded || '%d degraded').replace('%d', String(data.degraded_objects || 0));
        lines.push('<div class="status-summary-line"><strong>' + escapeHTML(statusText) + '</strong></div>');
        if (data.last_sample_at) {
            var ago = Math.max(0, Math.round((Date.now() - new Date(data.last_sample_at).getTime()) / 1000));
            lines.push('<div class="status-summary-line">' + escapeHTML((t.last_sample || 'Last sample') + ': ' + formatDateTime(data.last_sample_at) + ' · ' + formatRelativeMinutes(ago)) + '</div>');
        }
        lines.push('<div class="status-summary-line">' + escapeHTML((t.event_limit_note || 'Keeps the latest %d events').replace('%d', String(data.event_limit))) + '</div>');
        target.innerHTML = lines.join('');
    }, function() {
        target.innerHTML = '<div class="status-summary-line">' + escapeHTML(t.status_load_failed || 'Failed to load status') + '</div>';
    });
}

function loadDiskStatus() {
    var note = document.getElementById('disk-panel-note');
    var chart = document.getElementById('disk-chart');
    var t = window.I18N;
    note.textContent = t.loading;
    chart.className = 'disk-chart empty-state';
    chart.textContent = t.loading;
    fetchStatus('disk', '/-/status/disk', function(data) {
        var samples = data.samples || [];
        note.textContent = diskWindowNote(samples);
        if (!samples.length) {
            chart.textContent = t.no_data || 'No data';
            return;
        }
        renderDiskChart(chart, samples);
    }, function() {
        note.textContent = t.status_load_failed || 'Failed to load status';
        chart.textContent = t.status_load_failed || 'Failed to load status';
    });
}

function renderDiskChart(target, samples) {
    var t = window.I18N;
    var width = 760;
    var height = 260;
    var paddingX = 18;
    var paddingY = 22;
    var values = samples.map(function(item) { return item.total_bytes; });
    var min = Math.min.apply(Math, values);
    var max = Math.max.apply(Math, values);
    if (min === max) {
        min = Math.max(0, min - 1);
        max = max + 1;
    }
    var span = max - min;
    var step = values.length === 1 ? 0 : (width - paddingX * 2) / (values.length - 1);
    var points = [];
    for (var i = 0; i < values.length; i++) {
        var x = paddingX + step * i;
        var y = height - paddingY - ((values[i] - min) / span) * (height - paddingY * 2);
        points.push(x.toFixed(2) + ',' + y.toFixed(2));
    }
    var last = samples[samples.length - 1];
    target.className = 'disk-chart';
    target.innerHTML = '' +
        '<div class="chart-meta">' +
        '<strong>' + escapeHTML(formatBytes(last.total_bytes)) + '</strong>' +
        '<span>' + escapeHTML(formatDateTime(last.at)) + '</span>' +
        '</div>' +
        '<svg viewBox="0 0 ' + width + ' ' + height + '" aria-label="' + escapeHTML(t.disk_tab || 'Disk') + '">' +
        '<line class="chart-grid" x1="' + paddingX + '" y1="' + (height - paddingY) + '" x2="' + (width - paddingX) + '" y2="' + (height - paddingY) + '"></line>' +
        '<polyline class="chart-line" points="' + points.join(' ') + '"></polyline>' +
        '</svg>';
}

function loadEventStatus() {
    var note = document.getElementById('events-panel-note');
    var table = document.getElementById('events-table');
    var t = window.I18N;
    note.textContent = t.loading;
    table.className = 'empty-state';
    table.textContent = t.loading;
    fetchStatus('events', '/-/status/events?limit=' + eventRequestLimit(), function(data) {
        var events = data.events || [];
        var eventLimit = statusState.cache.summary && statusState.cache.summary.event_limit ? statusState.cache.summary.event_limit : events.length;
        note.textContent = (t.event_limit_note || 'Keeps the latest %d events').replace('%d', String(eventLimit));
        renderEventsTable(table, events);
    }, function() {
        note.textContent = t.status_load_failed || 'Failed to load status';
        table.textContent = t.status_load_failed || 'Failed to load status';
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
        return '' +
            '<tr>' +
            '<td title="' + escapeHTML(item.storage) + '">' + escapeHTML(item.storage) + '</td>' +
            '<td>' + escapeHTML(item.task_type) + '</td>' +
            '<td title="' + escapeHTML(item.target) + '"><span class="clip-cell">' + escapeHTML(item.target) + '</span></td>' +
            '<td>' + escapeHTML(formatDateTime(item.started_at)) + '</td>' +
            '<td>' + escapeHTML(formatDateTime(item.finished_at)) + '</td>' +
            '<td>' + escapeHTML(formatDurationMS(item.duration_ms)) + '</td>' +
            '<td>' + escapeHTML(item.result) + '</td>' +
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
        '</tr></thead>' +
        '<tbody>' + rows.join('') + '</tbody>' +
        '</table>';
}

function escapeHTML(value) {
    return String(value).replace(/[&<>"']/g, function(ch) {
        return {
            '&': '&amp;',
            '<': '&lt;',
            '>': '&gt;',
            '"': '&quot;',
            "'": '&#39;'
        }[ch];
    });
}

function initStatusModal() {
    statusState.modal = document.getElementById('status-modal');
    document.addEventListener('keydown', function(evt) {
        if (evt.key === 'Escape' && !statusState.modal.hidden) {
            closeStatusModal();
        }
    });
}

document.addEventListener('DOMContentLoaded', function() {
    if (!document.cookie.includes('theme=') && !window.location.search.includes('theme=')) {
        if (window.matchMedia('(prefers-color-scheme:dark)').matches) {
            document.documentElement.setAttribute('data-theme', 'dark');
        }
    }
    updateThemeBtn(document.documentElement.getAttribute('data-theme') || 'light');
    initSearch();
    initStatusModal();
});
