var networkStageState = {
    state: 'idle',
    openedModal: false,
    timers: {},
    controllers: {},
    summary: null,
    disk: null,
    events: null,
    previousNetwork: null,
    previousNetworkAt: 0,
    network: null,
    networkAt: 0,
    nextNetworkRefreshAt: 0,
    hotspotIndex: -1,
    hotspot: null
};

function initNetworkStage() {
    var btn = document.getElementById('stage-btn');
    if (btn) {
        btn.addEventListener('click', function(evt) {
            evt.preventDefault();
            requestNetworkStage();
        });
    }
    document.addEventListener('fullscreenchange', handleNetworkStageFullscreenChange);
}

function requestNetworkStage() {
    var t = window.I18N;
    if (!statusState.modal || !statusState.modal.requestFullscreen) {
        showStageLaunchError(t.stage_fullscreen_unsupported || 'Fullscreen is not supported');
        return;
    }
    if (networkStageState.state === 'requesting' || networkStageState.state === 'active') {
        return;
    }

    networkStageState.state = 'requesting';
    networkStageState.openedModal = !statusState.modal.classList.contains('is-open');
    statusState.activeTab = 'network';
    openStatusModal();
    switchStatusTab('network');

    statusState.modal.requestFullscreen().then(function() {
        if (document.fullscreenElement === statusState.modal) {
            activateNetworkStage();
        }
    }).catch(function() {
        networkStageState.state = 'failed';
        showStageLaunchError(t.stage_fullscreen_failed || 'Fullscreen permission was denied');
        exitNetworkStage({ closeModal: networkStageState.openedModal });
    });
}

function handleNetworkStageFullscreenChange() {
    if (document.fullscreenElement === statusState.modal) {
        activateNetworkStage();
        return;
    }
    if (networkStageState.state === 'active' || networkStageState.state === 'requesting') {
        exitNetworkStage({ fromFullscreen: true, closeModal: true });
    }
}

function activateNetworkStage() {
    if (networkStageState.state === 'active') {
        return;
    }
    networkStageState.state = 'active';
    document.body.classList.add('network-stage-open');
    statusState.modal.classList.add('network-stage-open');
    setStageARIA(true);
    stopAutoRefresh();
    statusState.activeTab = 'network';
    switchStatusTab('network');
    refreshNetworkStageAll();
    startNetworkStageTimers();
}

function exitNetworkStage(opts) {
    opts = opts || {};
    if (networkStageState.state === 'idle') {
        return;
    }
    networkStageState.state = 'exiting';
    stopNetworkStageTimers();
    abortNetworkStageRequests();
    document.body.classList.remove('network-stage-open');
    if (statusState.modal) {
        statusState.modal.classList.remove('network-stage-open');
    }
    setStageARIA(false);
    setNetworkMapFocus('');
    networkStageState.hotspot = null;
    networkStageState.hotspotIndex = -1;

    var shouldExitFullscreen = !opts.fromFullscreen &&
        document.fullscreenElement === statusState.modal &&
        document.exitFullscreen;
    networkStageState.state = 'idle';
    if (shouldExitFullscreen) {
        document.exitFullscreen().catch(function() {});
    }
    if (opts.closeModal || networkStageState.openedModal) {
        networkStageState.openedModal = false;
        closeStatusModal(true);
    }
}

function setStageARIA(open) {
    [
        'network-stage-banner',
        'network-stage-insights',
        'network-stage-focus',
        'network-stage-trends'
    ].forEach(function(id) {
        var node = document.getElementById(id);
        if (node) {
            node.setAttribute('aria-hidden', open ? 'false' : 'true');
        }
    });
}

function startNetworkStageTimers() {
    stopNetworkStageTimers();
    networkStageState.timers.clock = window.setInterval(renderNetworkStageFromCache, 1000);
    networkStageState.timers.network = window.setInterval(refreshNetworkStageNetwork, 4000);
    networkStageState.timers.summary = window.setInterval(refreshNetworkStageSummary, 10000);
    networkStageState.timers.events = window.setInterval(refreshNetworkStageEvents, 8000);
    networkStageState.timers.disk = window.setInterval(refreshNetworkStageDisk, 20000);
    networkStageState.timers.hotspot = window.setInterval(advanceNetworkStageHotspot, 7000);
}

function stopNetworkStageTimers() {
    Object.keys(networkStageState.timers).forEach(function(key) {
        window.clearInterval(networkStageState.timers[key]);
    });
    networkStageState.timers = {};
}

function abortNetworkStageRequests() {
    Object.keys(networkStageState.controllers).forEach(function(key) {
        networkStageState.controllers[key].abort();
    });
    networkStageState.controllers = {};
}

function refreshNetworkStageAll() {
    refreshNetworkStageSummary();
    refreshNetworkStageNetwork();
    refreshNetworkStageDisk();
    refreshNetworkStageEvents();
    renderNetworkStageFromCache();
}

function refreshNetworkStageSummary() {
    fetchNetworkStageJSON('summary', '/-/status/summary', function(data) {
        networkStageState.summary = data;
        statusState.cache.summary = data;
        renderNetworkStageFromCache();
    });
}

function refreshNetworkStageNetwork() {
    networkStageState.nextNetworkRefreshAt = Date.now() + 4000;
    fetchNetworkStageJSON('network', '/-/status/network', function(data) {
        networkStageState.previousNetwork = networkStageState.network || statusState.cache.network || null;
        networkStageState.previousNetworkAt = networkStageState.networkAt || Date.now();
        networkStageState.network = data;
        networkStageState.networkAt = Date.now();
        statusState.cache.network = data;
        renderNetworkMap(document.getElementById('network-map'), data.edges || [], data);
        renderNetworkTable(document.getElementById('network-table'), data.edges || []);
        ensureNetworkStageHotspot(true);
        renderNetworkStageFromCache();
    });
}

function refreshNetworkStageDisk() {
    fetchNetworkStageJSON('disk', '/-/status/disk', function(data) {
        networkStageState.disk = data;
        statusState.cache.disk = data;
        renderNetworkStageFromCache();
    });
}

function refreshNetworkStageEvents() {
    fetchNetworkStageJSON('events', '/-/status/events?limit=' + eventRequestLimit(), function(data) {
        networkStageState.events = data;
        statusState.cache.events = data;
        renderNetworkStageFromCache();
    });
}

function fetchNetworkStageJSON(key, url, onSuccess) {
    if (networkStageState.controllers[key]) {
        networkStageState.controllers[key].abort();
    }
    var controller = new AbortController();
    networkStageState.controllers[key] = controller;
    fetch(url, { signal: controller.signal }).then(function(resp) {
        if (!resp.ok) {
            throw new Error(resp.statusText || String(resp.status));
        }
        return resp.json();
    }).then(function(data) {
        if (networkStageState.controllers[key] === controller) {
            delete networkStageState.controllers[key];
        }
        onSuccess(data);
    }).catch(function(err) {
        if (networkStageState.controllers[key] === controller) {
            delete networkStageState.controllers[key];
        }
        if (!err || err.name !== 'AbortError') {
            renderNetworkStageFromCache();
        }
    });
}

function renderNetworkStageFromCache() {
    if (!isNetworkStageOpen()) {
        return;
    }
    var network = networkStageState.network || statusState.cache.network || {};
    var summary = network.summary || {};
    var disk = networkStageState.disk || statusState.cache.disk || {};
    var events = networkStageState.events || statusState.cache.events || {};
    renderNetworkStageBanner(summary);
    renderNetworkStageMetrics(network, disk);
    renderNetworkStageFocus(network, events);
    renderNetworkStageTrends(network, disk, events);
}

function renderNetworkStageBanner(summary) {
    var t = window.I18N;
    var now = formatDateTime(new Date().toISOString());
    var refreshed = networkStageState.networkAt ?
        formatDisplayTime(new Date(networkStageState.networkAt).toISOString()).display :
        (t.loading || 'Loading');
    var countdown = Math.max(0, Math.ceil((networkStageState.nextNetworkRefreshAt - Date.now()) / 1000));
    var healthy = (networkStageState.summary && networkStageState.summary.healthy) &&
        (summary.degraded_upstreams || 0) === 0 &&
        (summary.upstream_error_rate || 0) === 0;
    renderStageCards(document.getElementById('network-stage-banner'), [
        {
            label: t.stage_title || 'Network stage',
            value: t.title || 'Cache Proxy',
            sub: now,
            level: 'is-info'
        },
        {
            label: t.stage_health || 'Health',
            value: healthy ? (t.store_healthy || 'Healthy') : (t.degraded || 'Degraded'),
            sub: String(summary.degraded_upstreams || 0) + ' ' + (t.network_degraded || 'degraded'),
            level: healthy ? 'is-ok' : 'is-alert'
        },
        {
            label: t.stage_refreshed || 'Refreshed',
            value: refreshed,
            sub: (t.stage_next_refresh || 'Next refresh') + ': ' + countdown + 's',
            level: countdown <= 1 ? 'is-live' : 'is-muted'
        }
    ], 'network-stage-status-card');
}

function renderNetworkStageMetrics(network, disk) {
    var t = window.I18N;
    var summary = network.summary || {};
    var currentDisk = latestDiskBytes(disk);
    renderStageCards(document.getElementById('network-stage-insights'), [
        {
            label: t.network_active || 'Active',
            value: String(summary.active_upstream_requests || 0),
            sub: String(summary.active_downloads || 0) + ' ' + (t.stage_active_downloads || 'downloads'),
            level: (summary.active_upstream_requests || 0) > 0 ? 'is-live' : 'is-muted'
        },
        {
            label: t.requests || 'Requests',
            value: formatCompactNumber(summary.requests || 0),
            sub: (t.network_hit_rate || 'Hit rate') + ' ' + formatPercent(summary.hit_rate || 0),
            level: 'is-info'
        },
        {
            label: t.network_traffic || 'Traffic',
            value: formatBytes(summary.upstream_bytes || 0),
            sub: (t.network_error_rate || 'Error rate') + ' ' + formatPercent(summary.upstream_error_rate || 0),
            level: (summary.upstream_error_rate || 0) > 0 ? 'is-alert' : 'is-live'
        },
        {
            label: t.disk_usage || 'Disk',
            value: currentDisk > 0 ? formatBytes(currentDisk) : '-',
            sub: String(summary.degraded_upstreams || 0) + ' ' + (t.network_degraded || 'degraded'),
            level: (summary.degraded_upstreams || 0) > 0 ? 'is-alert' : 'is-ok'
        }
    ], 'network-stage-card');
}

function renderNetworkStageFocus(network, events) {
    var t = window.I18N;
    var edges = network.edges || [];
    var instances = network.instances || [];
    var hotspot = networkStageState.hotspot || chooseNetworkStageHotspots()[0] || {};
    var busiestEdge = topBy(edges, function(edge) { return edge.response_bytes || 0; }) || {};
    var busiestInstance = topBy(instances, function(item) { return item.requests || 0; }) || {};
    var errorEdge = topBy(edges, function(edge) { return (edge.errors || 0) + (edge.last_error ? 1 : 0); }) || {};
    var degradedList = degradedUpstreamList(network.upstreams || []);
    var latestEvent = latestStageEvent(events);
    renderStageCards(document.getElementById('network-stage-focus'), [
        {
            label: t.stage_hotspot || 'Hotspot',
            value: hotspot.title || '-',
            sub: hotspot.detail || '',
            level: hotspot.level || 'is-muted'
        },
        {
            label: t.stage_busiest_upstream || 'Busiest upstream',
            value: busiestEdge.upstream_host || '-',
            sub: formatBytes(busiestEdge.response_bytes || 0),
            level: busiestEdge.response_bytes > 0 ? 'is-live' : 'is-muted'
        },
        {
            label: t.stage_busiest_instance || 'Busiest instance',
            value: busiestInstance.name || '-',
            sub: formatCompactNumber(busiestInstance.requests || 0) + ' ' + (t.requests_short || 'req'),
            level: busiestInstance.requests > 0 ? 'is-info' : 'is-muted'
        },
        {
            label: t.stage_error_focus || 'Error focus',
            value: errorEdge.upstream_host || '-',
            sub: errorEdge.last_error || String(errorEdge.errors || 0) + ' ' + (t.errors || 'Errors'),
            level: errorEdge.errors > 0 || errorEdge.last_error ? 'is-alert' : 'is-ok'
        },
        {
            label: t.stage_degraded_list || 'Degraded upstreams',
            value: degradedList.value,
            sub: degradedList.sub,
            level: degradedList.value === '-' ? 'is-ok' : 'is-alert'
        },
        {
            label: t.stage_latest_event || 'Latest event',
            value: latestEvent.result || '-',
            sub: latestEvent.message || latestEvent.detail || latestEvent.task_type || '',
            level: resultClass(latestEvent.result || '')
        }
    ], 'network-stage-focus-card');
}

function renderNetworkStageTrends(network, disk, events) {
    var t = window.I18N;
    var summary = network.summary || {};
    var previousSummary = networkStageState.previousNetwork && networkStageState.previousNetwork.summary ?
        networkStageState.previousNetwork.summary : {};
    var seconds = Math.max(1, (networkStageState.networkAt - networkStageState.previousNetworkAt) / 1000);
    var diskDelta = diskBytesDelta(disk);
    var latestEvent = latestStageEvent(events);
    renderStageCards(document.getElementById('network-stage-trends'), [
        {
            label: t.stage_request_rate || 'Request rate',
            value: formatRate(summary.requests, previousSummary.requests, seconds),
            sub: t.requests || 'Requests',
            level: 'is-info'
        },
        {
            label: t.stage_traffic_rate || 'Traffic rate',
            value: formatBytesRate(summary.upstream_bytes, previousSummary.upstream_bytes, seconds),
            sub: t.network_traffic || 'Traffic',
            level: 'is-live'
        },
        {
            label: t.stage_error_rate_delta || 'Error rate',
            value: formatRate(summary.upstream_errors, previousSummary.upstream_errors, seconds),
            sub: t.errors || 'Errors',
            level: (summary.upstream_errors || 0) > (previousSummary.upstream_errors || 0) ? 'is-alert' : 'is-ok'
        },
        {
            label: t.stage_disk_delta || 'Disk delta',
            value: diskDelta === 0 ? '0 B' : formatSignedBytes(diskDelta),
            sub: t.disk_usage || 'Disk usage',
            level: diskDelta > 0 ? 'is-info' : 'is-muted'
        },
        {
            label: t.stage_scheduler || 'Scheduler',
            value: latestEvent.task_type ? translateTaskType(latestEvent.task_type) : '-',
            sub: latestEvent.result ? translateResult(latestEvent.result) : '',
            level: resultClass(latestEvent.result || '')
        }
    ], 'network-stage-trend-card');
}

function renderStageCards(target, cards, className) {
    if (!target) {
        return;
    }
    var state = target._stageCards;
    if (!state) {
        target.textContent = '';
        state = [];
        target._stageCards = state;
    }
    for (var i = 0; i < cards.length; i++) {
        var item = state[i];
        if (!item) {
            var root = document.createElement('div');
            var label = document.createElement('span');
            var value = document.createElement('strong');
            var sub = document.createElement('em');
            root.appendChild(label);
            root.appendChild(value);
            root.appendChild(sub);
            target.appendChild(root);
            item = { root: root, label: label, value: value, sub: sub };
            state[i] = item;
        }
        item.root.className = className + ' ' + (cards[i].level || '');
        item.label.textContent = cards[i].label || '';
        item.value.textContent = cards[i].value || '';
        item.sub.textContent = cards[i].sub || '';
    }
    while (state.length > cards.length) {
        var removed = state.pop();
        if (removed.root.parentNode) {
            removed.root.parentNode.removeChild(removed.root);
        }
    }
}

function advanceNetworkStageHotspot() {
    ensureNetworkStageHotspot(false);
    renderNetworkStageFromCache();
}

function ensureNetworkStageHotspot(forcePriority) {
    var hotspots = chooseNetworkStageHotspots();
    if (!hotspots.length) {
        networkStageState.hotspot = null;
        setNetworkMapFocus('');
        return;
    }
    if (forcePriority && hotspots[0].level === 'is-alert') {
        networkStageState.hotspotIndex = 0;
    } else {
        networkStageState.hotspotIndex = (networkStageState.hotspotIndex + 1) % hotspots.length;
    }
    networkStageState.hotspot = hotspots[networkStageState.hotspotIndex];
    setNetworkMapFocus(networkStageState.hotspot.focusID || '');
}

function chooseNetworkStageHotspots() {
    var network = networkStageState.network || statusState.cache.network || {};
    var previousEdges = {};
    ((networkStageState.previousNetwork && networkStageState.previousNetwork.edges) || []).forEach(function(edge) {
        previousEdges[edge.id || edge.upstream_url || ''] = edge;
    });
    return (network.edges || []).map(function(edge) {
        var key = edge.id || edge.upstream_url || '';
        var previous = previousEdges[key] || {};
        var deltaBytes = Math.max(0, (edge.response_bytes || 0) - (previous.response_bytes || 0));
        var score = deltaBytes / 1048576 + (edge.active_upstream_requests || 0) * 10 + (edge.errors || 0);
        var level = 'is-info';
        if (edge.last_error || edge.state === 'open' || edge.state === 'degraded') {
            score += 10000;
            level = 'is-alert';
        } else if (edge.state === 'halfopen') {
            score += 5000;
            level = 'is-alert';
        } else if ((edge.active_upstream_requests || 0) > 0) {
            score += 2000;
            level = 'is-live';
        } else if (edge.last_status && !String(edge.last_status).match(/^([23])/)) {
            score += 1000;
            level = 'is-alert';
        }
        return {
            score: score,
            level: level,
            focusID: edge.to || edge.from,
            title: edge.upstream_host || edge.upstream_url || edge.instance || '-',
            detail: (edge.instance || '-') + ' -> ' + (edge.last_status || translateUpstreamState(edge.state)) +
                ' · ' + formatBytes(edge.response_bytes || 0)
        };
    }).filter(function(item) {
        return item.score > 0;
    }).sort(function(a, b) {
        return b.score - a.score;
    }).slice(0, 8);
}

function topBy(items, scoreFn) {
    var best = null;
    var bestScore = -Infinity;
    (items || []).forEach(function(item) {
        var score = scoreFn(item);
        if (score > bestScore) {
            best = item;
            bestScore = score;
        }
    });
    return best;
}

function degradedUpstreamList(upstreams) {
    var degraded = (upstreams || []).filter(function(item) {
        return item.state && item.state !== 'closed' && item.state !== 'unknown';
    }).map(function(item) {
        return item.host;
    });
    if (!degraded.length) {
        return { value: '-', sub: window.I18N.store_healthy || 'Healthy' };
    }
    return {
        value: String(degraded.length),
        sub: degraded.slice(0, 3).join(', ') + (degraded.length > 3 ? ' +' + String(degraded.length - 3) : '')
    };
}

function latestStageEvent(events) {
    var items = events && events.events ? events.events : [];
    var latest = null;
    var latestAt = -Infinity;
    items.forEach(function(item) {
        var finishedAt = new Date(item.finished_at || item.started_at || '').getTime();
        if (!isNaN(finishedAt) && finishedAt >= latestAt) {
            latest = item;
            latestAt = finishedAt;
        }
    });
    return latest || (items.length ? items[items.length - 1] : {});
}

function latestDiskBytes(disk) {
    var samples = disk && disk.samples ? disk.samples : [];
    if (!samples.length) {
        return 0;
    }
    return samples[samples.length - 1].total_bytes || 0;
}

function diskBytesDelta(disk) {
    var samples = disk && disk.samples ? disk.samples : [];
    if (samples.length < 2) {
        return 0;
    }
    return (samples[samples.length - 1].total_bytes || 0) - (samples[samples.length - 2].total_bytes || 0);
}

function formatRate(current, previous, seconds) {
    return formatCompactNumber(Math.max(0, (current || 0) - (previous || 0)) / seconds) + '/s';
}

function formatBytesRate(current, previous, seconds) {
    return formatBytes(Math.max(0, (current || 0) - (previous || 0)) / seconds) + '/s';
}

function formatSignedBytes(value) {
    return (value > 0 ? '+' : '-') + formatBytes(Math.abs(value));
}

function formatCompactNumber(value) {
    var n = Number(value) || 0;
    if (n >= 1000000000) return (n / 1000000000).toFixed(1).replace(/\.0$/, '') + 'B';
    if (n >= 1000000) return (n / 1000000).toFixed(1).replace(/\.0$/, '') + 'M';
    if (n >= 1000) return (n / 1000).toFixed(1).replace(/\.0$/, '') + 'K';
    return String(Math.round(n));
}

function showStageLaunchError(message) {
    var btn = document.getElementById('stage-btn');
    if (!btn) {
        return;
    }
    if (!showStageLaunchError._html) {
        showStageLaunchError._html = btn.innerHTML;
    }
    btn.classList.add('stage-launch-failed');
    btn.textContent = message;
    window.clearTimeout(showStageLaunchError._timer);
    showStageLaunchError._timer = window.setTimeout(function() {
        btn.classList.remove('stage-launch-failed');
        btn.innerHTML = showStageLaunchError._html;
        showStageLaunchError._html = '';
    }, 1800);
}
