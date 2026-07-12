var SVG_NS = 'http://www.w3.org/2000/svg';

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

function networkSummaryCards(data) {
    var t = window.I18N;
    var s = data.summary || {};
    return [
        [t.network_active || 'Active upstream', String(s.active_upstream_requests || 0)],
        [t.network_hit_rate || 'Hit rate', formatPercent(s.hit_rate || 0)],
        [t.network_error_rate || 'Upstream errors', formatPercent(s.upstream_error_rate || 0)],
        [t.network_traffic || 'Upstream traffic', formatBytes(s.upstream_bytes || 0)],
        [t.network_degraded || 'Degraded upstreams', String(s.degraded_upstreams || 0)]
    ];
}

function renderNetworkPanelNote(target, data) {
    var state = target._networkNote;
    if (!state || !state.summary || state.summary.parentNode !== target) {
        target.textContent = '';
        var summary = document.createElement('div');
        summary.className = 'network-summary';
        var refresh = document.createElement('span');
        refresh.className = 'last-refresh';
        target.appendChild(summary);
        target.appendChild(refresh);
        state = { summary: summary, refresh: refresh, metrics: [] };
        target._networkNote = state;
    }
    var cards = networkSummaryCards(data);
    for (var i = 0; i < cards.length; i++) {
        var metric = state.metrics[i];
        if (!metric) {
            var root = document.createElement('div');
            root.className = 'network-metric';
            var label = document.createElement('span');
            var value = document.createElement('strong');
            root.appendChild(label);
            root.appendChild(value);
            state.summary.appendChild(root);
            metric = { root: root, label: label, value: value };
            state.metrics[i] = metric;
        }
        metric.label.textContent = cards[i][0];
        metric.value.textContent = cards[i][1];
    }
    while (state.metrics.length > cards.length) {
        var removed = state.metrics.pop();
        if (removed.root.parentNode) {
            removed.root.parentNode.removeChild(removed.root);
        }
    }
    var t = window.I18N;
    var stamp = formatDisplayTime(new Date(statusState.lastRefresh).toISOString());
    state.refresh.title = stamp.exact || '';
    state.refresh.textContent = (t.last_refreshed || 'Last refreshed') + ': ' + stamp.display;
}

function renderNetworkStageInsights(target, data, edges) {
    if (!target) {
        return;
    }
    var cards = networkStageInsightCards(data, edges);
    var state = target._networkStageInsights;
    if (!state || state.root !== target) {
        target.textContent = '';
        state = { root: target, cards: [] };
        target._networkStageInsights = state;
    }
    for (var i = 0; i < cards.length; i++) {
        var item = state.cards[i];
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
            state.cards[i] = item;
        }
        item.root.className = 'network-stage-card ' + cards[i].level;
        item.label.textContent = cards[i].label;
        item.value.textContent = cards[i].value;
        item.sub.textContent = cards[i].sub;
    }
    while (state.cards.length > cards.length) {
        var removed = state.cards.pop();
        if (removed.root.parentNode) {
            removed.root.parentNode.removeChild(removed.root);
        }
    }
}

function networkStageInsightCards(data, edges) {
    var t = window.I18N;
    var summary = data.summary || {};
    var upstreams = data.upstreams || [];
    var active = edges.reduce(function(total, edge) {
        return total + (edge.active_upstream_requests || 0);
    }, 0);
    var failed = edges.filter(function(edge) {
        return (edge.errors || 0) > 0 || edge.last_error || edge.state === 'open';
    }).length;
    var degraded = summary.degraded_upstreams || 0;
    var busiest = edges.slice().sort(function(a, b) {
        return (b.response_bytes || 0) - (a.response_bytes || 0) ||
            (b.requests || 0) - (a.requests || 0);
    })[0] || {};
    var busiestName = busiest.upstream_host || '-';
    var activeLevel = active > 0 ? 'is-live' : 'is-muted';
    var issueLevel = failed > 0 || degraded > 0 ? 'is-alert' : 'is-ok';
    return [
        {
            label: t.network_stage_active || 'Live flows',
            value: String(active),
            sub: String(edges.length) + ' ' + (t.network_links || 'links'),
            level: activeLevel
        },
        {
            label: t.network_stage_traffic || 'Traffic focus',
            value: formatBytes(summary.upstream_bytes || 0),
            sub: busiestName,
            level: busiest.response_bytes > 0 ? 'is-live' : 'is-muted'
        },
        {
            label: t.network_stage_topology || 'Topology',
            value: String(data.instances ? data.instances.length : 0) + ' / ' + String(upstreams.length),
            sub: (t.storage_name || 'Storage') + ' / ' + (t.upstream || 'Upstream'),
            level: 'is-info'
        },
        {
            label: t.network_stage_issues || 'Issues',
            value: String(failed + degraded),
            sub: String(degraded) + ' ' + (t.network_degraded || 'degraded'),
            level: issueLevel
        }
    ];
}

function formatPercent(value) {
    return (value * 100).toFixed(value > 0 && value < 0.1 ? 1 : 0).replace(/\.0$/, '') + '%';
}

function renderNetworkMap(target, edges, data) {
    var t = window.I18N;
    if (!edges.length) {
        target.className = 'network-map empty-state';
        target.removeAttribute('data-focus-id');
        target.textContent = t.no_data || 'No data';
        target._networkGraph = null;
        return;
    }

    var graph = ensureNetworkGraph(target);
    var layout = networkLayout(target, edges, data);
    graph.svg.setAttribute('viewBox', '0 0 ' + layout.width + ' ' + layout.height);
    graph.svg.setAttribute('preserveAspectRatio', 'xMidYMid meet');
    target.className = 'network-map';
    target.classList.toggle('is-stage', document.body.classList.contains('network-stage-open'));

    syncNetworkEdges(graph, layout);
    syncNetworkNodes(graph, layout);
    applyNetworkFocus(target, target.getAttribute('data-focus-id') || '');
    target.classList.add('is-fade-in');
}

function ensureNetworkGraph(target) {
    if (target._networkGraph && target._networkGraph.svg && target._networkGraph.svg.parentNode === target) {
        return target._networkGraph;
    }
    target.textContent = '';
    var svg = document.createElementNS(SVG_NS, 'svg');
    svg.setAttribute('aria-label', (window.I18N.network_tab || 'Network'));
    svg.setAttribute('role', 'img');
    var edgeLayer = document.createElementNS(SVG_NS, 'g');
    edgeLayer.setAttribute('class', 'network-edge-layer');
    var labelLayer = document.createElementNS(SVG_NS, 'g');
    labelLayer.setAttribute('class', 'network-edge-label-layer');
    var nodeLayer = document.createElementNS(SVG_NS, 'g');
    nodeLayer.setAttribute('class', 'network-node-layer');
    svg.appendChild(edgeLayer);
    svg.appendChild(labelLayer);
    svg.appendChild(nodeLayer);
    target.appendChild(svg);
    target._networkGraph = {
        svg: svg,
        edgeLayer: edgeLayer,
        labelLayer: labelLayer,
        nodeLayer: nodeLayer,
        edges: {},
        labels: {},
        packets: {},
        nodes: {}
    };
    wireNetworkMapFocus(target);
    return target._networkGraph;
}

function networkLayout(target, edges, data) {
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

    var stage = document.body.classList.contains('network-stage-open');
    var width = Math.max(stage ? 1280 : 860, target.clientWidth ? target.clientWidth - (stage ? 16 : 28) : 0);
    var rowCount = Math.max(instances.length, upstreams.length, 1);
    var height = Math.max(
        stage ? 560 : 280,
        target.clientHeight ? target.clientHeight - (stage ? 16 : 0) : 0,
        rowCount * (stage ? 104 : 76) + 74
    );
    var proxy = { x: stage ? 104 : 86, y: height / 2 };
    var instX = Math.round(width * (stage ? 0.34 : 0.38));
    var upX = width - (stage ? 176 : 140);
    var instancePos = {};
    var upstreamPos = {};
    instances.forEach(function(instance, idx) {
        instancePos[instance.id] = { x: instX, y: laneY(idx, instances.length, height) };
    });
    upstreams.forEach(function(upstream, idx) {
        upstreamPos[upstream.id] = { x: upX, y: laneY(idx, upstreams.length, height) };
    });

    var maxReq = Math.max.apply(Math, edges.map(function(edge) { return edge.requests || 1; }));
    var edgeModels = instances.map(function(instance) {
        var p = instancePos[instance.id];
        return {
            key: 'proxy:' + instance.id,
            from: 'proxy:cache',
            to: instance.id,
            state: 'closed',
            active: 0,
            width: 2,
            path: curvePath(proxy, p),
            label: instance.mode || '',
            title: (window.I18N.title || 'Cache Proxy') + ' -> ' + instance.name,
            midpoint: midpoint(proxy, p),
            hash: instance.mode + ':' + instance.requests
        };
    });

    edges.forEach(function(edge) {
        var a = instancePos[edge.from];
        var b = upstreamPos[edge.to];
        if (!a || !b) return;
        var strokeWidth = 1.6 + Math.min(8, ((edge.requests || 1) / maxReq) * 7);
        var edgeHash = [
            edge.state, edge.active_upstream_requests, edge.requests, edge.errors,
            edge.response_bytes, edge.last_status, edge.last_error, edge.latency_ms
        ].join(':');
        edgeModels.push({
            key: edge.id || (edge.from + '->' + edge.to + ':' + edge.upstream_url),
            from: edge.from,
            to: edge.to,
            state: edge.state || 'unknown',
            active: edge.active_upstream_requests || 0,
            traffic: trafficLevel(edge),
            width: strokeWidth,
            path: curvePath(a, b),
            label: edgeMetricsText(edge),
            title: edgeTitle(edge),
            midpoint: midpoint(a, b),
            hash: edgeHash
        });
    });

    var nodeModels = [{
        key: 'proxy:cache',
        x: proxy.x,
        y: proxy.y,
        kind: 'proxy',
        label: window.I18N.title || 'Cache Proxy',
        sub: String(edges.length) + ' ' + (window.I18N.network_links || 'links'),
        state: 'closed',
        hash: 'proxy:' + edges.length
    }];
    instances.forEach(function(instance) {
        var p = instancePos[instance.id];
        nodeModels.push({
            key: instance.id,
            x: p.x,
            y: p.y,
            kind: 'instance',
            label: instance.name,
            sub: networkInstanceSubtitle(instance),
            state: instance.active_upstream_requests > 0 ? 'halfopen' : 'closed',
            hash: [
                instance.mode, instance.requests, instance.upstream_requests,
                instance.active_upstream_requests, instance.hit_rate
            ].join(':')
        });
    });
    upstreams.forEach(function(upstream) {
        var p = upstreamPos[upstream.id];
        nodeModels.push({
            key: upstream.id,
            x: p.x,
            y: p.y,
            kind: 'upstream',
            label: upstream.host,
            sub: networkUpstreamNodeSubtitle(upstream),
            state: upstream.state || 'unknown',
            hash: [
                upstream.state, upstream.active_upstream_requests, upstream.requests,
                upstream.errors, upstream.weight, upstream.latency_ms
            ].join(':')
        });
    });

    return { width: width, height: height, edges: edgeModels, nodes: nodeModels };
}

function syncNetworkEdges(graph, layout) {
    var seen = {};
    layout.edges.forEach(function(model) {
        seen[model.key] = true;
        var pathNode = graph.edges[model.key];
        if (!pathNode) {
            pathNode = document.createElementNS(SVG_NS, 'path');
            pathNode.setAttribute('data-edge-key', model.key);
            graph.edgeLayer.appendChild(pathNode);
            graph.edges[model.key] = pathNode;
        }
        updateEdgePath(pathNode, model);

        var labelNode = graph.labels[model.key];
        if (!labelNode) {
            labelNode = document.createElementNS(SVG_NS, 'text');
            labelNode.setAttribute('class', 'network-edge-label');
            labelNode.setAttribute('text-anchor', 'middle');
            graph.labelLayer.appendChild(labelNode);
            graph.labels[model.key] = labelNode;
        }
        updateEdgeLabel(labelNode, model);

        var packetNode = graph.packets[model.key];
        if (!packetNode) {
            packetNode = createTrafficPacket();
            graph.edgeLayer.appendChild(packetNode);
            graph.packets[model.key] = packetNode;
        }
        updatePacket(packetNode, model);
    });
    removeMissingNetworkItems(graph.edges, seen);
    removeMissingNetworkItems(graph.labels, seen);
    removeMissingNetworkItems(graph.packets, seen);
}

function updateEdgePath(pathNode, model) {
    var cls = 'network-edge state-' + safeState(model.state) + (model.active > 0 ? ' is-active' : '');
    pathNode.setAttribute('class', cls);
    pathNode.setAttribute('data-edge-from', model.from);
    pathNode.setAttribute('data-edge-to', model.to);
    pathNode.setAttribute('data-edge-hash', model.hash);
    pathNode.setAttribute('d', model.path);
    pathNode.setAttribute('stroke-width', model.width.toFixed(1));
    setSVGTitle(pathNode, model.title);
    flashIfChanged(pathNode, model.hash);
}

function updateEdgeLabel(labelNode, model) {
    labelNode.setAttribute('data-edge-from', model.from);
    labelNode.setAttribute('data-edge-to', model.to);
    labelNode.setAttribute('x', model.midpoint.x);
    labelNode.setAttribute('y', model.midpoint.y - 8);
    labelNode.textContent = model.label;
    labelNode.classList.toggle('is-active', model.active > 0);
}

function updatePacket(packetNode, model) {
    packetNode.setAttribute('data-edge-from', model.from);
    packetNode.setAttribute('data-edge-to', model.to);
    packetNode.setAttribute('class', 'network-packet traffic-' + (model.traffic || 'idle'));
    packetNode.classList.toggle('is-active', model.active > 0);
    var duration = model.active >= 8 ? '1.05s' : (model.active >= 3 ? '1.35s' : '1.7s');
    var motions = packetNode.querySelectorAll('animateMotion');
    motions.forEach(function(motion) {
        motion.setAttribute('path', model.path);
        motion.setAttribute('dur', duration);
    });
}

function createTrafficPacket() {
    var group = document.createElementNS(SVG_NS, 'g');
    group.setAttribute('class', 'network-packet');
    ['0s', '-0.48s', '-0.96s'].forEach(function(begin, idx) {
        var dot = document.createElementNS(SVG_NS, 'circle');
        var motion = document.createElementNS(SVG_NS, 'animateMotion');
        dot.setAttribute('r', idx === 0 ? '4.2' : '3.1');
        motion.setAttribute('begin', begin);
        motion.setAttribute('repeatCount', 'indefinite');
        motion.setAttribute('rotate', 'auto');
        dot.appendChild(motion);
        group.appendChild(dot);
    });
    return group;
}

function syncNetworkNodes(graph, layout) {
    var seen = {};
    layout.nodes.forEach(function(model) {
        seen[model.key] = true;
        var node = graph.nodes[model.key];
        if (!node) {
            node = createNetworkNode(model.key);
            graph.nodeLayer.appendChild(node);
            graph.nodes[model.key] = node;
        }
        updateNetworkNode(node, model);
    });
    removeMissingNetworkItems(graph.nodes, seen);
}

function createNetworkNode(key) {
    var node = document.createElementNS(SVG_NS, 'g');
    node.setAttribute('class', 'network-node');
    node.setAttribute('data-node-id', key);
    var title = document.createElementNS(SVG_NS, 'title');
    var circle = document.createElementNS(SVG_NS, 'circle');
    var label = document.createElementNS(SVG_NS, 'text');
    var sub = document.createElementNS(SVG_NS, 'text');
    circle.setAttribute('r', '22');
    label.setAttribute('class', 'node-label');
    label.setAttribute('x', '0');
    label.setAttribute('y', '-30');
    label.setAttribute('text-anchor', 'middle');
    sub.setAttribute('class', 'node-sub');
    sub.setAttribute('x', '0');
    sub.setAttribute('y', '40');
    sub.setAttribute('text-anchor', 'middle');
    node.appendChild(title);
    node.appendChild(circle);
    node.appendChild(label);
    node.appendChild(sub);
    return node;
}

function updateNetworkNode(node, model) {
    node.setAttribute('class', 'network-node node-' + model.kind + ' state-' + safeState(model.state) +
        (model.active ? ' is-active' : ''));
    node.setAttribute('data-node-id', model.key);
    node.setAttribute('data-node-hash', model.hash);
    node.setAttribute('transform', 'translate(' + model.x + ' ' + model.y + ')');
    node.querySelector('title').textContent = model.label;
    node.querySelector('.node-label').textContent = compactNetworkLabel(model.label);
    node.querySelector('.node-sub').textContent = model.sub || '';
    flashIfChanged(node, model.hash);
}

function removeMissingNetworkItems(items, seen) {
    Object.keys(items).forEach(function(key) {
        if (seen[key]) return;
        if (items[key].parentNode) {
            items[key].parentNode.removeChild(items[key]);
        }
        delete items[key];
    });
}

function setSVGTitle(node, text) {
    var title = node.querySelector('title');
    if (!title) {
        title = document.createElementNS(SVG_NS, 'title');
        node.appendChild(title);
    }
    title.textContent = text || '';
}

function flashIfChanged(node, hash) {
    if (node._networkHash === undefined) {
        node._networkHash = hash;
        return;
    }
    if (node._networkHash === hash) {
        return;
    }
    node._networkHash = hash;
    node.classList.remove('is-updated');
    void node.getBoundingClientRect();
    node.classList.add('is-updated');
}

function edgeMetricsText(edge) {
    var parts = [];
    if ((edge.active_upstream_requests || 0) > 0) {
        parts.push(String(edge.active_upstream_requests) + ' ' + (window.I18N.network_active_short || 'active'));
    }
    parts.push(String(edge.requests || 0) + ' ' + (window.I18N.requests_short || 'req'));
    if ((edge.errors || 0) > 0) {
        parts.push(String(edge.errors) + ' ' + (window.I18N.errors || 'Errors'));
    }
    if (edge.last_status) {
        parts.push(edge.last_status);
    }
    if ((edge.latency_ms || 0) > 0) {
        parts.push(Math.round(edge.latency_ms) + 'ms');
    }
    return parts.join(' · ');
}

function edgeTitle(edge) {
    var used = formatDisplayTime(edge.last_used_at);
    return edge.instance + ' -> ' + edge.upstream_url +
        '\n' + (window.I18N.status || 'Status') + ': ' + translateUpstreamState(edge.state) +
        '\n' + (window.I18N.requests || 'Requests') + ': ' + String(edge.requests || 0) +
        '\n' + (window.I18N.errors || 'Errors') + ': ' + String(edge.errors || 0) +
        '\n' + (window.I18N.network_error_rate || 'Error rate') + ': ' + formatPercent(edge.error_rate || 0) +
        '\n' + (window.I18N.network_traffic || 'Traffic') + ': ' + formatBytes(edge.response_bytes || 0) +
        (edge.last_status ? '\n' + (window.I18N.last_status || 'Last status') + ': ' + edge.last_status : '') +
        (used.display ? '\n' + (window.I18N.last_used || 'Last used') + ': ' + used.display : '') +
        (edge.last_error ? '\n' + (window.I18N.last_error || 'Last error') + ': ' + edge.last_error : '');
}

function trafficLevel(edge) {
    var active = edge.active_upstream_requests || 0;
    var requests = edge.requests || 0;
    var bytes = edge.response_bytes || 0;
    if (active >= 8 || bytes >= 1024 * 1024 * 1024 || requests >= 10000) {
        return 'high';
    }
    if (active >= 3 || bytes >= 100 * 1024 * 1024 || requests >= 1000) {
        return 'medium';
    }
    if (active > 0 || requests > 0 || bytes > 0) {
        return 'low';
    }
    return 'idle';
}

function networkInstanceSubtitle(instance) {
    var active = instance.active_upstream_requests || 0;
    if (active > 0) {
        return String(active) + ' ' + (window.I18N.network_active_short || 'active');
    }
    return formatPercent(instance.hit_rate || 0) + ' · ' + String(instance.upstream_requests || 0) + ' ' +
        (window.I18N.requests_short || 'req');
}

function networkUpstreamNodeSubtitle(upstream) {
    var active = upstream.active_upstream_requests || 0;
    if (active > 0) {
        return String(active) + ' ' + (window.I18N.network_active_short || 'active');
    }
    var requests = upstream.requests || 0;
    if (requests > 0) {
        return String(requests) + ' ' + (window.I18N.requests_short || 'req') + ' · ' +
            formatPercent(upstream.error_rate || 0);
    }
    if (upstream.latency_ms > 0) {
        return Math.round(upstream.latency_ms) + 'ms';
    }
    return translateUpstreamState(upstream.state || 'unknown');
}

function laneY(index, total, height) {
    if (total <= 1) return height / 2;
    var top = 58;
    var bottom = height - 58;
    return top + ((bottom - top) * index / (total - 1));
}

function curvePath(a, b) {
    var dx = Math.max(86, Math.abs(b.x - a.x) * 0.34);
    return 'M' + a.x + ',' + a.y + ' C' + (a.x + dx) + ',' + a.y +
        ' ' + (b.x - dx) + ',' + b.y + ' ' + b.x + ',' + b.y;
}

function midpoint(a, b) {
    return {
        x: Math.round((a.x + b.x) / 2),
        y: Math.round((a.y + b.y) / 2)
    };
}

function compactNetworkLabel(label) {
    label = String(label || '');
    if (label.length <= 28) return label;
    return label.slice(0, 13) + '...' + label.slice(-12);
}

function safeState(state) {
    state = String(state || 'unknown').toLowerCase();
    return state.replace(/[^a-z0-9_-]/g, '') || 'unknown';
}

function wireNetworkMapFocus(target) {
    if (target._networkFocusBound) {
        return;
    }
    target._networkFocusBound = true;
    target.addEventListener('click', function(evt) {
        var node = evt.target.closest && evt.target.closest('.network-node');
        if (!node) {
            applyNetworkFocus(target, '');
            return;
        }
        var id = node.getAttribute('data-node-id') || '';
        applyNetworkFocus(target, target.getAttribute('data-focus-id') === id ? '' : id);
    });
    target.addEventListener('mouseover', function(evt) {
        if (target.getAttribute('data-focus-id')) return;
        var node = evt.target.closest && evt.target.closest('.network-node');
        if (node) applyNetworkFocus(target, node.getAttribute('data-node-id') || '');
    });
    target.addEventListener('mouseout', function(evt) {
        if (target.getAttribute('data-focus-id')) return;
        var node = evt.target.closest && evt.target.closest('.network-node');
        if (node && (!evt.relatedTarget || !node.contains(evt.relatedTarget))) {
            applyNetworkFocus(target, '');
        }
    });
}

function applyNetworkFocus(target, focusID) {
    target.setAttribute('data-focus-id', focusID || '');
    var edges = target.querySelectorAll('.network-edge,.network-edge-label,.network-packet');
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
        target._networkTable = null;
        return;
    }
    var tableState = ensureNetworkTable(target);
    var rows = edges.slice().sort(function(a, b) {
        return (b.active_upstream_requests || 0) - (a.active_upstream_requests || 0) ||
            (b.requests || 0) - (a.requests || 0);
    });
    var seen = {};
    rows.forEach(function(edge) {
        var key = edge.id || (edge.from + '->' + edge.to + ':' + edge.upstream_url);
        seen[key] = true;
        var row = tableState.rows[key];
        if (!row) {
            row = document.createElement('tr');
            row.setAttribute('data-network-row', key);
            for (var i = 0; i < 11; i++) {
                row.appendChild(document.createElement('td'));
            }
            tableState.tbody.appendChild(row);
            tableState.rows[key] = row;
        }
        updateNetworkTableRow(row, edge);
        tableState.tbody.appendChild(row);
    });
    Object.keys(tableState.rows).forEach(function(key) {
        if (seen[key]) return;
        tableState.rows[key].parentNode.removeChild(tableState.rows[key]);
        delete tableState.rows[key];
    });
    target.className = 'network-table-wrap';
    target.classList.add('is-fade-in');
}

function ensureNetworkTable(target) {
    if (target._networkTable && target._networkTable.table.parentNode === target) {
        return target._networkTable;
    }
    target.textContent = '';
    var table = document.createElement('table');
    table.className = 'status-table';
    var thead = document.createElement('thead');
    var tbody = document.createElement('tbody');
    var tr = document.createElement('tr');
    [
        window.I18N.storage_name || 'Storage',
        window.I18N.upstream || 'Upstream',
        window.I18N.status || 'Status',
        window.I18N.last_status || 'Last status',
        window.I18N.network_active || 'Active',
        window.I18N.requests || 'Requests',
        window.I18N.errors || 'Errors',
        window.I18N.network_error_rate || 'Error rate',
        window.I18N.network_traffic || 'Traffic',
        window.I18N.network_latency || 'Latency',
        window.I18N.last_used || 'Last used'
    ].forEach(function(label) {
        var th = document.createElement('th');
        th.textContent = label;
        tr.appendChild(th);
    });
    thead.appendChild(tr);
    table.appendChild(thead);
    table.appendChild(tbody);
    target.appendChild(table);
    target._networkTable = { table: table, tbody: tbody, rows: {} };
    return target._networkTable;
}

function updateNetworkTableRow(row, edge) {
    var used = formatDisplayTime(edge.last_used_at);
    var cells = row.children;
    row.title = edgeTitle(edge).replace(/\n/g, ' · ');
    setCellText(cells[0], edge.instance, edge.instance);
    setCellText(cells[1], edge.upstream_host, edge.upstream_url, true);
    setBadgeCell(cells[2], translateUpstreamState(edge.state), resultClass(edge.state));
    setCellText(cells[3], edge.last_status || '');
    setCellText(cells[4], String(edge.active_upstream_requests || 0));
    setCellText(cells[5], String(edge.requests || 0));
    setCellText(cells[6], String(edge.errors || 0));
    setCellText(cells[7], formatPercent(edge.error_rate || 0));
    setCellText(cells[8], formatBytes(edge.response_bytes || 0));
    setCellText(cells[9], (edge.latency_ms || 0).toFixed(0) + 'ms');
    setCellText(cells[10], used.display || '', used.exact || '');
}

function setCellText(cell, text, title, clipped) {
    title = title === undefined ? text : title;
    if (clipped) {
        var clip = cell.firstElementChild;
        if (!clip || !clip.classList.contains('clip-cell')) {
            cell.textContent = '';
            clip = document.createElement('span');
            clip.className = 'clip-cell';
            cell.appendChild(clip);
        }
        clip.textContent = text || '';
    } else {
        cell.textContent = text || '';
    }
    if (title) {
        cell.title = title;
    } else {
        cell.removeAttribute('title');
    }
}

function setBadgeCell(cell, text, className) {
    var badge = cell.firstElementChild;
    if (!badge || !badge.classList.contains('result-badge')) {
        cell.textContent = '';
        badge = document.createElement('span');
        cell.appendChild(badge);
    }
    badge.className = 'result-badge ' + className;
    badge.textContent = text || '';
}

function closeNetworkStage() {
    setNetworkStageOpen(false);
}

function refreshNetworkFromCache() {
    var cached = statusState.cache.network;
    if (!cached) {
        return;
    }
    var edges = filterNetworkEdges(cached.edges || [], activeNetworkFilter());
    renderNetworkStageInsights(document.getElementById('network-stage-insights'), cached, edges);
    renderNetworkMap(document.getElementById('network-map'), edges, cached);
    renderNetworkTable(document.getElementById('network-table'), edges);
}

function syncNetworkStageWithViewport() {
    setNetworkStageOpen(shouldOpenNetworkStage());
}

function setNetworkStageOpen(open) {
    var current = document.body.classList.contains('network-stage-open');
    if (current === open) {
        return;
    }
    document.body.classList.toggle('network-stage-open', open);
    if (statusState.modal) {
        statusState.modal.classList.toggle('network-stage-open', open);
    }
    var insights = document.getElementById('network-stage-insights');
    if (insights) {
        insights.setAttribute('aria-hidden', open ? 'false' : 'true');
    }
    refreshNetworkFromCache();
}

function shouldOpenNetworkStage() {
    return !!(statusState.modal &&
        statusState.modal.classList.contains('is-open') &&
        statusState.activeTab === 'network' &&
        browserLooksFullscreen());
}

function browserLooksFullscreen() {
    if (document.fullscreenElement) {
        return true;
    }
    if (!window.screen) {
        return false;
    }
    var widthMatch = Math.abs(window.innerWidth - window.screen.width) <= 2 ||
        Math.abs(window.outerWidth - window.screen.width) <= 2;
    var heightMatch = Math.abs(window.innerHeight - window.screen.height) <= 2 ||
        Math.abs(window.outerHeight - window.screen.height) <= 2;
    return widthMatch && heightMatch && window.innerWidth >= 900 && window.innerHeight >= 560;
}

function scheduleNetworkStageSync() {
    window.clearTimeout(scheduleNetworkStageSync._timer);
    scheduleNetworkStageSync._timer = window.setTimeout(syncNetworkStageWithViewport, 80);
}

document.addEventListener('fullscreenchange', scheduleNetworkStageSync);
window.addEventListener('resize', scheduleNetworkStageSync);
window.addEventListener('orientationchange', scheduleNetworkStageSync);
