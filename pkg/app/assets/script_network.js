var SVG_NS = 'http://www.w3.org/2000/svg';

function formatPercent(value) {
    return (value * 100).toFixed(value > 0 && value < 0.1 ? 1 : 0).replace(/\.0$/, '') + '%';
}

function renderNetworkMap(target, edges, data) {
    var t = window.I18N;
    if (!edges.length) {
        target.classList.add('network-map', 'empty-state', 'network-empty-state');
        target.classList.toggle('is-stage', document.body.classList.contains('network-stage-open'));
        if (isNetworkStageOpen()) {
            target.classList.remove('is-fade-in');
        }
        target.removeAttribute('data-focus-id');
        if (target.textContent !== (t.no_data || 'No data')) {
            target.textContent = t.no_data || 'No data';
        }
        target._networkGraph = null;
        return;
    }

    var firstRender = !target._networkGraph;
    var graph = ensureNetworkGraph(target);
    var layout = networkLayout(target, edges, data);
    graph.svg.setAttribute('viewBox', '0 0 ' + layout.width + ' ' + layout.height);
    graph.svg.setAttribute('preserveAspectRatio', 'xMidYMid meet');
    target.classList.add('network-map');
    target.classList.remove('empty-state', 'network-empty-state');
    target.classList.toggle('is-stage', document.body.classList.contains('network-stage-open'));
    if (isNetworkStageOpen()) {
        target.classList.remove('is-fade-in');
    }

    syncNetworkEdges(graph, layout);
    syncNetworkNodes(graph, layout);
    applyNetworkFocus(target, target.getAttribute('data-focus-id') || '');
    if (firstRender && !isNetworkStageOpen()) {
        target.classList.add('is-fade-in');
    }
}

function ensureNetworkGraph(target) {
    if (target._networkGraph && target._networkGraph.svg && target._networkGraph.svg.parentNode === target) {
        return target._networkGraph;
    }
    target.textContent = '';
    var svg = document.createElementNS(SVG_NS, 'svg');
    svg.setAttribute('aria-label', (window.I18N.stage_title || window.I18N.title || 'Cache Proxy'));
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
        if (isNetworkStageOpen()) {
            return;
        }
        var node = evt.target.closest && evt.target.closest('.network-node');
        if (!node) {
            applyNetworkFocus(target, '');
            return;
        }
        var id = node.getAttribute('data-node-id') || '';
        applyNetworkFocus(target, target.getAttribute('data-focus-id') === id ? '' : id);
    });
    target.addEventListener('mouseover', function(evt) {
        if (isNetworkStageOpen()) {
            return;
        }
        if (target.getAttribute('data-focus-id')) return;
        var node = evt.target.closest && evt.target.closest('.network-node');
        if (node) applyNetworkFocus(target, node.getAttribute('data-node-id') || '');
    });
    target.addEventListener('mouseout', function(evt) {
        if (isNetworkStageOpen()) {
            return;
        }
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
        target.classList.add('network-table-wrap', 'empty-state', 'network-empty-state');
        if (isNetworkStageOpen()) {
            target.classList.remove('is-fade-in');
        }
        if (target.textContent !== (t.no_data || 'No data')) {
            target.textContent = t.no_data || 'No data';
        }
        target._networkList = null;
        return;
    }
    var firstRender = !target._networkList;
    var listState = ensureNetworkList(target);
    var rows = edges.slice().sort(function(a, b) {
        return networkNumberValue(b.active_upstream_requests) - networkNumberValue(a.active_upstream_requests) ||
            networkNumberValue(b.requests) - networkNumberValue(a.requests);
    });
    var seen = {};
    rows.forEach(function(edge) {
        var key = edge.id || (edge.from + '->' + edge.to + ':' + edge.upstream_url);
        seen[key] = true;
        var item = listState.rows[key];
        if (!item) {
            item = createNetworkListItem(key);
            listState.list.appendChild(item);
            listState.rows[key] = item;
        }
        updateNetworkListItem(item, edge);
        listState.list.appendChild(item);
    });
    Object.keys(listState.rows).forEach(function(key) {
        if (seen[key]) return;
        listState.rows[key].parentNode.removeChild(listState.rows[key]);
        delete listState.rows[key];
    });
    target.classList.add('network-table-wrap');
    target.classList.remove('empty-state', 'network-empty-state');
    if (isNetworkStageOpen()) {
        target.classList.remove('is-fade-in');
    }
    if (firstRender && !isNetworkStageOpen()) {
        target.classList.add('is-fade-in');
    }
}

function ensureNetworkList(target) {
    if (target._networkList && target._networkList.list.parentNode === target) {
        return target._networkList;
    }
    target.textContent = '';
    var list = document.createElement('div');
    list.className = 'network-link-list';
    target.appendChild(list);
    target._networkList = { list: list, rows: {} };
    return target._networkList;
}

function createNetworkListItem(key) {
    var item = document.createElement('article');
    item.className = 'network-link-item';
    item.setAttribute('data-network-row', key);

    var head = document.createElement('div');
    head.className = 'network-link-head';
    var title = document.createElement('strong');
    var badge = document.createElement('span');
    badge.className = 'result-badge';
    head.appendChild(title);
    head.appendChild(badge);

    var meta = document.createElement('div');
    meta.className = 'network-link-meta';
    var metrics = document.createElement('div');
    metrics.className = 'network-link-metrics';
    var issue = document.createElement('div');
    issue.className = 'network-link-issue';

    for (var i = 0; i < 4; i++) {
        metrics.appendChild(document.createElement('span'));
    }
    item.appendChild(head);
    item.appendChild(meta);
    item.appendChild(metrics);
    item.appendChild(issue);
    item._networkFields = {
        title: title,
        badge: badge,
        meta: meta,
        metrics: metrics.children,
        issue: issue
    };
    return item;
}

function updateNetworkListItem(item, edge) {
    var used = formatDisplayTime(edge.last_used_at);
    var fields = item._networkFields;
    var host = edge.upstream_host || edge.upstream_url || '-';
    item.title = edgeTitle(edge).replace(/\n/g, ' · ');
    fields.title.textContent = host;
    fields.badge.className = 'result-badge ' + resultClass(edge.state);
    fields.badge.textContent = translateUpstreamState(edge.state);
    fields.meta.textContent = (edge.instance || '-') + ' -> ' +
        (edge.last_status ? 'HTTP ' + edge.last_status : translateUpstreamState(edge.state)) +
        (used.display ? ' · ' + used.display : '');
    fields.metrics[0].textContent = (window.I18N.network_active || 'Active') + ' ' +
        String(edge.active_upstream_requests || 0);
    fields.metrics[1].textContent = (window.I18N.requests || 'Requests') + ' ' + String(edge.requests || 0);
    fields.metrics[2].textContent = (window.I18N.network_traffic || 'Traffic') + ' ' +
        formatBytes(edge.response_bytes || 0);
    fields.metrics[3].textContent = (window.I18N.network_latency || 'Latency') + ' ' +
        networkNumberValue(edge.latency_ms).toFixed(0) + 'ms';
    var lastError = String(edge.last_error || '').trim();
    var errors = networkNumberValue(edge.errors);
    var issue = lastError || (errors > 0 ?
        formatNetworkCompactNumber(errors) + ' ' + (window.I18N.errors || 'Errors') + ' · ' +
        formatPercent(edge.error_rate || 0) : '');
    fields.issue.textContent = issue;
    fields.issue.hidden = !issue;
}

function networkNumberValue(value) {
    var n = Number(value);
    return Number.isFinite(n) ? n : 0;
}

function formatNetworkCompactNumber(value) {
    var n = networkNumberValue(value);
    if (n >= 1000000000) return (n / 1000000000).toFixed(1).replace(/\.0$/, '') + 'B';
    if (n >= 1000000) return (n / 1000000).toFixed(1).replace(/\.0$/, '') + 'M';
    if (n >= 1000) return (n / 1000).toFixed(1).replace(/\.0$/, '') + 'K';
    return String(Math.round(n));
}

function closeNetworkStage() {
    if (typeof exitNetworkStage === 'function') {
        exitNetworkStage({ closeModal: false });
    }
}

function isNetworkStageOpen() {
    return document.body.classList.contains('network-stage-open');
}

function setNetworkMapFocus(focusID) {
    var map = document.getElementById('network-map');
    if (!map) {
        return;
    }
    applyNetworkFocus(map, focusID || '');
}
