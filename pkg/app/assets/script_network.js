var SVG_NS = 'http://www.w3.org/2000/svg';
var maxNetworkRelatedIDs = 80;
var maxNetworkVisibleUpstreams = 24;

function formatPercent(value) {
    return (value * 100).toFixed(value > 0 && value < 0.1 ? 1 : 0).replace(/\.0$/, '') + '%';
}

function formatErrorPercent(value) {
    var n = Number(value);
    if (!Number.isFinite(n)) {
        n = 0;
    }
    return (n * 100).toFixed(3) + '%';
}

function renderNetworkMap(target, edges, data) {
    var t = window.I18N;
    if (!edges.length) {
        target.classList.add('network-map', 'empty-state', 'network-empty-state');
        target.classList.remove('has-focus', 'is-fade-in', 'is-dense', 'is-clustered');
        target.classList.toggle('is-stage', document.body.classList.contains('network-stage-open'));
        target.removeAttribute('data-focus-id');
        if (target._networkGraph || target.textContent !== (t.no_data || 'No data')) {
            clearNetworkGraphTimers(target._networkGraph);
            target.textContent = t.no_data || 'No data';
        }
        target._networkGraph = null;
        target._networkTopology = null;
        return;
    }

    var firstRender = !target._networkGraph;
    var graph = ensureNetworkGraph(target);
    var previousData = networkPreviousData(target);
    var layout = networkLayout(target, edges, data, previousData);
    graph.svg.setAttribute('viewBox', '0 0 ' + layout.width + ' ' + layout.height);
    graph.svg.setAttribute('preserveAspectRatio', 'xMidYMid meet');
    target.classList.add('network-map');
    target.classList.remove('empty-state', 'network-empty-state');
    target.classList.toggle('is-stage', document.body.classList.contains('network-stage-open'));
    target.classList.toggle('is-dense', layout.mode === 'dense');
    target.classList.toggle('is-clustered', layout.mode === 'clustered');
    if (isNetworkStageOpen()) {
        target.classList.remove('is-fade-in');
    }

    syncNetworkEdges(graph, layout);
    syncNetworkNodes(graph, layout);
    applyNetworkFocus(target, target.getAttribute('data-focus-id') || '');
    target._networkData = data;
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

function clearNetworkGraphTimers(graph) {
    if (!graph) {
        return;
    }
    [graph.edges, graph.labels, graph.packets, graph.nodes].forEach(function(items) {
        Object.keys(items || {}).forEach(function(key) {
            window.clearTimeout(items[key]._networkUpdateTimer);
        });
    });
}

function networkTopologyState(target) {
    if (!target._networkTopology) {
        target._networkTopology = {
            instances: [],
            upstreams: [],
            clusteredUpstreams: []
        };
    }
    return target._networkTopology;
}

function networkPreviousData(target) {
    if (typeof networkStageState !== 'undefined' && networkStageState.previousNetwork) {
        return networkStageState.previousNetwork;
    }
    return target._networkData || null;
}

function networkPreviousEdges(previousData) {
    var previousEdges = {};
    ((previousData && previousData.edges) || []).forEach(function(edge) {
        previousEdges[networkEdgeKey(edge)] = edge;
    });
    return previousEdges;
}

function networkEdgeKey(edge) {
    if (!edge) {
        return '';
    }
    return edge.id || (edge.from + '->' + edge.to + ':' + edge.upstream_url);
}

function networkEdgeStats(edge, previous) {
    var active = networkNumberValue(edge.active_upstream_requests);
    var hasPrevious = !!previous && (previous.requests !== undefined || previous.response_bytes !== undefined);
    var requestDelta = hasPrevious ?
        Math.max(0, networkNumberValue(edge.requests) - networkNumberValue(previous.requests)) :
        networkNumberValue(edge.request_delta);
    var byteDelta = hasPrevious ?
        Math.max(0, networkNumberValue(edge.response_bytes) - networkNumberValue(previous.response_bytes)) :
        networkNumberValue(edge.byte_delta);
    var score = active * 1400 + Math.log1p(byteDelta / 65536) * 260 + Math.log1p(requestDelta) * 140;
    return {
        edge: edge,
        active: active,
        requestDelta: requestDelta,
        byteDelta: byteDelta,
        score: score
    };
}

function networkLayoutMode(instanceCount, upstreamCount) {
    var nodes = instanceCount + upstreamCount;
    if (nodes > 30 || upstreamCount > 24) {
        return 'clustered';
    }
    if (nodes > 12 || upstreamCount > 10) {
        return 'dense';
    }
    return 'simple';
}

function visibleNetworkEdges(edges, upstreams, upstreamByID, layoutMode, topology, edgeStatsByKey) {
    var result = { edges: edges.slice(), upstreams: upstreams.slice() };
    if (layoutMode !== 'clustered') {
        return result;
    }

    var upstreamIDs = {};
    upstreams.forEach(function(upstream) {
        upstreamIDs[upstream.id] = true;
    });
    var kept = {};
    var visibleIDs = [];
    (topology.clusteredUpstreams || []).forEach(function(id) {
        if (upstreamIDs[id] && visibleIDs.length < maxNetworkVisibleUpstreams) {
            kept[id] = true;
            visibleIDs.push(id);
        }
    });
    upstreams.forEach(function(upstream) {
        if (!kept[upstream.id] && visibleIDs.length < maxNetworkVisibleUpstreams) {
            kept[upstream.id] = true;
            visibleIDs.push(upstream.id);
        }
    });
    topology.clusteredUpstreams = visibleIDs;

    var visibleEdges = edges.filter(function(edge) {
        return !!kept[edge.to];
    });

    var hidden = edges.filter(function(edge) {
        return !kept[edge.to];
    });
    if (!hidden.length) {
        result.edges = visibleEdges;
        result.upstreams = visibleIDs.map(function(id) { return upstreamByID[id]; }).filter(Boolean);
        return result;
    }

    var group = groupedUpstream(hidden);
    upstreamByID[group.id] = group;
    var groupedByInstance = {};
    hidden.forEach(function(edge) {
        var hiddenStat = edgeStatsByKey[networkEdgeKey(edge)] || networkEdgeStats(edge, {});
        var key = edge.from || edge.instance || '';
        if (!groupedByInstance[key]) {
            groupedByInstance[key] = groupedNetworkEdge(edge, group.id, hiddenStat);
        } else {
            mergeGroupedNetworkEdge(groupedByInstance[key], edge, hiddenStat);
        }
    });
    Object.keys(groupedByInstance).forEach(function(key) {
        var edge = groupedByInstance[key];
        var stat = networkEdgeStats(edge, {});
        stat.requestDelta = edge.request_delta || 0;
        stat.byteDelta = edge.byte_delta || 0;
        stat.score = networkNumberValue(edge.active_upstream_requests) * 1400 +
            Math.log1p(stat.byteDelta / 65536) * 260 + Math.log1p(stat.requestDelta) * 140;
        edgeStatsByKey[networkEdgeKey(edge)] = stat;
        visibleEdges.push(edge);
    });
    result.edges = visibleEdges;
    result.upstreams = visibleIDs.map(function(id) { return upstreamByID[id]; }).filter(Boolean);
    result.upstreams.push(group);
    return result;
}

function groupedUpstream(hiddenEdges) {
    var count = {};
    hiddenEdges.forEach(function(edge) {
        count[edge.to] = true;
    });
    return {
        id: 'upstream:group:idle',
        host: '+' + String(Object.keys(count).length) + ' ' + (window.I18N.network_upstreams || 'upstreams'),
        hidden_count: Object.keys(count).length,
        state: groupedState(hiddenEdges),
        requests: sumNetworkField(hiddenEdges, 'requests'),
        errors: sumNetworkField(hiddenEdges, 'errors'),
        response_bytes: sumNetworkField(hiddenEdges, 'response_bytes'),
        active_upstream_requests: sumNetworkField(hiddenEdges, 'active_upstream_requests')
    };
}

function groupedNetworkEdge(edge, groupID, stat) {
    return {
        id: 'group:' + edge.from + '->' + groupID,
        from: edge.from,
        to: groupID,
        instance: edge.instance,
        upstream_url: groupID,
        upstream_host: '+ ' + (window.I18N.network_upstreams || 'upstreams'),
        hidden_count: 1,
        state: edge.state || 'unknown',
        requests: networkNumberValue(edge.requests),
        errors: networkNumberValue(edge.errors),
        response_bytes: networkNumberValue(edge.response_bytes),
        active_upstream_requests: networkNumberValue(edge.active_upstream_requests),
        request_delta: stat ? stat.requestDelta : 0,
        byte_delta: stat ? stat.byteDelta : 0,
        related: networkRelatedIDs(edge.to)
    };
}

function mergeGroupedNetworkEdge(grouped, edge, stat) {
    grouped.requests += networkNumberValue(edge.requests);
    grouped.errors += networkNumberValue(edge.errors);
    grouped.response_bytes += networkNumberValue(edge.response_bytes);
    grouped.active_upstream_requests += networkNumberValue(edge.active_upstream_requests);
    grouped.request_delta += stat ? stat.requestDelta : 0;
    grouped.byte_delta += stat ? stat.byteDelta : 0;
    grouped.hidden_count += 1;
    grouped.state = worseNetworkState(grouped.state, edge.state);
    appendNetworkRelatedID(grouped.related, edge.to);
}

function networkInstanceActivity(instances, edges, edgeStatsByKey) {
    var byID = {};
    instances.forEach(function(instance) {
        byID[instance.id] = emptyNetworkActivity();
        byID[instance.id].state = 'closed';
    });
    edges.forEach(function(edge) {
        var activity = byID[edge.from];
        if (!activity) {
            return;
        }
        var stat = edgeStatsByKey[networkEdgeKey(edge)] || networkEdgeStats(edge, {});
        activity.active += stat.active;
        activity.requestDelta += stat.requestDelta;
        activity.byteDelta += stat.byteDelta;
        activity.score += stat.score;
        activity.state = worseNetworkState(activity.state, edge.state);
        appendNetworkRelatedID(activity.related, edge.to);
        (edge.related || []).forEach(function(id) {
            appendNetworkRelatedID(activity.related, id);
        });
    });
    return byID;
}

function networkRelatedIDs(id) {
    var related = [];
    appendNetworkRelatedID(related, id);
    return related;
}

function appendNetworkRelatedID(related, id) {
    if (!id || related.length >= maxNetworkRelatedIDs || related.indexOf(id) !== -1) {
        return;
    }
    related.push(id);
}

function emptyNetworkActivity() {
    return { active: 0, requestDelta: 0, byteDelta: 0, score: 0, state: 'closed', related: [] };
}

function networkEdgeWidth(activity) {
    var active = activity.active || 0;
    var bytes = activity.byteDelta || 0;
    var requests = activity.requestDelta || 0;
    if (active > 0) {
        return Math.min(9.5, 4.2 + Math.log1p(active) * 1.25 +
            Math.log1p(bytes / 1048576) * 0.42 + Math.log1p(requests) * 0.18);
    }
    if (bytes > 0 || requests > 0) {
        return Math.min(5.2, 2.2 + Math.log1p(bytes / 1048576) * 0.34 + Math.log1p(requests) * 0.16);
    }
    return 1.7;
}

function networkAnimationPhase(key) {
    var hash = 0;
    key = String(key || '');
    for (var i = 0; i < key.length; i++) {
        hash = (hash * 31 + key.charCodeAt(i)) % 9973;
    }
    return (hash % 1200) / 1000;
}

function networkAnimationDuration(activity) {
    var active = activity.active || 0;
    if (active >= 8) return 0.92;
    if (active >= 3) return 1.16;
    if ((activity.byteDelta || 0) > 0 || (activity.requestDelta || 0) > 0) return 1.42;
    return 1.7;
}

function networkNodeSize(layoutMode, kind) {
    if (kind === 'proxy') {
        return layoutMode === 'simple' ? 22 : 20;
    }
    if (kind === 'group') {
        return 19;
    }
    if (layoutMode === 'clustered') {
        return 17;
    }
    if (layoutMode === 'dense') {
        return 18.5;
    }
    return 22;
}

function worseNetworkState(a, b) {
    var ranks = { unknown: 0, closed: 1, halfopen: 2, degraded: 3, open: 4 };
    a = String(a || 'unknown').toLowerCase();
    b = String(b || 'unknown').toLowerCase();
    return (ranks[b] || 0) > (ranks[a] || 0) ? b : a;
}

function groupedState(edges) {
    return edges.reduce(function(state, edge) {
        return worseNetworkState(state, edge.state);
    }, 'closed');
}

function sumNetworkField(items, field) {
    return items.reduce(function(sum, item) {
        return sum + networkNumberValue(item[field]);
    }, 0);
}

function stableNetworkItems(topology, key, items, itemID) {
    var byID = {};
    var preferred = [];
    items.forEach(function(item) {
        var id = itemID(item);
        if (!id || byID[id]) {
            return;
        }
        byID[id] = item;
        preferred.push(id);
    });

    var nextOrder = [];
    var seen = {};
    (topology[key] || []).forEach(function(id) {
        if (!byID[id] || seen[id]) {
            return;
        }
        seen[id] = true;
        nextOrder.push(id);
    });
    preferred.forEach(function(id) {
        if (seen[id]) {
            return;
        }
        seen[id] = true;
        nextOrder.push(id);
    });
    topology[key] = nextOrder;
    return nextOrder.map(function(id) { return byID[id]; }).filter(Boolean);
}

function networkLayout(target, edges, data, previousData) {
    var topology = networkTopologyState(target);
    var previousEdges = networkPreviousEdges(previousData);
    var edgeStats = edges.map(function(edge) {
        return networkEdgeStats(edge, previousEdges[networkEdgeKey(edge)]);
    });
    var edgeStatsByKey = {};
    edgeStats.forEach(function(stat) {
        edgeStatsByKey[networkEdgeKey(stat.edge)] = stat;
    });

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
    instances = stableNetworkItems(topology, 'instances', instances, function(instance) {
        return instance.id;
    });
    upstreams = stableNetworkItems(topology, 'upstreams', upstreams, function(upstream) {
        return upstream.id;
    });

    var stage = document.body.classList.contains('network-stage-open');
    var layoutMode = networkLayoutMode(instances.length, upstreams.length);
    var visible = visibleNetworkEdges(edges, upstreams, upstreamByID, layoutMode, topology, edgeStatsByKey);
    upstreams = visible.upstreams;
    var visibleEdges = visible.edges;
    var instanceActivity = networkInstanceActivity(instances, visibleEdges, edgeStatsByKey);
    var width = Math.max(stage ? 1280 : 860, target.clientWidth ? target.clientWidth - (stage ? 16 : 28) : 0);
    var rowGap = layoutMode === 'simple' ? (stage ? 104 : 76) : (layoutMode === 'dense' ? (stage ? 74 : 62) : (stage ? 68 : 58));
    var rowCount = Math.max(instances.length, upstreams.length, 1);
    var height = Math.max(
        stage ? 560 : 280,
        target.clientHeight ? target.clientHeight - (stage ? 16 : 0) : 0,
        rowCount * rowGap + 74
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

    var quietLabels = layoutMode !== 'simple';
    var edgeModels = instances.map(function(instance) {
        var p = instancePos[instance.id];
        var activity = instanceActivity[instance.id] || emptyNetworkActivity();
        var active = activity.active;
        var edgeKey = 'proxy:' + instance.id;
        return {
            key: edgeKey,
            from: 'proxy:cache',
            to: instance.id,
            related: activity.related,
            state: activity.state,
            active: active,
            moving: active > 0 || activity.score > 0,
            traffic: trafficLevel(activity),
            width: networkEdgeWidth(activity),
            phase: networkAnimationPhase(edgeKey),
            duration: networkAnimationDuration(activity),
            path: curvePath(proxy, p),
            label: instanceLinkLabel(instance, activity),
            quiet: quietLabels && active <= 0 && activity.score <= 0,
            title: instanceLinkTitle(instance, activity),
            midpoint: midpoint(proxy, p),
            hash: [
                instance.mode, instance.requests, activity.active, activity.requestDelta,
                activity.byteDelta, activity.state
            ].join(':')
        };
    });

    visibleEdges.forEach(function(edge) {
        var a = instancePos[edge.from];
        var b = upstreamPos[edge.to];
        if (!a || !b) return;
        var stat = edgeStatsByKey[networkEdgeKey(edge)] || networkEdgeStats(edge, {});
        var edgeKey = edge.id || (edge.from + '->' + edge.to + ':' + edge.upstream_url);
        var edgeHash = [
            edge.state, edge.active_upstream_requests, edge.requests, stat.requestDelta,
            edge.errors, edge.response_bytes, stat.byteDelta, edge.last_status,
            edge.last_error, edge.latency_ms
        ].join(':');
        edgeModels.push({
            key: edgeKey,
            from: edge.from,
            to: edge.to,
            related: edge.related || [],
            state: edge.state || 'unknown',
            active: stat.active,
            moving: stat.active > 0 || stat.score > 0,
            traffic: trafficLevel(stat),
            width: networkEdgeWidth(stat),
            phase: networkAnimationPhase(edgeKey),
            duration: networkAnimationDuration(stat),
            path: curvePath(a, b),
            label: edgeMetricsText(edge, stat),
            quiet: quietLabels && stat.active <= 0 && stat.score <= 0 && !networkEdgeIsDegraded(edge),
            title: edgeTitle(edge, stat),
            midpoint: midpoint(a, b),
            hash: edgeHash
        });
    });

    var nodeModels = [{
        key: 'proxy:cache',
        x: proxy.x,
        y: proxy.y,
        size: networkNodeSize(layoutMode, 'proxy'),
        kind: 'proxy',
        label: window.I18N.title || 'Cache Proxy',
        sub: String(edges.length) + ' ' + (window.I18N.network_links || 'links'),
        state: 'closed',
        hash: 'proxy:' + edges.length + ':' + layoutMode
    }];
    instances.forEach(function(instance) {
        var p = instancePos[instance.id];
        nodeModels.push({
            key: instance.id,
            x: p.x,
            y: p.y,
            size: networkNodeSize(layoutMode, 'instance'),
            kind: 'instance',
            label: instance.name,
            sub: networkInstanceSubtitle(instance),
            active: instance.active_upstream_requests > 0,
            state: 'closed',
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
            size: networkNodeSize(layoutMode, upstream.id.indexOf('upstream:group:') === 0 ? 'group' : 'upstream'),
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

    return { width: width, height: height, edges: edgeModels, nodes: nodeModels, mode: layoutMode };
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
    var cls = 'network-edge state-' + safeState(model.state) + (model.moving ? ' is-active' : '');
    pathNode.setAttribute('class', cls);
    pathNode.setAttribute('data-edge-from', model.from);
    pathNode.setAttribute('data-edge-to', model.to);
    pathNode.setAttribute('data-edge-related', (model.related || []).join(' '));
    pathNode.setAttribute('data-edge-hash', model.hash);
    pathNode.setAttribute('d', model.path);
    pathNode.setAttribute('stroke-width', model.width.toFixed(1));
    pathNode.style.animationDuration = model.moving ? model.duration.toFixed(2) + 's' : '';
    pathNode.style.animationDelay = model.moving ? '-' + model.phase.toFixed(2) + 's' : '';
    setSVGTitle(pathNode, model.title);
    flashIfChanged(pathNode, model.hash);
}

function updateEdgeLabel(labelNode, model) {
    labelNode.setAttribute('data-edge-from', model.from);
    labelNode.setAttribute('data-edge-to', model.to);
    labelNode.setAttribute('data-edge-related', (model.related || []).join(' '));
    labelNode.setAttribute('x', model.midpoint.x);
    labelNode.setAttribute('y', model.midpoint.y - 8);
    labelNode.textContent = model.label;
    labelNode.classList.toggle('is-active', !!model.moving);
    labelNode.classList.toggle('is-quiet', !!model.quiet);
}

function updatePacket(packetNode, model) {
    packetNode.setAttribute('data-edge-from', model.from);
    packetNode.setAttribute('data-edge-to', model.to);
    packetNode.setAttribute('data-edge-related', (model.related || []).join(' '));
    packetNode.setAttribute('class', 'network-packet traffic-' + (model.traffic || 'idle'));
    packetNode.classList.toggle('is-active', !!model.moving);
    var motions = packetNode.querySelectorAll('animateMotion');
    motions.forEach(function(motion) {
        motion.setAttribute('path', model.path);
        motion.setAttribute('dur', model.duration.toFixed(2) + 's');
        motion.setAttribute('begin', networkPacketBegin(motion.getAttribute('data-base-begin'), model.phase));
    });
}

function networkPacketBegin(base, phase) {
    var seconds = parseFloat(base || '0') || 0;
    return (seconds - phase).toFixed(2) + 's';
}

function createTrafficPacket() {
    var group = document.createElementNS(SVG_NS, 'g');
    group.setAttribute('class', 'network-packet');
    ['0s', '-0.48s', '-0.96s'].forEach(function(begin, idx) {
        var dot = document.createElementNS(SVG_NS, 'circle');
        var motion = document.createElementNS(SVG_NS, 'animateMotion');
        dot.setAttribute('r', idx === 0 ? '4.2' : '3.1');
        motion.setAttribute('data-base-begin', begin);
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
    node.querySelector('circle').setAttribute('r', model.size || 22);
    node.querySelector('.node-label').textContent = compactNetworkLabel(model.label);
    node.querySelector('.node-sub').textContent = model.sub || '';
    flashIfChanged(node, model.hash);
}

function removeMissingNetworkItems(items, seen) {
    Object.keys(items).forEach(function(key) {
        if (seen[key]) return;
        window.clearTimeout(items[key]._networkUpdateTimer);
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
    window.clearTimeout(node._networkUpdateTimer);
    node.classList.remove('is-updated');
    void node.getBoundingClientRect();
    node.classList.add('is-updated');
    node._networkUpdateTimer = window.setTimeout(function() {
        node.classList.remove('is-updated');
    }, 760);
}

function edgeMetricsText(edge, stat) {
    var parts = [];
    stat = stat || networkEdgeStats(edge, {});
    if (stat.active > 0) {
        parts.push(String(stat.active) + ' ' + (window.I18N.network_active_short || 'active'));
    } else if (stat.byteDelta > 0) {
        parts.push('+' + formatBytes(stat.byteDelta));
    } else if (stat.requestDelta > 0) {
        parts.push('+' + String(stat.requestDelta) + ' ' + (window.I18N.requests_short || 'req'));
    }
    if (edge.last_status) {
        parts.push(edge.last_status);
    }
    if ((edge.latency_ms || 0) > 0) {
        parts.push(Math.round(edge.latency_ms) + 'ms');
    }
    return parts.join(' · ');
}

function edgeTitle(edge, stat) {
    var used = formatDisplayTime(edge.last_used_at);
    stat = stat || networkEdgeStats(edge, {});
    var hidden = networkNumberValue(edge.hidden_count);
    return edge.instance + ' -> ' + edge.upstream_url +
        '\n' + (window.I18N.status || 'Status') + ': ' + translateUpstreamState(edge.state) +
        '\n' + (window.I18N.requests || 'Requests') + ': ' + String(edge.requests || 0) +
        '\n' + (window.I18N.errors || 'Errors') + ': ' + String(edge.errors || 0) +
        '\n' + (window.I18N.network_error_rate || 'Error rate') + ': ' + formatErrorPercent(edge.error_rate || 0) +
        '\n' + (window.I18N.network_upstream_traffic || window.I18N.network_traffic || 'Upstream traffic') +
        ': ' + formatBytes(edge.response_bytes || 0) +
        (hidden > 0 ? '\n' + (window.I18N.network_upstreams || 'Upstreams') + ': ' + String(hidden) : '') +
        ((edge.active_upstream_requests || 0) > 0 ?
            '\n' + (window.I18N.network_active || 'Active') + ': ' + String(edge.active_upstream_requests || 0) : '') +
        (stat.byteDelta > 0 ?
            '\n' + (window.I18N.stage_traffic_rate || 'Traffic rate') + ': +' + formatBytes(stat.byteDelta) : '') +
        (stat.requestDelta > 0 ?
            '\n' + (window.I18N.stage_request_rate || 'Request rate') + ': +' + String(stat.requestDelta) : '') +
        (edge.last_status ? '\n' + (window.I18N.last_status || 'Last status') + ': ' + edge.last_status : '') +
        (used.display ? '\n' + (window.I18N.last_used || 'Last used') + ': ' + used.display : '') +
        (edge.last_error ? '\n' + (window.I18N.last_error || 'Last error') + ': ' + edge.last_error : '');
}

function instanceLinkLabel(instance, activity) {
    if (activity.active > 0) {
        return String(activity.active) + ' ' + (window.I18N.network_active_short || 'active');
    }
    if (activity.byteDelta > 0) {
        return '+' + formatBytes(activity.byteDelta);
    }
    if (activity.requestDelta > 0) {
        return '+' + String(activity.requestDelta) + ' ' + (window.I18N.requests_short || 'req');
    }
    return instance.mode || '';
}

function instanceLinkTitle(instance, activity) {
    return (window.I18N.title || 'Cache Proxy') + ' -> ' + instance.name +
        '\n' + (window.I18N.network_active || 'Active') + ': ' + String(activity.active) +
        '\n' + (window.I18N.requests || 'Requests') + ' +' + String(activity.requestDelta) +
        '\n' + (window.I18N.network_upstream_traffic || window.I18N.network_traffic || 'Upstream traffic') +
        ' +' + formatBytes(activity.byteDelta) +
        '\n' + (window.I18N.status || 'Status') + ': ' + translateUpstreamState(activity.state);
}

function trafficLevel(activity) {
    var active = activity.active || 0;
    var requests = activity.requestDelta || 0;
    var bytes = activity.byteDelta || 0;
    if (active >= 8 || bytes >= 256 * 1024 * 1024 || requests >= 1000) {
        return 'high';
    }
    if (active >= 3 || bytes >= 32 * 1024 * 1024 || requests >= 100) {
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
    if (networkStateIsDegraded(upstream.state)) {
        return translateUpstreamState(upstream.state || 'unknown') + ' · ' + formatErrorPercent(upstream.error_rate || 0);
    }
    var requests = upstream.requests || 0;
    if (requests > 0) {
        return String(requests) + ' ' + (window.I18N.requests_short || 'req');
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
    var routeEdges = {};
    edges.forEach(function(edge) {
        var from = edge.getAttribute('data-edge-from') || '';
        var to = edge.getAttribute('data-edge-to') || '';
        var related = edgeRelatedIDs(edge);
        var hit = from === focusID || to === focusID || related.indexOf(focusID) !== -1;
        if (hit) {
            routeEdges[from] = true;
            routeEdges[to] = true;
            related.forEach(function(id) {
                routeEdges[id] = true;
            });
        }
    });
    edges.forEach(function(edge) {
        var from = edge.getAttribute('data-edge-from') || '';
        var to = edge.getAttribute('data-edge-to') || '';
        var related = edgeRelatedIDs(edge);
        var hit = from === focusID || to === focusID || related.indexOf(focusID) !== -1 ||
            (from === 'proxy:cache' && routeEdges[to]);
        edge.classList.toggle('is-focused', hit);
        edge.classList.toggle('is-dimmed', !hit);
        if (hit) {
            connected[from] = true;
            connected[to] = true;
            related.forEach(function(id) {
                connected[id] = true;
            });
        }
    });
    nodes.forEach(function(node) {
        var id = node.getAttribute('data-node-id') || '';
        node.classList.toggle('is-focused', !!connected[id]);
        node.classList.toggle('is-dimmed', !connected[id]);
    });
}

function edgeRelatedIDs(edge) {
    var related = edge.getAttribute('data-edge-related') || '';
    return related ? related.split(/\s+/).filter(Boolean) : [];
}

function renderNetworkTable(target, edges) {
    var t = window.I18N;
    if (!edges.length) {
        target.classList.add('network-table-wrap', 'empty-state', 'network-empty-state');
        target.classList.remove('is-fade-in');
        if (target._networkList || target.textContent !== (t.no_data || 'No data')) {
            target.textContent = t.no_data || 'No data';
        }
        target._networkList = null;
        return;
    }
    var firstRender = !target._networkList;
    var listState = ensureNetworkList(target);
    var previousEdges = networkPreviousEdges(
        typeof networkStageState !== 'undefined' ? networkStageState.previousNetwork : null
    );
    var rows = edges.slice().sort(function(a, b) {
        return networkListSortScore(b, previousEdges) - networkListSortScore(a, previousEdges) ||
            networkNumberValue(b.active_upstream_requests) - networkNumberValue(a.active_upstream_requests) ||
            networkNumberValue(b.response_bytes) - networkNumberValue(a.response_bytes) ||
            networkNumberValue(b.requests) - networkNumberValue(a.requests) ||
            String(a.upstream_host || a.upstream_url || '').localeCompare(String(b.upstream_host || b.upstream_url || ''));
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
        updateNetworkListItem(item, edge, previousEdges);
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

function updateNetworkListItem(item, edge, previousEdges) {
    var used = formatDisplayTime(edge.last_used_at);
    var fields = item._networkFields;
    var host = edge.upstream_host || edge.upstream_url || '-';
    var stat = networkEdgeStats(edge, previousEdges[networkEdgeKey(edge)]);
    item.title = edgeTitle(edge, stat).replace(/\n/g, ' · ');
    item.classList.toggle('is-alert', networkEdgeHealthLevel(edge) === 'alert');
    item.classList.toggle('is-notice', networkEdgeHealthLevel(edge) === 'notice');
    fields.title.textContent = host;
    fields.badge.className = 'result-badge ' + resultClass(edge.state);
    fields.badge.textContent = translateUpstreamState(edge.state);
    fields.meta.textContent = (edge.instance || '-') + ' -> ' +
        (edge.last_status ? 'HTTP ' + edge.last_status : translateUpstreamState(edge.state)) +
        (used.display ? ' · ' + used.display : '');
    fields.metrics[0].textContent = (window.I18N.network_active || 'Active') + ' ' +
        String(stat.active || 0);
    fields.metrics[1].textContent = (window.I18N.stage_request_rate || 'Request rate') + ' +' +
        String(stat.requestDelta || 0);
    fields.metrics[2].textContent = (window.I18N.network_upstream_traffic || window.I18N.network_traffic || 'Traffic') +
        ' +' + formatBytes(stat.byteDelta || 0);
    fields.metrics[3].textContent = (window.I18N.network_error_rate || 'Error rate') + ' ' +
        formatErrorPercent(edge.error_rate || 0) + ' · ' + (window.I18N.network_latency || 'Latency') + ' ' +
        networkNumberValue(edge.latency_ms).toFixed(0) + 'ms';
    var status = networkEdgeStatusText(edge);
    fields.issue.textContent = status;
    fields.issue.hidden = !status;
}

function networkListSortScore(edge, previousEdges) {
    if (!edge) {
        return 0;
    }
    var stat = networkEdgeStats(edge, previousEdges[networkEdgeKey(edge)]);
    var score = stat.active * 1000000000 + stat.byteDelta * 10 + stat.requestDelta;
    if (networkEdgeHealthLevel(edge) === 'alert') {
        score += 100000;
    } else if (networkEdgeHealthLevel(edge) === 'notice') {
        score += 100;
    }
    return score;
}

function networkEdgeHealthLevel(edge) {
    if (!edge) {
        return 'normal';
    }
    if (networkEdgeIsDegraded(edge)) {
        return 'alert';
    }
    if (networkEdgeHasRecentNotice(edge)) {
        return 'notice';
    }
    return 'normal';
}

function networkEdgeIsDegraded(edge) {
    return !!edge && networkStateIsDegraded(edge.state);
}

function networkStateIsDegraded(state) {
    return state === 'open' || state === 'degraded' || state === 'halfopen';
}

function networkEdgeHasRecentNotice(edge) {
    if (!edge || networkEdgeIsDegraded(edge)) {
        return false;
    }
    if (String(edge.last_error || '').trim()) {
        return true;
    }
    return networkStatusIsIssue(edge.last_status);
}

function networkEdgeStatusText(edge) {
    if (!edge) {
        return '';
    }
    if (networkEdgeIsDegraded(edge)) {
        var alertError = String(edge.last_error || '').trim();
        if (alertError) {
            return alertError;
        }
        if (edge.state && edge.state !== 'closed' && edge.state !== 'unknown') {
            return translateUpstreamState(edge.state);
        }
        return '';
    }
    if (networkEdgeHasRecentNotice(edge)) {
        var noticeError = String(edge.last_error || '').trim();
        if (noticeError) {
            return (window.I18N.network_recent_error || 'Recent error') + ': ' + noticeError;
        }
        if (networkStatusIsIssue(edge.last_status)) {
            return (window.I18N.network_recent_error || 'Recent error') + ': HTTP ' + String(edge.last_status);
        }
    }
    return networkEdgeHistoryText(edge);
}

function networkEdgeHistoryText(edge) {
    var errors = networkNumberValue(edge && edge.errors);
    if (errors <= 0) {
        return '';
    }
    return (window.I18N.network_history_errors || 'Historical errors') + ' ' +
        formatNetworkCompactNumber(errors) + ' · ' + formatErrorPercent(edge.error_rate || 0);
}

function networkStatusIsIssue(status) {
    var code = networkNumberValue(status);
    return code >= 500 || (code >= 400 && code !== 404);
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
