var statusState = {
    modal: null,
    activeTab: 'disk',
    controllers: { summary: null, disk: null, network: null, events: null },
    cache: { summary: null, disk: null, network: null, events: null },
    refreshTimer: null,
    scrollTops: { disk: 0, network: 0, events: 0 },
    lastRefresh: 0,
    openedAt: 0
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

function toggleReleases(btn) {
    var body = null;
    var targetID = btn.getAttribute('aria-controls');
    if (targetID) {
        body = document.getElementById(targetID);
    }
    if (!body) {
        var container = btn.closest ? btn.closest('.card-releases') : null;
        body = container ? container.querySelector('.release-body') : null;
    }
    var open = btn.classList.toggle('open');
    btn.setAttribute('aria-expanded', open ? 'true' : 'false');
    if (body) {
        body.classList.toggle('is-open', open);
    }
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
    root.classList.add('theme-animating');
    root.setAttribute('data-theme', next);
    document.cookie = 'theme=' + next + ';path=/;max-age=31536000;samesite=lax';
    updateThemeBtn(next);
    window.clearTimeout(toggleTheme._timer);
    toggleTheme._timer = window.setTimeout(function() {
        root.classList.remove('theme-animating');
    }, 260);
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

document.addEventListener('DOMContentLoaded', function() {
    if (!document.cookie.includes('theme=') && !window.location.search.includes('theme=')) {
        if (window.matchMedia('(prefers-color-scheme:dark)').matches) {
            document.documentElement.setAttribute('data-theme', 'dark');
        }
    }
    updateThemeBtn(document.documentElement.getAttribute('data-theme') || 'light');
    initSearch();
    initStatusModal();
    if (typeof initNetworkStage === 'function') {
        initNetworkStage();
    }
});
