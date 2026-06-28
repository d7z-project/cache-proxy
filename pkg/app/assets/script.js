function copyURL(btn, txt) {
    navigator.clipboard.writeText(txt);
    var t = window.I18N;
    btn.textContent = t.copied;
    btn.classList.add('copied');
    setTimeout(function() {
        btn.textContent = t.copy;
        btn.classList.remove('copied');
    }, 1500);
}

function copyCode(btn, txt) {
    navigator.clipboard.writeText(txt);
    var t = window.I18N;
    btn.textContent = t.copied;
    btn.classList.add('copied');
    setTimeout(function() {
        btn.textContent = t.copy;
        btn.classList.remove('copied');
    }, 1500);
}

function toggleReleases(btn) {
    btn.classList.toggle('open');
}

function toggleTheme() {
    var root = document.documentElement;
    var next = root.getAttribute('data-theme') === 'dark' ? 'light' : 'dark';
    root.setAttribute('data-theme', next);
    document.cookie = 'theme=' + next + ';path=/;max-age=31536000;samesite=lax';
    updateThemeBtn(next);
}

function toggleLang() {
    var p = new URL(window.location).searchParams;
    var next = p.get('lang') === 'zh' ? 'en' : 'zh';
    p.set('lang', next);
    window.location.search = p.toString();
}

function updateThemeBtn(theme) {
    var btn = document.getElementById('theme-btn');
    if (btn) btn.textContent = theme === 'dark' ? '\u65e5' : '\u591c';
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

    function filterCards() {
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
    }

    for (var j = 0; j < chips.length; j++) {
        chips[j].addEventListener('click', function() {
            if (this.disabled) return;
            var group = this.getAttribute('data-filter-group');
            var value = this.getAttribute('data-filter-value') || '';
            active[group] = value;
            for (var k = 0; k < chips.length; k++) {
                if (chips[k].getAttribute('data-filter-group') === group) {
                    chips[k].classList.toggle('is-active', chips[k] === this);
                }
            }
            filterCards();
        });
    }

    input.addEventListener('input', filterCards);
    filterCards();
}

document.addEventListener('DOMContentLoaded', function() {
    if (!document.cookie.includes('theme=') && !window.location.search.includes('theme=')) {
        if (window.matchMedia('(prefers-color-scheme:dark)').matches) {
            document.documentElement.setAttribute('data-theme', 'dark');
        }
    }
    updateThemeBtn(document.documentElement.getAttribute('data-theme') || 'light');
    initSearch();
});
