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

function copyCode(el, txt) {
    navigator.clipboard.writeText(txt);
    var root = document.documentElement;
    var s = getComputedStyle(root).getPropertyValue('--code-flash').trim();
    el.style.background = s;
    setTimeout(function() { el.style.background = ''; }, 600);
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

document.addEventListener('DOMContentLoaded', function() {
    if (!document.cookie.includes('theme=') && !window.location.search.includes('theme=')) {
        if (window.matchMedia('(prefers-color-scheme:dark)').matches) {
            document.documentElement.setAttribute('data-theme', 'dark');
        }
    }
    updateThemeBtn(document.documentElement.getAttribute('data-theme') || 'light');
});
