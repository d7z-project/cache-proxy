function toggleTheme(){var root=document.documentElement;var next=root.dataset.theme==='dark'?'light':'dark';root.dataset.theme=next;localStorage.setItem('cache-proxy-theme',next)}
function toggleLangMenu(){var menu=document.getElementById('lang-menu');if(menu)menu.hidden=!menu.hidden}
function selectLang(button){var url=new URL(location.href);url.searchParams.set('lang',button.dataset.lang);location.href=url.toString()}
async function copyToClipboard(button){var value=button.dataset.copy||'';try{await navigator.clipboard.writeText(value);var old=button.textContent;button.textContent=(window.I18N&&window.I18N.copied)||'Copied';setTimeout(function(){button.textContent=old},1000)}catch(_){}}
(function(){var saved=localStorage.getItem('cache-proxy-theme');if(saved)document.documentElement.dataset.theme=saved})();
