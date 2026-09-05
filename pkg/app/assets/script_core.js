(function () {
  "use strict";

  var i18n = window.I18N || {};
  var toastTimer;
  var statusController;
  var diskSamples = [];

  function text(key, fallback) {
    return i18n[key] || fallback || key;
  }

  function formatNumber(value) {
    return new Intl.NumberFormat(document.documentElement.lang).format(Number(value) || 0);
  }

  function formatPercent(value) {
    return ((Number(value) || 0) * 100).toFixed(1) + "%";
  }

  function formatBytes(value) {
    var bytes = Number(value) || 0;
    var units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    var unit = 0;
    while (Math.abs(bytes) >= 1024 && unit < units.length - 1) {
      bytes /= 1024;
      unit++;
    }
    var digits = unit === 0 || Math.abs(bytes) >= 100 ? 0 : 1;
    return bytes.toFixed(digits) + " " + units[unit];
  }

  function formatDateTime(value) {
    var date = new Date(value);
    if (!value || Number.isNaN(date.getTime())) {
      return text("none", "None");
    }
    return new Intl.DateTimeFormat(document.documentElement.lang, {
      dateStyle: "short",
      timeStyle: "medium"
    }).format(date);
  }

  function formatDuration(milliseconds) {
    var duration = Number(milliseconds) || 0;
    if (duration < 1000) {
      return Math.max(duration, 0) + " ms";
    }
    if (duration < 60000) {
      return (duration / 1000).toFixed(1) + " s";
    }
    return (duration / 60000).toFixed(1) + " min";
  }

  function showToast(message) {
    var toast = document.getElementById("toast");
    if (!toast) {
      return;
    }
    clearTimeout(toastTimer);
    toast.textContent = message;
    toast.hidden = false;
    requestAnimationFrame(function () {
      toast.classList.add("visible");
    });
    toastTimer = setTimeout(function () {
      toast.classList.remove("visible");
      setTimeout(function () {
        toast.hidden = true;
      }, 160);
    }, 1600);
  }

  function updateThemeControl() {
    var isDark = document.documentElement.dataset.theme === "dark";
    var button = document.getElementById("theme-button");
    var icon = document.getElementById("theme-icon");
    if (button) {
      button.setAttribute("aria-pressed", String(isDark));
    }
    if (icon) {
      icon.setAttribute("href", isDark ? "#icon-sun" : "#icon-moon");
    }
  }

  function toggleTheme() {
    var root = document.documentElement;
    var next = root.dataset.theme === "dark" ? "light" : "dark";
    root.dataset.theme = next;
    localStorage.setItem("cache-proxy-theme", next);
    updateThemeControl();
    drawDiskChart();
  }

  function selectLanguage(language) {
    var url = new URL(window.location.href);
    url.searchParams.set("lang", language);
    window.location.assign(url.toString());
  }

  async function copyToClipboard(button) {
    var value = button.dataset.copy || "";
    try {
      if (navigator.clipboard && window.isSecureContext) {
        await navigator.clipboard.writeText(value);
      } else {
        var input = document.createElement("textarea");
        input.value = value;
        input.setAttribute("readonly", "");
        input.style.position = "fixed";
        input.style.opacity = "0";
        document.body.appendChild(input);
        input.select();
        document.execCommand("copy");
        input.remove();
      }
      showToast(text("copied", "Copied"));
    } catch (_) {
      showToast(text("copy_failed", "Copy failed"));
    }
  }

  function appendCell(row, value, title) {
    var cell = document.createElement("td");
    cell.textContent = value;
    if (title) {
      cell.title = title;
    }
    row.appendChild(cell);
    return cell;
  }

  function renderUpstreams(upstreams) {
    var body = document.getElementById("status-upstream-rows");
    var empty = document.getElementById("status-upstream-empty");
    var count = document.getElementById("status-network-count");
    body.replaceChildren();
    empty.hidden = upstreams.length !== 0;
    count.textContent = formatNumber(upstreams.length) + " " + text("hosts", "hosts");

    upstreams.forEach(function (upstream) {
      var row = document.createElement("tr");
      appendCell(row, upstream.host || text("none", "None"));
      appendCell(row, formatNumber(upstream.requests));
      appendCell(row, formatNumber(upstream.admission_active) + " / " + formatNumber(upstream.admission_queued));
      appendCell(row, (Number(upstream.latency_ms) || 0).toFixed(0) + " ms");
      appendCell(row, formatPercent(upstream.error_rate), formatNumber(upstream.errors) + " / " + formatNumber(upstream.requests));

      var until = new Date(upstream.cooldown_until || 0);
      var coolingDown = !Number.isNaN(until.getTime()) && until.getTime() > Date.now();
      var cooldown = appendCell(row, coolingDown ? formatDateTime(upstream.cooldown_until) : text("none", "None"));
      if (coolingDown) {
        cooldown.classList.add("cooldown-active");
      }
      body.appendChild(row);
    });
  }

  function renderEvents(events) {
    var body = document.getElementById("status-event-rows");
    var empty = document.getElementById("status-event-empty");
    var count = document.getElementById("status-event-count");
    body.replaceChildren();
    empty.hidden = events.length !== 0;
    count.textContent = formatNumber(events.length) + " " + text("events", "events");

    events.slice().reverse().forEach(function (event) {
      var row = document.createElement("tr");
      var result = String(event.result || "unknown").toLowerCase();
      var storage = event.storage || text("none", "None");
      var task = String(event.task_type || "").replaceAll("_", " ");
      appendCell(row, formatDateTime(event.finished_at)).className = "event-col-finished";
      appendCell(row, storage).className = "event-col-storage";
      appendCell(row, task).className = "event-col-task";
      var targetCell = appendCell(row, "");
      targetCell.className = "event-target event-col-target";
      var compactContext = document.createElement("span");
      compactContext.className = "event-compact-context";
      compactContext.textContent = storage + " / " + task + " / " + formatDateTime(event.finished_at);
      targetCell.appendChild(compactContext);
      var target = document.createElement("span");
      target.className = "event-target-main";
      target.textContent = event.target || "/";
      targetCell.appendChild(target);
      var message = String(event.message || "").trim();
      if (message) {
        var detail = document.createElement("span");
        detail.className = "event-detail" + (result === "success" || result === "skipped" ? "" : " event-detail-error");
        detail.textContent = message;
        targetCell.appendChild(detail);
      }
      var resultCell = appendCell(row, "");
      resultCell.className = "event-col-result";
      var chip = document.createElement("span");
      chip.className = "status-chip status-chip-" + (result === "success" ? "success" : result === "skipped" ? "skipped" : "failure");
      chip.textContent = result;
      resultCell.appendChild(chip);
      appendCell(row, formatDuration(event.duration_ms)).className = "event-col-duration";
      body.appendChild(row);
    });
  }

  function drawDiskChart() {
    var canvas = document.getElementById("disk-chart");
    var empty = document.getElementById("disk-chart-empty");
    var range = document.getElementById("disk-range");
    if (!canvas || !empty || !range) {
      return;
    }
    empty.hidden = diskSamples.length !== 0;
    range.replaceChildren();
    if (diskSamples.length === 0) {
      var emptyContext = canvas.getContext("2d");
      emptyContext.clearRect(0, 0, canvas.width, canvas.height);
      return;
    }

    var width = Math.max(canvas.clientWidth, 1);
    var height = Math.max(canvas.clientHeight, 1);
    var scale = window.devicePixelRatio || 1;
    canvas.width = Math.round(width * scale);
    canvas.height = Math.round(height * scale);
    var context = canvas.getContext("2d");
    context.scale(scale, scale);
    context.clearRect(0, 0, width, height);

    var styles = getComputedStyle(document.documentElement);
    var lineColor = styles.getPropertyValue("--line-strong").trim();
    var accentColor = styles.getPropertyValue("--accent").trim();
    var points = diskSamples.map(function (sample) {
      return Math.max(Number(sample.total_bytes) || 0, 0);
    });
    var maximum = Math.max.apply(null, points.concat([1]));
    var minimum = Math.min.apply(null, points);
    var spread = Math.max(maximum - minimum, maximum * 0.08, 1);
    var lower = Math.max(0, minimum - spread * 0.25);
    var upper = maximum + spread * 0.15;
    var left = 8;
    var right = width - 8;
    var top = 12;
    var bottom = height - 14;

    context.strokeStyle = lineColor;
    context.lineWidth = 1;
    for (var gridLine = 0; gridLine < 4; gridLine++) {
      var gridY = top + ((bottom - top) * gridLine) / 3;
      context.beginPath();
      context.moveTo(left, gridY + 0.5);
      context.lineTo(right, gridY + 0.5);
      context.stroke();
    }

    var coordinates = points.map(function (point, index) {
      return {
        x: points.length === 1 ? (left + right) / 2 : left + ((right - left) * index) / (points.length - 1),
        y: bottom - ((point - lower) / (upper - lower)) * (bottom - top)
      };
    });

    context.beginPath();
    context.moveTo(coordinates[0].x, bottom);
    coordinates.forEach(function (point) {
      context.lineTo(point.x, point.y);
    });
    context.lineTo(coordinates[coordinates.length - 1].x, bottom);
    context.closePath();
    context.fillStyle = colorWithAlpha(accentColor, 0.12);
    context.fill();

    context.beginPath();
    coordinates.forEach(function (point, index) {
      if (index === 0) {
        context.moveTo(point.x, point.y);
      } else {
        context.lineTo(point.x, point.y);
      }
    });
    context.strokeStyle = accentColor;
    context.lineWidth = 2;
    context.stroke();

    var first = document.createElement("span");
    first.textContent = formatDateTime(diskSamples[0].at);
    var last = document.createElement("span");
    last.textContent = diskSamples.length === 1 ? text("now", "now") : formatDateTime(diskSamples[diskSamples.length - 1].at);
    range.append(first, last);
  }

  function colorWithAlpha(color, alpha) {
    var probe = document.createElement("canvas").getContext("2d");
    probe.fillStyle = color;
    var normalized = probe.fillStyle;
    if (normalized.charAt(0) === "#") {
      var hex = normalized.slice(1);
      if (hex.length === 3) {
        hex = hex.split("").map(function (character) { return character + character; }).join("");
      }
      var number = parseInt(hex, 16);
      return "rgba(" + ((number >> 16) & 255) + "," + ((number >> 8) & 255) + "," + (number & 255) + "," + alpha + ")";
    }
    return color;
  }

  function renderStatus(summary, disk, events, network) {
    var statusSummary = network.summary || {};
    var health = document.getElementById("status-health");
    health.textContent = summary.healthy ? text("healthy", "Healthy") : text("degraded", "Degraded");
    health.className = summary.healthy ? "state-green" : "state-red";
    document.getElementById("status-hit-rate").textContent = formatPercent(statusSummary.hit_rate);
    document.getElementById("status-active-downloads").textContent = formatNumber(statusSummary.active_downloads);
    document.getElementById("status-upstream-errors").textContent = formatNumber(statusSummary.upstream_errors) + " / " + formatNumber(statusSummary.upstream_requests);
    document.getElementById("status-queued").textContent = formatNumber(statusSummary.queued_upstream_requests);
    document.getElementById("status-rate-limited").textContent = formatNumber(statusSummary.rate_limited_upstreams);

    diskSamples = Array.isArray(disk.samples) ? disk.samples : [];
    var latest = diskSamples.length ? diskSamples[diskSamples.length - 1].total_bytes : 0;
    document.getElementById("status-disk-total").textContent = formatBytes(latest);
    renderUpstreams(Array.isArray(network.upstreams) ? network.upstreams : []);
    renderEvents(Array.isArray(events.events) ? events.events : []);
    document.getElementById("status-updated").textContent = text("updated", "Updated") + " " + formatDateTime(network.generated_at || new Date().toISOString());
  }

  async function fetchStatusJSON(path, signal) {
    var response;
    try {
      response = await fetch(path, { cache: "no-store", signal: signal, headers: { Accept: "application/json" } });
    } catch (fetchError) {
      if (fetchError.name === "AbortError") {
        throw fetchError;
      }
      throw new Error(path + ": " + fetchError.message);
    }
    if (!response.ok) {
      var responseBody = "";
      try {
        responseBody = (await response.text()).trim();
      } catch (_) {
        responseBody = "";
      }
      if (responseBody.length > 600) {
        responseBody = responseBody.slice(0, 600) + "...";
      }
      var message = path + ": " + response.status + " " + response.statusText;
      if (responseBody) {
        message += "\n" + responseBody;
      }
      throw new Error(message);
    }
    try {
      return await response.json();
    } catch (parseError) {
      throw new Error(path + ": " + parseError.message);
    }
  }

  async function loadStatus() {
    var loading = document.getElementById("status-loading");
    var error = document.getElementById("status-error");
    var errorDetail = document.getElementById("status-error-detail");
    var content = document.getElementById("status-content");
    var refresh = document.getElementById("status-refresh");
    if (!loading || !error || !errorDetail || !content || !refresh) {
      return;
    }
    if (statusController) {
      statusController.abort();
    }
    var controller = new AbortController();
    statusController = controller;
    loading.hidden = false;
    error.hidden = true;
    errorDetail.textContent = "";
    content.hidden = true;
    refresh.classList.add("is-loading");
    refresh.disabled = true;

    try {
      var payloads = await Promise.all([
        fetchStatusJSON("/-/status/summary", controller.signal),
        fetchStatusJSON("/-/status/disk", controller.signal),
        fetchStatusJSON("/-/status/events?limit=50", controller.signal),
        fetchStatusJSON("/-/status/network", controller.signal)
      ]);
      renderStatus(payloads[0], payloads[1], payloads[2], payloads[3]);
      loading.hidden = true;
      content.hidden = false;
      requestAnimationFrame(drawDiskChart);
    } catch (requestError) {
      if (requestError.name === "AbortError") {
        return;
      }
      loading.hidden = true;
      error.hidden = false;
      errorDetail.textContent = requestError.message;
    } finally {
      if (statusController === controller) {
        refresh.classList.remove("is-loading");
        refresh.disabled = false;
      }
    }
  }

  function activateStatusTab(tab) {
    var name = tab.dataset.statusTab;
    document.querySelectorAll("[data-status-tab]").forEach(function (candidate) {
      var selected = candidate === tab;
      candidate.setAttribute("aria-selected", String(selected));
      candidate.tabIndex = selected ? 0 : -1;
    });
    document.querySelectorAll("[data-status-panel]").forEach(function (panel) {
      panel.hidden = panel.dataset.statusPanel !== name;
    });
    var content = document.getElementById("status-content");
    if (content) {
      content.scrollTop = 0;
      content.scrollLeft = 0;
    }
    if (name === "overview") {
      requestAnimationFrame(drawDiskChart);
    }
  }

  var savedTheme = localStorage.getItem("cache-proxy-theme");
  if (savedTheme === "light" || savedTheme === "dark") {
    document.documentElement.dataset.theme = savedTheme;
  }
  updateThemeControl();

  var themeButton = document.getElementById("theme-button");
  if (themeButton) {
    themeButton.addEventListener("click", toggleTheme);
  }

  document.querySelectorAll("[data-copy]").forEach(function (button) {
    button.addEventListener("click", function () {
      copyToClipboard(button);
    });
  });

  var language = document.getElementById("language-select");
  var languageButton = document.getElementById("language-button");
  var languageMenu = document.getElementById("language-menu");
  if (language && languageButton && languageMenu) {
    languageButton.addEventListener("click", function () {
      languageMenu.hidden = !languageMenu.hidden;
      languageButton.setAttribute("aria-expanded", String(!languageMenu.hidden));
    });
    languageMenu.querySelectorAll("[data-language]").forEach(function (button) {
      button.addEventListener("click", function () {
        selectLanguage(button.dataset.language);
      });
    });
    document.addEventListener("click", function (event) {
      if (!language.contains(event.target)) {
        languageMenu.hidden = true;
        languageButton.setAttribute("aria-expanded", "false");
      }
    });
  }

  var dialog = document.getElementById("status-dialog");
  var statusButton = document.getElementById("status-button");
  if (dialog && statusButton && typeof dialog.showModal === "function") {
    statusButton.addEventListener("click", function (event) {
      event.preventDefault();
      dialog.showModal();
      loadStatus();
    });
    document.getElementById("status-close").addEventListener("click", function () {
      dialog.close();
    });
    document.getElementById("status-refresh").addEventListener("click", loadStatus);
    dialog.addEventListener("click", function (event) {
      if (event.target === dialog) {
        dialog.close();
      }
    });
    dialog.addEventListener("close", function () {
      if (statusController) {
        statusController.abort();
      }
    });

    var statusTabs = Array.from(document.querySelectorAll("[data-status-tab]"));
    statusTabs.forEach(function (tab, index) {
      tab.addEventListener("click", function () {
        activateStatusTab(tab);
      });
      tab.addEventListener("keydown", function (event) {
        if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") {
          return;
        }
        event.preventDefault();
        var direction = event.key === "ArrowRight" ? 1 : -1;
        var next = statusTabs[(index + direction + statusTabs.length) % statusTabs.length];
        activateStatusTab(next);
        next.focus();
      });
    });
  }

  var chartWrap = document.querySelector(".chart-wrap");
  if ("ResizeObserver" in window && chartWrap) {
    new ResizeObserver(function () {
      var statusContent = document.getElementById("status-content");
      if (dialog && dialog.open && statusContent && !statusContent.hidden) {
        drawDiskChart();
      }
    }).observe(chartWrap);
  } else {
    window.addEventListener("resize", drawDiskChart);
  }

  document.addEventListener("keydown", function (event) {
    if (event.key === "Escape" && languageMenu && !languageMenu.hidden) {
      languageMenu.hidden = true;
      languageButton.setAttribute("aria-expanded", "false");
      languageButton.focus();
    }
  });
})();
