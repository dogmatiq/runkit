// Keeps an inspector page up to date.
//
// The server sends two kinds of server-sent events. An "observed" event means
// that the server has read the database again, so the page fetches itself and
// htmx replaces the live part of the page. A "heartbeat" event carries the ID
// of a node that sent a heartbeat, so that node's indicators pulse.
//
// Every page works without this script. It only adds the live updates, the
// color-mode control, the ticking relative times and the highlighting of
// values that changed.
(function () {
  "use strict";

  var body = document.body;
  var status = document.getElementById("live-status");

  // Relative times.
  //
  // Each <time data-offset> element holds the difference between its time
  // and the database's clock when the page was rendered. The text is
  // recomputed from the time that has passed since the element appeared, so
  // it does not depend on the browser's clock.

  var appeared = new WeakMap();

  function formatDuration(ms) {
    ms = Math.abs(ms);
    var s = Math.floor(ms / 1000);
    if (ms < 1000) return Math.floor(ms) + "ms";
    if (s < 60) return s + "s";
    if (s < 3600) return Math.floor(s / 60) + "m " + (s % 60) + "s";
    if (s < 172800) return Math.floor(s / 3600) + "h " + Math.floor((s % 3600) / 60) + "m";
    return Math.floor(s / 86400) + "d " + Math.floor((s % 86400) / 3600) + "h";
  }

  function describeOffset(ms) {
    if (ms >= 1000) return "in " + formatDuration(ms);
    if (ms > 0) return "in under 1s";
    if (ms > -1000) return "just now";
    return formatDuration(ms) + " ago";
  }

  function tick() {
    var now = performance.now();
    var elements = document.querySelectorAll("time[data-offset]");

    for (var i = 0; i < elements.length; i++) {
      var el = elements[i];
      if (!appeared.has(el)) appeared.set(el, now);

      var offset = Number(el.getAttribute("data-offset")) - (now - appeared.get(el));
      var text = describeOffset(offset);
      if (el.textContent !== text) el.textContent = text;
    }
  }

  tick();
  window.setInterval(tick, 500);

  // Heartbeats.

  function pulse(nodeID) {
    var elements = document.querySelectorAll('[data-node="' + CSS.escape(nodeID) + '"]');
    for (var i = 0; i < elements.length; i++) {
      restartAnimation(elements[i], "beat");
    }
  }

  function restartAnimation(el, className) {
    el.classList.remove(className);
    void el.offsetWidth; // Restarts the CSS animation.
    el.classList.add(className);
  }

  // Live updates.

  function setStatus(state, label, title) {
    if (!status) return;
    status.setAttribute("data-state", state);
    status.title = title;
    status.querySelector(".live-label").textContent = label;
  }

  function connect() {
    if (!window.EventSource) {
      setStatus("offline", "static", "This browser can not receive live updates");
      return;
    }

    var source = new EventSource(body.getAttribute("data-live"));

    source.onopen = function () {
      setStatus("live", "live", "Receiving live updates");
    };

    // The browser reconnects by itself after an error.
    source.onerror = function () {
      setStatus("offline", "reconnecting", "Not receiving live updates, reconnecting");
    };

    source.addEventListener("observed", function () {
      if (status) restartAnimation(status, "tick");
      if (window.htmx) window.htmx.trigger(body, "observed");
    });

    source.addEventListener("heartbeat", function (e) {
      pulse(e.data);
    });
  }

  connect();

  // Highlighting and open sections.
  //
  // Before htmx replaces the live part of the page, remember the text of each
  // element with a data-watch attribute, and which <details> elements are
  // open. Afterwards, highlight each value that changed, and reopen the same
  // <details> elements.

  var watched = {};
  var openDetails = {};

  body.addEventListener("htmx:beforeSwap", function () {
    watched = {};
    openDetails = {};

    var elements = document.querySelectorAll("[data-watch]");
    for (var i = 0; i < elements.length; i++) {
      watched[elements[i].getAttribute("data-watch")] = elements[i].textContent.trim();
    }

    var details = document.querySelectorAll("details[id][open]");
    for (var j = 0; j < details.length; j++) {
      openDetails[details[j].id] = true;
    }
  });

  body.addEventListener("htmx:afterSwap", function () {
    var elements = document.querySelectorAll("[data-watch]");
    for (var i = 0; i < elements.length; i++) {
      var key = elements[i].getAttribute("data-watch");
      var before = watched[key];
      if (before !== undefined && before !== elements[i].textContent.trim()) {
        restartAnimation(elements[i], "changed");
      }
    }

    for (var id in openDetails) {
      var d = document.getElementById(id);
      if (d) d.open = true;
    }

    tick();
  });

  // Color mode.
  //
  // The control cycles through automatic, light and dark. The choice is kept
  // in this browser only.

  var toggle = document.getElementById("theme-toggle");
  var modes = ["auto", "light", "dark"];

  function currentMode() {
    return document.documentElement.getAttribute("data-theme") || "auto";
  }

  function showMode(mode) {
    if (!toggle) return;
    toggle.querySelector(".theme-label").textContent = mode;
    toggle.title = "Color mode: " + (mode === "auto" ? "automatic" : mode);
  }

  function applyMode(mode) {
    if (mode === "auto") {
      document.documentElement.removeAttribute("data-theme");
    } else {
      document.documentElement.setAttribute("data-theme", mode);
    }

    try {
      if (mode === "auto") {
        window.localStorage.removeItem("inspector-theme");
      } else {
        window.localStorage.setItem("inspector-theme", mode);
      }
    } catch (e) {
      // Storage is unavailable, so the choice lasts only for this page.
    }

    showMode(mode);
  }

  if (toggle) {
    showMode(currentMode());

    toggle.addEventListener("click", function () {
      var next = modes[(modes.indexOf(currentMode()) + 1) % modes.length];
      applyMode(next);
    });
  }
})();
