// Keeps an inspector page up to date.
//
// The server sends two kinds of server-sent events. An "observed" event means
// that the server has read the database again, so the page fetches itself and
// htmx replaces the live part of the page. A "heartbeat" event carries the ID
// of a node that sent a heartbeat, so that node's indicators pulse.
//
// Every page works without this script. It only adds the live updates, the
// color-mode control and the ticking relative times.
(function () {
  "use strict";

  var body = document.body;

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

  // The browser reconnects by itself when the connection breaks, such as while
  // the server restarts. Each time the page connects, the server sends a
  // "hello" event that holds the version of the pages that it renders.
  //
  // If the version differs from the one that rendered this page, the server
  // has restarted with different templates, styles or scripts, so the page
  // reloads itself. Otherwise, after a reconnection, the page fetches itself
  // again, because it may have missed updates while it was disconnected.
  var version = body.getAttribute("data-version");
  var hasConnected = false;

  function connect() {
    if (!window.EventSource) return;

    var source = new EventSource(body.getAttribute("data-live"));

    source.addEventListener("hello", function (e) {
      if (e.data !== version) {
        window.location.reload();
        return;
      }

      if (hasConnected && window.htmx) window.htmx.trigger(body, "observed");
      hasConnected = true;
    });

    source.addEventListener("observed", function () {
      if (window.htmx) window.htmx.trigger(body, "observed");
    });

    source.addEventListener("heartbeat", function (e) {
      pulse(e.data);
    });
  }

  connect();

  // Updates.
  //
  // Each update morphs the new page into the old one, so elements that have
  // not changed stay as they are, and nothing flashes. A node's heartbeat
  // blip is animated by a class that the server never renders, so the morph
  // leaves an element's classes alone while it blips, and an update does not
  // cut a blip short. After an update, <details> elements that were open are
  // opened again.

  if (window.Idiomorph) {
    window.Idiomorph.defaults.callbacks.beforeAttributeUpdated = function (name, node) {
      return !(name === "class" && node.classList && node.classList.contains("beat"));
    };
  }

  var openDetails = {};

  body.addEventListener("htmx:beforeSwap", function () {
    openDetails = {};

    var details = document.querySelectorAll("details[id][open]");
    for (var i = 0; i < details.length; i++) {
      openDetails[details[i].id] = true;
    }
  });

  body.addEventListener("htmx:afterSwap", function () {
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
