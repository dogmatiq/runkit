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
  //
  // A heartbeat dims each of the node's indicators briefly and brings it
  // back. The animation is not a class, so an update that morphs the page
  // leaves it running, and each heartbeat starts it again. There is no
  // animation if the viewer asks for reduced motion.

  var beat = [{ opacity: 0.65 }, { opacity: 1 }];
  var reducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)");

  function pulse(nodeID) {
    recordBeat(nodeID);

    if (reducedMotion.matches) return;

    var elements = document.querySelectorAll('[data-node="' + CSS.escape(nodeID) + '"]');
    for (var i = 0; i < elements.length; i++) {
      if (elements[i].animate) {
        elements[i].animate(beat, { duration: 1200, easing: "ease-out" });
      }
    }
  }

  // Heartbeat levels.
  //
  // The <canvas id="heartbeats"> element on the overview has a column for each
  // live node, in the order of the node IDs in its data-nodes attribute, like
  // the segments of the workload bar above it. A heartbeat fills the node's
  // column, and the column drains until the next heartbeat.
  //
  // Each node can send heartbeats at its own interval, which the page can't
  // see, so it measures each node's usual interval from the heartbeats that
  // it receives. A column drains to empty over drainBeats of the node's usual
  // intervals, so it empties when the node has missed about that many
  // heartbeats. Until the page has seen two of a node's heartbeats, the
  // node's column stays full after a heartbeat. A page that has just loaded
  // starts with empty columns.

  // riseTime is how long a column takes to fill after a heartbeat, so that it
  // rises instead of jumping.
  var riseTime = 160;

  // drainBeats is the number of a node's usual intervals over which its
  // column drains to empty.
  var drainBeats = 3;

  // levels holds, for each node ID, the times of the node's recent
  // heartbeats, and the level that its column had just before the most recent
  // one.
  var levels = {};

  function recordBeat(nodeID) {
    var now = performance.now();
    var l = levels[nodeID] || { times: [] };

    l.from = levelOf(nodeID, now);
    l.times.push(now);
    if (l.times.length > 9) l.times.shift();

    levels[nodeID] = l;
  }

  // usualInterval returns the median time between the given heartbeats, or
  // 0 if there are fewer than two.
  function usualInterval(times) {
    var intervals = [];
    for (var i = 1; i < times.length; i++) {
      intervals.push(times[i] - times[i - 1]);
    }

    if (intervals.length === 0) return 0;

    intervals.sort(function (a, b) {
      return a - b;
    });

    return intervals[Math.floor(intervals.length / 2)];
  }

  // levelOf returns the level of a node's column, from 0 when it is empty to
  // 1 when it is full.
  function levelOf(nodeID, now) {
    var l = levels[nodeID];
    if (!l) return 0;

    var since = now - l.times[l.times.length - 1];
    if (since < riseTime) {
      return l.from + (1 - l.from) * (since / riseTime);
    }

    var interval = usualInterval(l.times);
    if (interval === 0) return 1;

    return Math.max(0, 1 - (since - riseTime) / (drainBeats * interval));
  }

  // Scrolling charts.
  //
  // The server places each point of a chart by its time, with the time of the
  // most recent observation at the right-hand edge. Between updates, the page
  // moves each chart left at the rate that time passes, so it scrolls smoothly
  // although new points arrive only now and then. An update that changes a
  // chart starts it again from its new right-hand edge, where the old one had
  // scrolled to.

  var chartsSince = new WeakMap();

  function scrollCharts(now) {
    var groups = document.querySelectorAll("svg.spark[data-window] > g");

    for (var i = 0; i < groups.length; i++) {
      var group = groups[i];
      var svg = group.parentNode;
      var d = group.firstElementChild ? group.firstElementChild.getAttribute("d") : "";

      var seen = chartsSince.get(group);
      if (!seen || seen.d !== d) {
        seen = { d: d, since: now };
        chartsSince.set(group, seen);
      }

      var shift = ((now - seen.since) / Number(svg.getAttribute("data-window"))) * svg.viewBox.baseVal.width;
      group.setAttribute("transform", "translate(" + (-shift).toFixed(3) + " 0)");
    }
  }

  // roundRect fills a rectangle with rounded corners, or square ones if the
  // browser can't round them.
  function roundRect(g, x, y, w, h, r) {
    g.beginPath();
    if (g.roundRect) {
      g.roundRect(x, y, w, h, r);
    } else {
      g.rect(x, y, w, h);
    }
    g.fill();
  }

  function drawBeats() {
    if (!reducedMotion.matches) scrollCharts(performance.now());

    var canvas = document.getElementById("heartbeats");
    if (canvas && canvas.getContext) {
      var ratio = window.devicePixelRatio || 1;
      var width = Math.round(canvas.clientWidth * ratio);
      var height = Math.round(canvas.clientHeight * ratio);

      // Setting the size also clears the canvas, as does an update that
      // morphs the page and removes the size attributes.
      if (canvas.width !== width || canvas.height !== height) {
        canvas.width = width;
        canvas.height = height;
      }

      var g = canvas.getContext("2d");
      g.clearRect(0, 0, width, height);
      g.fillStyle = window.getComputedStyle(canvas).color;

      var nodes = (canvas.getAttribute("data-nodes") || "").split(" ").filter(Boolean);
      var now = performance.now();

      // The columns line up with the card's content, as the bar above does.
      var side = 20 * ratio;
      var gap = 3 * ratio;
      var top = 8 * ratio;
      var full = height - 2 * top;
      var column = (width - 2 * side - gap * (nodes.length - 1)) / Math.max(nodes.length, 1);
      var corner = Math.min(2 * ratio, column / 2);

      for (var i = 0; i < nodes.length; i++) {
        var x = side + i * (column + gap);
        var level = levelOf(nodes[i], now);
        var fill = full * level;

        g.globalAlpha = 0.08;
        roundRect(g, x, top, column, full, corner);

        if (fill > 0) {
          g.globalAlpha = 0.25 + 0.6 * level;
          roundRect(g, x, top + full - fill, column, fill, Math.min(corner, fill / 2));
        }
      }

      g.globalAlpha = 1;
    }

    // With reduced motion, the columns are redrawn once a second, so they
    // step instead of draining smoothly.
    if (reducedMotion.matches) {
      window.setTimeout(drawBeats, 1000);
    } else {
      window.requestAnimationFrame(drawBeats);
    }
  }

  drawBeats();

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
  // not changed stay as they are, and nothing flashes. After an update,
  // <details> elements that were open are opened again.
  //
  // A segment of a bar that the update removes, such as when a queue empties,
  // fades out instead of vanishing, because a queue often moves between one
  // message and none. If a later update reuses the segment before it has
  // faded, the morph removes its data-leaving attribute, and the segment
  // stays.

  if (window.Idiomorph) {
    window.Idiomorph.defaults.callbacks.beforeNodeRemoved = function (node) {
      if (!(node instanceof Element) || !node.animate || !node.matches(".segbar rect, .nodeseg")) {
        return true;
      }

      if (node.hasAttribute("data-leaving")) return false;
      node.setAttribute("data-leaving", "");

      var fade = node.animate(
        [{ opacity: window.getComputedStyle(node).opacity }, { opacity: 0 }],
        { duration: 600, easing: "ease-out", fill: "forwards" },
      );

      fade.onfinish = function () {
        if (node.hasAttribute("data-leaving")) {
          node.remove();
        } else {
          fade.cancel();
        }
      };

      return false;
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
