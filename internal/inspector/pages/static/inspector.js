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

  // Heartbeat lanes.
  //
  // The <canvas id="heartbeats"> element on the overview has a lane for each
  // live node, listed by ID in its data-nodes attribute. Each heartbeat that
  // the page has received in the last 30 seconds starts a segment in its
  // node's lane, like a segment of a bar, which lasts until the next
  // heartbeat. The segments move left as time passes and fade as they age. A
  // node that misses heartbeats leaves a gap. A page that has just loaded
  // starts with empty lanes.

  var beatWindow = 30000;
  var beats = {};

  function recordBeat(nodeID) {
    var times = beats[nodeID] || (beats[nodeID] = []);
    times.push(performance.now());
  }

  // usualInterval returns the median time between the given heartbeats, or
  // Infinity if there are too few to tell.
  function usualInterval(times) {
    var intervals = [];
    for (var i = 1; i < times.length; i++) {
      intervals.push(times[i] - times[i - 1]);
    }

    if (intervals.length < 3) return Infinity;

    intervals.sort(function (a, b) {
      return a - b;
    });

    return intervals[Math.floor(intervals.length / 2)];
  }

  function drawBeats() {
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

      var color = window.getComputedStyle(canvas).color;

      var nodes = (canvas.getAttribute("data-nodes") || "").split(" ").filter(Boolean);
      // Each lane and the gap below it share a pitch, which shrinks so that
      // every lane fits, however many nodes there are.
      var pad = 4 * ratio;
      var pitch = Math.min(10 * ratio, (height - 2 * pad) / Math.max(nodes.length, 1));
      var lane = (pitch * 2) / 3;
      var gap = pitch - lane;
      var top = (height - nodes.length * pitch + gap) / 2;
      var spacing = 1.5 * ratio;
      var corner = Math.min(lane / 2, 2 * ratio);
      var now = performance.now();

      g.fillStyle = color;

      for (var i = 0; i < nodes.length; i++) {
        var y = top + i * (lane + gap);

        g.globalAlpha = 0.08;
        g.fillRect(0, y, width, lane);

        var times = beats[nodes[i]] || [];
        while (times.length && now - times[0] > beatWindow) times.shift();

        var usual = usualInterval(times);

        for (var j = 0; j < times.length; j++) {
          var age = (now - times[j]) / beatWindow;
          var x = width - age * width;

          // Each heartbeat starts a segment that lasts until the next one, but
          // no longer than the node's usual interval, so a missed heartbeat
          // leaves a gap.
          var until = j + 1 < times.length ? times[j + 1] : now;
          var length = (Math.min(until - times[j], usual) / beatWindow) * width - spacing;
          if (length <= 0) continue;

          g.globalAlpha = 0.15 + 0.75 * (1 - age) * (1 - age);
          g.beginPath();
          if (g.roundRect) {
            g.roundRect(x, y, length, lane, corner);
          } else {
            g.rect(x, y, length, lane);
          }
          g.fill();
        }
      }

      g.globalAlpha = 1;
    }

    // With reduced motion, the lanes are redrawn once a second, so the segments
    // step instead of gliding.
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
