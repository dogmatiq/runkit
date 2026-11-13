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

  // Heartbeat wave.
  //
  // The <canvas id="heartbeats"> element on the overview draws one line,
  // like the charts on the other cards. Each heartbeat from a live node, whose
  // IDs are in the canvas's data-nodes attribute, adds a small bump to the
  // line. The bump starts just past the right-hand edge, slides in, moves left,
  // and leaves the chart after beatWindow. While the nodes are sending
  // heartbeats, the line keeps rippling. When a node misses heartbeats, the
  // line has fewer bumps. A page that has just loaded starts with a flat line.

  var beatWindow = 30000;

  // bumpWidth is the width of a bump's middle, in CSS pixels.
  var bumpWidth = 5;
  var beats = {};

  function recordBeat(nodeID) {
    var times = beats[nodeID] || (beats[nodeID] = []);
    times.push(performance.now());
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

      var style = window.getComputedStyle(canvas);
      var nodes = (canvas.getAttribute("data-nodes") || "").split(" ").filter(Boolean);
      var now = performance.now();

      // spread is the width of a bump's middle, as a time. A bump starts
      // lead before the right-hand edge, so it slides in instead of
      // appearing.
      var spread = ((bumpWidth * ratio) / width) * beatWindow;
      var lead = 4 * spread;

      // The bumps of all of the nodes' heartbeats, by time. More nodes make
      // more bumps, so each is smaller, and the line stays inside the chart.
      var times = [];
      for (var i = 0; i < nodes.length; i++) {
        var own = beats[nodes[i]] || [];
        while (own.length && now - own[0] > beatWindow + 2 * lead) own.shift();
        times = times.concat(own);
      }

      var base = height - 2 * ratio;
      var bump = (height * 0.55) / Math.sqrt(Math.max(nodes.length, 1));
      var points = [];

      // The line is drawn through points at fixed times, not at fixed
      // places, so each point moves left with the bumps and keeps its height.
      // Points at fixed places would sample each bump at a slightly different
      // part from one frame to the next, and its edges would shimmer.
      var step = ((2 * ratio) / width) * beatWindow;
      var right = now - lead;
      var left = right - beatWindow;

      for (var t = Math.floor(left / step) * step; t <= right + step; t += step) {
        var x = width - ((right - t) / beatWindow) * width;
        var y = 0;
        for (var j = 0; j < times.length; j++) {
          var d = (t - times[j]) / spread;
          if (d > -4 && d < 4) y += Math.exp(-d * d) * bump;
        }
        points.push([x, Math.max(base - y, ratio)]);
      }

      g.beginPath();
      g.moveTo(0, height);
      for (var k = 0; k < points.length; k++) g.lineTo(points[k][0], points[k][1]);
      g.lineTo(width, height);
      g.closePath();
      g.fillStyle = style.getPropertyValue("--subsystem-soft");
      g.fill();

      g.beginPath();
      for (var m = 0; m < points.length; m++) {
        if (m === 0) g.moveTo(points[m][0], points[m][1]);
        else g.lineTo(points[m][0], points[m][1]);
      }
      g.strokeStyle = style.color;
      g.lineWidth = 1.5 * ratio;
      g.stroke();
    }

    // With reduced motion, the wave is redrawn once a second, so the bumps
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
