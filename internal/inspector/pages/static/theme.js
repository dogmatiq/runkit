// Applies the viewer's chosen color mode before the page is drawn, so that
// the page does not flash in the wrong colors. The choice is kept only in
// this browser.
(function () {
  "use strict";

  try {
    var mode = window.localStorage.getItem("inspector-theme");
    if (mode === "light" || mode === "dark") {
      document.documentElement.setAttribute("data-theme", mode);
    }
  } catch (e) {
    // Storage is unavailable, so the page follows the browser's preference.
  }
})();
