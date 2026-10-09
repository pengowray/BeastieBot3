// Pengo Wray's Species Check: the theme setting in the header (System, Light or Dark).
//
// The layout loads this file in <head> without defer, so data-theme is set on <html> before the
// page is drawn and a visitor who chose Dark never sees the light colours first. site.css reads
// data-theme: "light" and "dark" override the system setting, "system" follows it. The choice is
// kept in this browser's localStorage; the server sends the same page whatever the choice.
//
// The theme menu in the header is hidden until setUp runs (site.css), so without JavaScript there
// is no menu and the site follows the system setting. When storage is blocked the choice still applies, but
// only to the page it was made on.
(function () {
    "use strict";

    var KEY = "theme";
    var root = document.documentElement;

    function read() {
        try {
            var value = window.localStorage.getItem(KEY);
            return value === "light" || value === "dark" ? value : "system";
        } catch (e) {
            return "system";
        }
    }

    function save(value) {
        try {
            if (value === "system") {
                window.localStorage.removeItem(KEY);
            } else {
                window.localStorage.setItem(KEY, value);
            }
        } catch (e) {
            // Storage is blocked: the theme applies to this page only.
        }
    }

    function apply(value) {
        root.setAttribute("data-theme", value === "light" || value === "dark" ? value : "system");
    }

    apply(read());

    function setUp() {
        var choices = document.querySelectorAll("input[name=\"theme\"]");
        var name = document.getElementById("theme-name");
        if (choices.length === 0) {
            return;
        }
        // Ticks the current choice and names it in the menu button ("Theme: Dark").
        function show() {
            var current = root.getAttribute("data-theme");
            choices.forEach(function (choice) {
                choice.checked = choice.value === current;
            });
            if (name && name.getAttribute("data-" + current)) {
                name.textContent = name.getAttribute("data-" + current);
            }
        }
        show();
        choices.forEach(function (choice) {
            choice.addEventListener("change", function () {
                if (choice.checked) {
                    apply(choice.value);
                    save(choice.value);
                    show();
                }
            });
        });
        function refresh() {
            apply(read());
            show();
        }
        // A choice made in another tab of this site, or while this page was in the back/forward
        // cache.
        window.addEventListener("storage", function (event) {
            if (event.key === KEY || event.key === null) {
                refresh();
            }
        });
        window.addEventListener("pageshow", function (event) {
            if (event.persisted) {
                refresh();
            }
        });
        root.setAttribute("data-theme-ready", "");
    }

    if (document.readyState === "loading") {
        document.addEventListener("DOMContentLoaded", setUp);
    } else {
        setUp();
    }
})();
