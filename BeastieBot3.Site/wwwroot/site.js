// Beastie Bot Species Status: copy buttons for the wikitext boxes, and name suggestions for the
// search boxes. The pages work without this file; it only adds these two conveniences.
(function () {
    "use strict";

    // Copy buttons are rendered hidden and shown here, so a page without JavaScript has no dead
    // buttons. The status line carries the texts so every string comes from the server.
    function setUpCopyButtons() {
        var status = document.querySelector(".copy-status");
        var buttons = document.querySelectorAll("button[data-copy]");
        if (!status || buttons.length === 0) {
            return;
        }
        var copiedText = status.getAttribute("data-copied") || "Copied";
        var copyText = status.getAttribute("data-copy-label") || "Copy";
        var failedText = status.getAttribute("data-copy-failed") || "";

        buttons.forEach(function (button) {
            var box = document.getElementById(button.getAttribute("data-copy"));
            if (!box) {
                return;
            }
            button.hidden = false;
            button.addEventListener("click", function () {
                copy(box).then(function () {
                    button.textContent = copiedText;
                    status.textContent = copiedText;
                    window.setTimeout(function () {
                        button.textContent = copyText;
                        status.textContent = "";
                    }, 2000);
                }, function () {
                    box.focus();
                    box.select();
                    status.textContent = failedText;
                });
            });
        });
    }

    function copy(box) {
        if (navigator.clipboard && window.isSecureContext) {
            return navigator.clipboard.writeText(box.value);
        }
        // Older browsers and pages served over plain http.
        return new Promise(function (resolve, reject) {
            box.focus();
            box.select();
            var ok = false;
            try {
                ok = document.execCommand("copy");
            } catch (e) {
                ok = false;
            }
            if (ok) {
                resolve();
            } else {
                reject();
            }
        });
    }

    // Suggestions from /api/suggest fill the input's <datalist>, so the browser shows them with
    // its own accessible list. Choosing one puts the name in the box; searching for it goes
    // straight to the taxon page.
    function setUpSuggestions() {
        var inputs = document.querySelectorAll("input[data-suggest]");
        inputs.forEach(function (input) {
            var list = document.getElementById(input.getAttribute("list"));
            if (!list || !window.fetch) {
                return;
            }
            var timer = 0;
            var lastQuery = "";
            var controller = null;
            input.addEventListener("input", function () {
                window.clearTimeout(timer);
                var query = input.value.trim();
                if (query.length < 3 || query === lastQuery) {
                    return;
                }
                timer = window.setTimeout(function () {
                    lastQuery = query;
                    if (controller) {
                        controller.abort();
                    }
                    controller = window.AbortController ? new AbortController() : null;
                    fetch("/api/suggest?q=" + encodeURIComponent(query), controller ? { signal: controller.signal } : {})
                        .then(function (response) { return response.ok ? response.json() : []; })
                        .then(function (items) { fill(list, query, items); })
                        .catch(function () { /* suggestions are optional */ });
                }, 350);
            });
        });
    }

    function fill(list, query, items) {
        while (list.firstChild) {
            list.removeChild(list.firstChild);
        }
        var lower = query.toLowerCase();
        items.forEach(function (item) {
            var option = document.createElement("option");
            // Offer the name the visitor is typing: the common name when it starts with the text,
            // otherwise the scientific name. The other name and the category are the label.
            var common = item.commonName || "";
            var useCommon = common && common.toLowerCase().indexOf(lower) === 0;
            option.value = useCommon ? common : item.name;
            var label = useCommon ? item.name : common;
            if (item.category) {
                label = label ? label + " (" + item.category + ")" : item.category;
            }
            if (label) {
                option.label = label;
            }
            list.appendChild(option);
        });
    }

    setUpCopyButtons();
    setUpSuggestions();
})();
