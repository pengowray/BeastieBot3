// Beastie Bot Species Status: copy buttons for the wikitext boxes, and name suggestions for the
// search boxes. The pages work without this file; it only adds these two conveniences.
(function () {
    "use strict";

    // Copy buttons are rendered hidden and shown here, so a page without JavaScript has no dead
    // buttons. The wikitext section carries the texts so every string comes from the server. Each
    // box has its own status line beside its button: "Copied" is read out by screen readers (the
    // button shows it too); a failure message is shown, and the button says the copy failed.
    function setUpCopyButtons() {
        var section = document.querySelector("[data-copy-failed]");
        var buttons = document.querySelectorAll("button[data-copy]");
        if (!section || buttons.length === 0) {
            return;
        }
        var copiedText = section.getAttribute("data-copied") || "Copied";
        var copyText = section.getAttribute("data-copy-label") || "Copy";
        var failedText = section.getAttribute("data-copy-failed") || "";
        var failedButtonText = section.getAttribute("data-copy-failed-button") || copyText;

        buttons.forEach(function (button) {
            var box = document.getElementById(button.getAttribute("data-copy"));
            var status = button.parentNode.querySelector(".copy-status");
            if (!box || !status) {
                return;
            }
            button.hidden = false;
            var timer = 0;
            button.addEventListener("click", function () {
                window.clearTimeout(timer);
                button.textContent = copyText;
                setStatus(status, "", false);
                copy(box).then(function () {
                    button.textContent = copiedText;
                    setStatus(status, copiedText, false);
                    timer = window.setTimeout(function () {
                        // Clear only the success message, never a later failure message.
                        if (status.textContent === copiedText) {
                            setStatus(status, "", false);
                        }
                        if (button.textContent === copiedText) {
                            button.textContent = copyText;
                        }
                    }, 2000);
                }, function () {
                    selectAll(box);
                    button.textContent = failedButtonText;
                    setStatus(status, failedText, true);
                });
            });
        });
    }

    function setStatus(status, text, failed) {
        status.textContent = text;
        status.classList.toggle("copy-status-failed", failed);
    }

    // select() alone often selects nothing visible in a read-only textarea on iOS.
    function selectAll(box) {
        box.focus();
        box.select();
        try {
            box.setSelectionRange(0, box.value.length);
        } catch (e) {
            // Selection is a convenience; the failure message still tells the visitor what to do.
        }
    }

    // The Clipboard API first; when it is missing or refuses (plain http, no permission), the
    // older execCommand("copy") on the selected text.
    function copy(box) {
        if (navigator.clipboard && window.isSecureContext) {
            return navigator.clipboard.writeText(box.value).catch(function () {
                return copyBySelection(box);
            });
        }
        return copyBySelection(box);
    }

    function copyBySelection(box) {
        return new Promise(function (resolve, reject) {
            selectAll(box);
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

    // Wikitext boxes grow to fit their text, so nothing is hidden behind a scroll bar.
    function fitTextareas() {
        document.querySelectorAll(".wikitext-box textarea").forEach(function (box) {
            box.rows = 1;
            box.style.height = "auto";
            box.style.height = (box.scrollHeight + 2) + "px";
        });
    }

    function setUpTextareas() {
        fitTextareas();
        var timer = 0;
        window.addEventListener("resize", function () {
            window.clearTimeout(timer);
            timer = window.setTimeout(fitTextareas, 150);
        });
    }

    setUpCopyButtons();
    setUpSuggestions();
    setUpTextareas();
})();
