// Beastie Bot Species Status: copy buttons for the wikitext boxes, wikitext that updates as the
// citation options change, and name suggestions for the search boxes. The pages work without this
// file; it only adds these conveniences.
(function () {
    "use strict";

    // Copy buttons are rendered hidden and shown here, so a page without JavaScript has no dead
    // buttons. The wikitext section carries the texts so every string comes from the server. Each
    // box has its own status line beside its button: "Copied" is read out by screen readers (the
    // button shows it too); a failure message is shown, and the button says the copy failed.
    // One click listener on the document serves every button, including the buttons of wikitext
    // that setUpLiveOptions puts in place later.
    function setUpCopyButtons() {
        showCopyButtons(document);
        document.addEventListener("click", function (event) {
            var button = event.target && event.target.closest ? event.target.closest("button[data-copy]") : null;
            if (button) {
                copyFromButton(button);
            }
        });
    }

    function showCopyButtons(root) {
        root.querySelectorAll("button[data-copy]").forEach(function (button) {
            if (document.getElementById(button.getAttribute("data-copy")) && button.parentNode.querySelector(".copy-status")) {
                button.hidden = false;
            }
        });
    }

    var copyTimers = typeof WeakMap === "function" ? new WeakMap() : null;

    function copyFromButton(button) {
        var section = button.closest("[data-copy-failed]");
        var box = document.getElementById(button.getAttribute("data-copy"));
        var status = button.parentNode.querySelector(".copy-status");
        if (!section || !box || !status) {
            return;
        }
        var copiedText = section.getAttribute("data-copied") || "Copied";
        var copyText = section.getAttribute("data-copy-label") || "Copy";
        var failedText = section.getAttribute("data-copy-failed") || "";
        var failedButtonText = section.getAttribute("data-copy-failed-button") || copyText;

        if (copyTimers && copyTimers.has(button)) {
            window.clearTimeout(copyTimers.get(button));
        }
        button.textContent = copyText;
        setStatus(status, "", false);
        copy(box).then(function () {
            button.textContent = copiedText;
            setStatus(status, copiedText, false);
            var timer = window.setTimeout(function () {
                // Clear only the success message, never a later failure message.
                if (status.textContent === copiedText) {
                    setStatus(status, "", false);
                }
                if (button.textContent === copiedText) {
                    button.textContent = copyText;
                }
            }, 2000);
            if (copyTimers) {
                copyTimers.set(button, timer);
            }
        }, function () {
            selectAll(box);
            button.textContent = failedButtonText;
            setStatus(status, failedText, true);
        });
    }

    // The citation options form updates the wikitext as soon as an option changes, and the ref
    // name a moment after the visitor stops typing. The script asks the server for the page with
    // the form's new query, the same request the form would make, and puts the new copy of each
    // data-live-region element of the wikitext section in place of the old one. The form itself is
    // not replaced, so focus and typing are not interrupted. The address bar gets the page's own
    // address for the new options (data-options-url), and the "Show wikitext" links in the
    // assessment tables get their new addresses. The update button is hidden; without JavaScript
    // it submits the form. If anything goes wrong, the browser loads the page the normal way.
    function setUpLiveOptions() {
        var section = document.getElementById("wikitext");
        var form = section ? section.querySelector("form.options-form") : null;
        if (!form || !window.fetch || !window.DOMParser || !window.URLSearchParams || !window.FormData
            || !window.history || !window.history.replaceState) {
            return;
        }
        var button = form.querySelector("button[type=\"submit\"]");
        var status = form.querySelector("[data-live-status]");
        var updatedText = section.getAttribute("data-live-updated") || "";
        var tooManyText = section.getAttribute("data-live-too-many") || "";
        if (button) {
            button.hidden = true;
        }
        if (status) {
            status.hidden = false;
        }

        var lastQuery = formQuery(form);
        var typingTimer = 0;
        var statusTimer = 0;
        var sequence = 0;
        var controller = null;

        function update() {
            window.clearTimeout(typingTimer);
            var query = formQuery(form);
            if (query === lastQuery) {
                return;
            }
            var url = new URL(form.getAttribute("action") || window.location.pathname, window.location.href);
            url.hash = "";
            url.search = query;
            var target = url.toString();
            sequence += 1;
            var mine = sequence;
            if (controller) {
                controller.abort();
            }
            controller = window.AbortController ? new AbortController() : null;
            section.setAttribute("aria-busy", "true");

            fetch(target, { credentials: "same-origin", signal: controller ? controller.signal : undefined })
                .then(function (response) {
                    if (response.status === 429 && tooManyText && status) {
                        // Rate limited: say so in place and bring the button back, rather than
                        // leaving the page for the error page.
                        var limited = new Error("HTTP 429");
                        limited.rateLimited = true;
                        throw limited;
                    }
                    if (!response.ok) {
                        throw new Error("HTTP " + response.status);
                    }
                    return response.text();
                })
                .then(function (html) {
                    if (mine !== sequence) {
                        return;
                    }
                    var page = new DOMParser().parseFromString(html, "text/html");
                    var next = page.getElementById("wikitext");
                    if (!next || !replaceRegions(section, next, form)) {
                        throw new Error("The page has no matching wikitext section");
                    }
                    updateOptionLinks(page);
                    var address = next.getAttribute("data-options-url");
                    if (address) {
                        window.history.replaceState(window.history.state, "", address + window.location.hash);
                    }
                    // Boxes the server ticks itself now match the new page; the next change
                    // compares with the form as it is after that.
                    lastQuery = syncOptions(form, next, query) ? formQuery(form) : query;
                    section.removeAttribute("aria-busy");
                    announce();
                })
                .catch(function (error) {
                    if (mine !== sequence || (error && error.name === "AbortError")) {
                        return;
                    }
                    section.removeAttribute("aria-busy");
                    if (error && error.rateLimited) {
                        window.clearTimeout(statusTimer);
                        status.textContent = tooManyText;
                        if (button) {
                            button.hidden = false;
                        }
                        return;
                    }
                    window.location.assign(target + "#wikitext");
                });
        }

        function announce() {
            if (!status) {
                return;
            }
            window.clearTimeout(statusTimer);
            // Emptied first, so the same text is read out again after the next change.
            status.textContent = "";
            window.setTimeout(function () {
                status.textContent = updatedText;
                statusTimer = window.setTimeout(function () {
                    status.textContent = "";
                }, 4000);
            }, 50);
        }

        form.addEventListener("change", update);
        form.addEventListener("input", function (event) {
            if (event.target && event.target.type === "text") {
                window.clearTimeout(typingTimer);
                typingTimer = window.setTimeout(update, 400);
            }
        });
        // Enter in the ref name box submits the form.
        form.addEventListener("submit", function (event) {
            event.preventDefault();
            update();
        });
    }

    // The group page's list editor: when the list type changes, show the options of that type
    // (data-list-type="bullets" or "tables") and hide the others. Hidden options are still sent,
    // so switching back keeps them as they were.
    function setUpListType() {
        var form = document.querySelector("form.list-options");
        if (!form) {
            return;
        }
        form.addEventListener("change", function (event) {
            if (!event.target || event.target.name !== "type") {
                return;
            }
            var tables = event.target.value === "table";
            form.querySelectorAll("[data-list-type]").forEach(function (element) {
                element.hidden = (element.getAttribute("data-list-type") === "tables") !== tables;
            });
        });
    }

    // The query the form would send, as the browser would write it.
    function formQuery(form) {
        var params = new URLSearchParams();
        new FormData(form).forEach(function (value, name) {
            params.append(name, value);
        });
        return params.toString();
    }

    // Puts each data-live-region element of next (the wikitext section of the new page) in place
    // of the element with the same id. Returns false, changing nothing, when the two sections do
    // not have the same regions. Keeps the form where it is on the screen, the open state of
    // details elements that have an id, the state of data-keep-checked boxes that have an id, and
    // the focus when it was in a region.
    function replaceRegions(section, next, form) {
        var oldRegions = Array.prototype.slice.call(section.querySelectorAll("[data-live-region]"));
        var newRegions = Array.prototype.slice.call(next.querySelectorAll("[data-live-region]"));
        if (oldRegions.length !== newRegions.length) {
            return false;
        }
        var pairs = [];
        for (var i = 0; i < oldRegions.length; i++) {
            var id = oldRegions[i].id;
            var match = null;
            for (var j = 0; j < newRegions.length; j++) {
                if (newRegions[j].id === id) {
                    match = newRegions[j];
                }
            }
            if (!id || !match) {
                return false;
            }
            pairs.push([oldRegions[i], match]);
        }

        var formTop = form.getBoundingClientRect().top;
        var active = document.activeElement;
        var focusId = null;
        var focusCopy = null;
        pairs.forEach(function (pair) {
            var old = pair[0];
            if (active && old.contains(active)) {
                focusId = active.id || null;
                focusCopy = active.getAttribute("data-copy");
            }
            var replacement = document.importNode(pair[1], true);
            old.querySelectorAll("details[id]").forEach(function (details) {
                var same = replacement.querySelector("details[id=\"" + details.id + "\"]");
                if (same) {
                    same.open = details.open;
                }
            });
            // So do the boxes that show a run of pairs in the possible-duplicates panel.
            old.querySelectorAll("input[data-keep-checked][id]").forEach(function (box) {
                var same = replacement.querySelector("input[data-keep-checked][id=\"" + box.id + "\"]");
                if (same) {
                    same.checked = box.checked;
                }
            });
            old.parentNode.replaceChild(replacement, old);
            showCopyButtons(replacement);
            fitTextareas(replacement);
        });

        var focusTarget = focusId ? document.getElementById(focusId)
            : focusCopy ? section.querySelector("button[data-copy=\"" + focusCopy + "\"]") : null;
        if (focusTarget) {
            focusTarget.focus({ preventScroll: true });
        }
        var moved = form.getBoundingClientRect().top - formTop;
        if (moved !== 0) {
            window.scrollBy(0, moved);
        }
        return true;
    }

    // Checkboxes marked data-live-sync are ones the server can tick itself (the group page ticks NE
    // when CoL or Wikidata is ticked). Each takes the state its copy has in the new page's form, but
    // only when the form still sends the query the new page was made for, so a change the visitor
    // made meanwhile is never undone. Returns true when it ran.
    function syncOptions(form, next, query) {
        var fresh = next.querySelector("form.options-form");
        if (!fresh || formQuery(form) !== query) {
            return false;
        }
        var escape = window.CSS && window.CSS.escape ? window.CSS.escape : function (text) { return text; };
        form.querySelectorAll("input[data-live-sync]").forEach(function (input) {
            var copy = fresh.querySelector("input[name=\"" + escape(input.name) + "\"][value=\"" + escape(input.value) + "\"]");
            if (copy) {
                input.checked = copy.checked;
            }
        });
        return true;
    }

    // The "Show wikitext" links in the assessment tables carry the options; each takes the address
    // its copy on the new page has.
    function updateOptionLinks(page) {
        document.querySelectorAll("a[data-options-link]").forEach(function (link) {
            var key = link.getAttribute("data-options-link");
            var fresh = page.querySelector("a[data-options-link=\"" + key + "\"]");
            if (fresh) {
                link.setAttribute("href", fresh.getAttribute("href"));
            }
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
    // its own accessible list. Choosing one puts its name in the box, and the search for that name
    // opens a taxon page when one taxon in the release has the name (an old IUCN id with the same
    // scientific name opens the taxon in the release), or the results list when several do.
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
        // Two options with the same name, ignoring letter case, open the same search result, so only
        // the first is kept. Taxa in the release are listed first, so an old IUCN id with the
        // scientific name of a taxon in the release is the one left out.
        var seen = {};
        items.forEach(function (item) {
            // Offer the name the visitor is typing: the common name when it starts with the text,
            // otherwise the scientific name. The other name and the category are the label.
            var common = item.commonName || "";
            var useCommon = common && common.toLowerCase().indexOf(lower) === 0;
            var value = useCommon ? common : item.name;
            var key = value.toLowerCase();
            if (Object.prototype.hasOwnProperty.call(seen, key)) {
                return;
            }
            seen[key] = true;
            var option = document.createElement("option");
            option.value = value;
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

    // Wikitext boxes grow to fit their text, so nothing is hidden behind a scroll bar. root: the
    // element whose boxes to fit; the whole page when not given.
    function fitTextareas(root) {
        // The status update page's result box keeps its fixed height (site.css).
        (root || document).querySelectorAll(".wikitext-box textarea:not(.update-output)").forEach(function (box) {
            box.rows = 1;
            box.style.height = "auto";
            box.style.height = (box.scrollHeight + 2) + "px";
        });
    }

    function setUpTextareas() {
        fitTextareas();
        // A box inside a closed details element has no height to measure until it is opened.
        // toggle does not bubble, so the document listens in the capture phase.
        document.addEventListener("toggle", function (event) {
            var details = event.target;
            if (details instanceof HTMLDetailsElement && details.open) {
                fitTextareas(details);
            }
        }, true);
        var timer = 0;
        window.addEventListener("resize", function () {
            window.clearTimeout(timer);
            timer = window.setTimeout(function () {
                fitTextareas();
            }, 150);
        });
    }

    // Info tips (_InfoTip): each "i" button opens its help text as a popover, which the browser
    // opens and closes on click, Escape and a click outside without this script. This adds:
    // - the text placed just below the button (above it when there is no room below), and kept
    //   there while the page scrolls;
    // - opening on mouse hover and on keyboard focus. A tip opened that way closes when the
    //   pointer or focus leaves; a click keeps it open until a second click, Escape or a click
    //   outside;
    // - aria-expanded on the button, which the style uses.
    function setUpInfoTips() {
        var buttons = document.querySelectorAll("button[data-info-tip]");
        if (buttons.length === 0 || !HTMLElement.prototype.hasOwnProperty("popover")) {
            return;
        }
        buttons.forEach(function (button) {
            var tip = document.getElementById(button.getAttribute("popovertarget"));
            if (!tip) {
                return;
            }
            var pinned = false;
            var hideTimer = 0;
            var showTimer = 0;

            function isOpen() {
                return tip.matches(":popover-open");
            }
            function show() {
                window.clearTimeout(hideTimer);
                if (!isOpen()) {
                    pinned = false;
                    tip.showPopover();
                }
            }
            function hideSoon() {
                window.clearTimeout(showTimer);
                window.clearTimeout(hideTimer);
                hideTimer = window.setTimeout(function () {
                    if (!pinned && isOpen() && !button.matches(":hover") && !tip.matches(":hover")
                        && document.activeElement !== button) {
                        tip.hidePopover();
                    }
                }, 250);
            }

            button.setAttribute("aria-expanded", "false");
            button.addEventListener("click", function (event) {
                if (isOpen() && !pinned) {
                    // Open from hover or focus: the click keeps it open instead of closing it.
                    event.preventDefault();
                    pinned = true;
                    return;
                }
                pinned = !isOpen();
            });
            button.addEventListener("pointerenter", function (event) {
                if (event.pointerType === "mouse") {
                    window.clearTimeout(showTimer);
                    showTimer = window.setTimeout(show, 150);
                }
            });
            button.addEventListener("pointerleave", hideSoon);
            tip.addEventListener("pointerleave", hideSoon);
            tip.addEventListener("pointerenter", function () {
                window.clearTimeout(hideTimer);
            });
            button.addEventListener("focus", function () {
                if (button.matches(":focus-visible")) {
                    show();
                }
            });
            button.addEventListener("blur", hideSoon);
            tip.addEventListener("toggle", function (event) {
                var open = event.newState === "open";
                button.setAttribute("aria-expanded", open ? "true" : "false");
                if (open) {
                    place(button, tip);
                } else {
                    pinned = false;
                }
            });
        });

        // A placed tip has a fixed position, so it follows its button when the page scrolls or the
        // window changes size.
        function placeOpenTips() {
            buttons.forEach(function (button) {
                var tip = document.getElementById(button.getAttribute("popovertarget"));
                if (tip && tip.matches(":popover-open")) {
                    place(button, tip);
                }
            });
        }
        window.addEventListener("scroll", placeOpenTips, { passive: true });
        window.addEventListener("resize", placeOpenTips);
    }

    function place(button, tip) {
        tip.classList.add("info-tip-placed");
        var gap = 6;
        var edge = 8;
        var anchor = button.getBoundingClientRect();
        var width = tip.offsetWidth;
        var height = tip.offsetHeight;
        // The viewport without its scroll bars.
        var viewWidth = document.documentElement.clientWidth;
        var viewHeight = document.documentElement.clientHeight;
        var left = Math.max(edge, Math.min(anchor.left - 12, viewWidth - width - edge));
        var top = anchor.bottom + gap;
        if (top + height > viewHeight - edge && anchor.top - gap - height >= edge) {
            top = anchor.top - gap - height;
        }
        tip.style.left = left + "px";
        tip.style.top = top + "px";
    }

    setUpCopyButtons();
    setUpInfoTips();
    setUpListType();
    setUpLiveOptions();
    setUpSuggestions();
    setUpTextareas();
})();
