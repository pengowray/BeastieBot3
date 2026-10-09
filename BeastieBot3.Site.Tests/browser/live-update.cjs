// Browser check of the taxon page's citation options: the wikitext updates as each option changes,
// without loading the page again, the address bar follows, copy buttons work on the new wikitext,
// and the console stays free of errors. It needs Playwright (installed globally is enough) and the
// site running on the fixture database:
//
//   SITE_FIXTURE_DB_OUT=/tmp/site-fixture.sqlite dotnet test BeastieBot3.Site.Tests --filter FixtureExport
//   ASPNETCORE_ENVIRONMENT=Production Site__DatabasePath=/tmp/site-fixture.sqlite \
//     Site__RateLimits__PagesPerMinute=10000 dotnet run --project BeastieBot3.Site --urls http://127.0.0.1:5391 &
//   node BeastieBot3.Site.Tests/browser/live-update.cjs
//
// Playwright is loaded from the script's own node_modules path when it is there, and otherwise from
// the global npm folder (npm root -g). SITE_URL changes the address (default
// http://127.0.0.1:5391); SHOTS names a folder for screenshots. Exits with 1 when a check fails.
// Only GET requests are made.
"use strict";

const fs = require("fs");
const path = require("path");

function loadPlaywright() {
    try {
        return require("playwright");
    } catch (e) {
        const globalRoot = require("child_process").execSync("npm root -g", { encoding: "utf8" }).trim();
        return require(path.join(globalRoot, "playwright"));
    }
}

const { chromium } = loadPlaywright();

const base = process.env.SITE_URL || "http://127.0.0.1:5391";
const shots = process.env.SHOTS || "";
const polarBear = 22823;
const polarBear2008 = 13045100;
const tiger = 15955;

const failures = [];
function check(ok, what) {
    console.log((ok ? "ok    " : "FAIL  ") + what);
    if (!ok) {
        failures.push(what);
    }
}

async function shot(page, name, selector) {
    if (!shots) {
        return;
    }
    fs.mkdirSync(shots, { recursive: true });
    const file = path.join(shots, name + ".png");
    if (selector) {
        await page.locator(selector).screenshot({ path: file });
    } else {
        await page.screenshot({ path: file, fullPage: true });
    }
}

// Sets a marker that only survives as long as the page is not loaded again.
async function mark(page) {
    await page.evaluate(() => { window.__liveCheck = (window.__liveCheck || 0) + 1; });
}
async function stillSamePage(page) {
    return page.evaluate(() => typeof window.__liveCheck === "number");
}

async function cite(page) {
    return page.locator("#wikitext-cite").inputValue();
}

async function waitForCite(page, test, what) {
    try {
        await page.waitForFunction(test, null, { timeout: 5000 });
        return true;
    } catch (e) {
        check(false, what + " (timed out; cite is: " + (await cite(page)) + ")");
        return false;
    }
}

(async () => {
    const browser = await chromium.launch();
    const context = await browser.newContext({ viewport: { width: 1100, height: 900 } });
    await context.grantPermissions(["clipboard-read", "clipboard-write"], { origin: base });
    const page = await context.newPage();
    const consoleErrors = [];
    page.on("console", (message) => {
        if (message.type() === "error") {
            consoleErrors.push(message.text());
        }
    });
    page.on("pageerror", (error) => consoleErrors.push(String(error)));
    const posts = [];
    page.on("request", (request) => {
        if (request.method() !== "GET" && request.method() !== "HEAD") {
            posts.push(request.method() + " " + request.url());
        }
    });

    // Polar bear: every option of the form.
    await page.goto(`${base}/species/${polarBear}/wikitext`);
    await mark(page);
    check(await page.locator("form.options-form button[type=submit]").isHidden(), "the Update wikitext button is hidden");
    check(await page.locator("[data-live-status]").isVisible() || (await page.locator("[data-live-status]").count()) === 1,
        "the status line is in place");
    check(await page.locator("#wikitext-cite").isVisible(), "the {{cite iucn}} box is shown");
    // The |author= form (not the default) changes the boxes above the form; the form stays where it was on the screen.
    const authorN = page.locator("input[name=authors][value=author]");
    await authorN.scrollIntoViewIfNeeded();
    const formTop = await page.locator("form.options-form").evaluate((form) => form.getBoundingClientRect().top);
    const boxesHeight = await page.locator("#wikitext-output").evaluate((region) => region.getBoundingClientRect().height);

    await authorN.check();
    if (await waitForCite(page, () => document.getElementById("wikitext-cite").value.includes("|author=Wiig"), "author format")) {
        check(true, "changing the author format updates {{cite iucn}}");
    }
    await page.waitForFunction(() => location.search.includes("authors=author"));
    check(page.url().includes("authors=author"), "the address bar has authors=author: " + page.url());
    check(await stillSamePage(page), "the page was not loaded again");
    await page.waitForFunction(() => document.querySelector("[data-live-status]").textContent === "Wikitext updated");
    check(true, "the status line says Wikitext updated");
    const newTop = await page.locator("form.options-form").evaluate((form) => form.getBoundingClientRect().top);
    const newBoxesHeight = await page.locator("#wikitext-output").evaluate((region) => region.getBoundingClientRect().height);
    check(Math.abs(newTop - formTop) < 2,
        `the form stays where it was on the screen (${formTop} -> ${newTop}; boxes ${boxesHeight} -> ${newBoxesHeight} px high)`);
    check((await page.locator(`a[data-options-link="${polarBear2008}"]`).getAttribute("href")).includes("authors=author"),
        "the Show wikitext link of the 2008 assessment carries the new option");
    check((await page.locator("#wikitext-speciesbox").inputValue()).includes("|author=Wiig"), "the {{Speciesbox}} box is updated too");

    await page.locator("input[name=access][value=none]").check();
    if (await waitForCite(page, () => !document.getElementById("wikitext-cite").value.includes("access-date"), "access date")) {
        check(true, "No access date removes |access-date=");
    }
    check(page.url().includes("access=none"), "the address bar has access=none");

    await page.locator("input[name=amp]").check();
    if (await waitForCite(page, () => document.getElementById("wikitext-cite").value.includes("|name-list-style=amp"), "amp")) {
        check(true, "the amp option adds |name-list-style=amp");
    }

    await page.locator("input[name=ref]").uncheck();
    if (await waitForCite(page, () => document.getElementById("wikitext-cite").value.startsWith("{{cite iucn"), "ref")) {
        check(true, "unticking the ref option removes the <ref> tags");
    }
    await page.locator("input[name=ref]").check();
    await waitForCite(page, () => document.getElementById("wikitext-cite").value.startsWith("<ref"), "ref back on");

    // Typing a ref name: one update after the typing stops, focus stays in the box.
    const refName = page.locator("input[name=refname]");
    await refName.click();
    await refName.press("Control+A");
    await refName.pressSequentially("polar", { delay: 60 });
    if (await waitForCite(page, () => document.getElementById("wikitext-cite").value.startsWith("<ref name=\"polar\">"), "ref name")) {
        check(true, "typing a ref name updates the citation");
    }
    check(await page.evaluate(() => document.activeElement && document.activeElement.name === "refname"), "focus stays in the ref name box");
    check(await page.evaluate(() => document.activeElement.value === "polar"), "the ref name box keeps the typed text");

    // Enter in the ref name box: no page load.
    await refName.press("End");
    await refName.pressSequentially("2");
    await refName.press("Enter");
    if (await waitForCite(page, () => document.getElementById("wikitext-cite").value.startsWith("<ref name=\"polar2\">"), "Enter")) {
        check(true, "Enter in the ref name box updates the citation");
    }
    check(await stillSamePage(page), "the page was still not loaded again after Enter");
    check(page.url().includes("refname=polar2"), "the address bar has the ref name: " + page.url());

    // Copy works on the new box.
    const copyButton = page.locator("button[data-copy=wikitext-cite]");
    check(await copyButton.isVisible(), "the copy button of the new {{cite iucn}} box is shown");
    await copyButton.click();
    await page.waitForFunction(() => document.querySelector("button[data-copy=wikitext-cite]").textContent === "Copied");
    const clipboard = await page.evaluate(() => navigator.clipboard.readText());
    check(clipboard === (await cite(page)), "Copy copies the updated {{cite iucn}}");
    await shot(page, "polar-bear-after-updates", "#wikitext");

    // Reloading the address in the address bar gives the same wikitext.
    const before = await cite(page);
    await page.reload();
    check((await cite(page)) === before, "loading the address again gives the same wikitext");

    // Tiger: the full given names option, and the citation template choices (the assessment has a
    // Wikidata item, so {{cite Q}} is shown by default). The item's commands come with the third
    // choice, and are on the Wikidata choice of the row.
    await page.goto(`${base}/species/${tiger}/wikitext`);
    await mark(page);
    check(await page.locator("#wikidata-cite").count() === 0, "the Wikipedia choice has no Wikidata commands by default");
    check(await page.locator("#wikitext-cite-q").isVisible(), "{{cite Q}} is shown by default");
    await page.locator("input[name=fullnames]").check();
    await page.waitForFunction(() => location.search.includes("fullnames=1"));
    check(page.url().includes("fullnames=1"), "the address bar has fullnames=1: " + page.url());
    check(await stillSamePage(page), "the full given names option updates without a page load");
    await page.locator("input[name=cite][value=iucn]").check();
    await page.waitForFunction(() => location.search.includes("cite=iucn"));
    check(await page.locator("#wikitext-cite-q").count() === 0, "choosing {{cite iucn}} takes the {{cite Q}} box away without a page load");
    await page.locator("input[name=cite][value=new]").check();
    await page.waitForFunction(() => location.search.includes("cite=new"));
    check(await page.locator("#wikitext-cite-q").isVisible(), "the third choice shows {{cite Q}}");
    check(await page.locator("#wikidata-cite a[href*='wikidata.org/wiki/Q900000001']").count() === 1,
        "and the item's section, without a page load");
    check(await stillSamePage(page), "and the page was not loaded again");
    await shot(page, "tiger-wikitext", "#wikitext");
    await page.locator(".wiki-chooser a", { hasText: "Wikidata" }).click();
    await page.waitForURL(/\/wikidata/);
    check(await page.locator("#wikidata-cite a[href*='wikidata.org/wiki/Q900000001']").count() === 1,
        "the Wikidata choice has the item link");
    check(await page.locator("#wikitext-heading").textContent() === "Cite for Wikidata", "and the heading Cite for Wikidata");

    // The theme menu: an icon button that opens three choices.
    await page.locator(".theme-menu summary").click();
    await page.locator("input[name=theme][value=dark]").check();
    check(await page.evaluate(() => document.documentElement.getAttribute("data-theme")) === "dark", "the theme menu sets Dark");
    check((await page.locator("#theme-name").textContent()) === "Theme: Dark", "and names it in the button");
    await page.keyboard.press("Escape");
    check(!(await page.locator(".theme-menu").evaluate((menu) => menu.open)), "Escape closes the theme menu");
    await page.locator(".theme-menu summary").click();
    await page.locator("input[name=theme][value=system]").check();
    await page.keyboard.press("Escape");

    // At phone width.
    await page.setViewportSize({ width: 375, height: 812 });
    await page.goto(`${base}/species/${tiger}/wikitext#wikitext`);
    await shot(page, "tiger-phone", "#wikitext");
    const overflow = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
    check(overflow <= 0, "no sideways scrolling at phone width (" + overflow + "px)");

    // At phone width the boxes wrap, so a longer citation makes them taller: the page scrolls by as
    // much, and the form stays where it was on the screen.
    await page.goto(`${base}/species/${polarBear}/wikitext`);
    const phoneAuthorN = page.locator("input[name=authors][value=author]");
    await phoneAuthorN.scrollIntoViewIfNeeded();
    const phoneTop = await page.locator("form.options-form").evaluate((form) => form.getBoundingClientRect().top);
    const phoneBoxes = await page.locator("#wikitext-output").evaluate((region) => region.getBoundingClientRect().height);
    await phoneAuthorN.check();
    await page.waitForFunction(() => location.search.includes("authors=author"));
    const phoneNewTop = await page.locator("form.options-form").evaluate((form) => form.getBoundingClientRect().top);
    const phoneNewBoxes = await page.locator("#wikitext-output").evaluate((region) => region.getBoundingClientRect().height);
    check(phoneNewBoxes !== phoneBoxes, `at phone width the boxes change height (${phoneBoxes} -> ${phoneNewBoxes} px)`);
    check(Math.abs(phoneNewTop - phoneTop) < 2, `and the form stays where it was on the screen (${phoneTop} -> ${phoneNewTop})`);
    await page.setViewportSize({ width: 1100, height: 900 });

    check(consoleErrors.length === 0, "no console errors" + (consoleErrors.length ? ": " + consoleErrors.join(" | ") : ""));

    // When the request fails, the browser loads the page the normal way.
    await page.goto(`${base}/species/${polarBear}/wikitext`);
    await mark(page);
    let failed = false;
    await page.route("**/species/**", (route) => {
        if (!failed && route.request().resourceType() === "fetch") {
            failed = true;
            return route.abort();
        }
        return route.continue();
    });
    await Promise.all([
        page.waitForNavigation(),
        page.locator("input[name=authors][value=author]").check(),
    ]);
    check(failed && !(await stillSamePage(page)), "a failed update loads the page instead");
    check(page.url().includes("authors=author") && page.url().endsWith("#wikitext"), "and that page has the new options: " + page.url());
    check((await cite(page)).includes("|author=Wiig"), "and its wikitext uses them");
    await page.unroute("**/species/**");

    // The group page's list: ticking a source updates the list in place and ticks Not Evaluated,
    // as the server does (data-live-sync).
    await page.goto(`${base}/taxa/genus/ursus/list`);
    await mark(page);
    const ne = page.locator("input[name=cat][value=NE]");
    check(!(await ne.isChecked()), "group list: Not Evaluated starts unticked");
    await page.locator("input[name=src][value=col]").check();
    await page.waitForFunction(() => location.search.includes("src=col"), null, { timeout: 5000 });
    check(await stillSamePage(page), "ticking CoL updates the list without loading the page");
    check(await ne.isChecked(), "and ticks Not Evaluated");

    // An info button opens its help text on click, and Escape closes it.
    await page.locator("[popovertarget=tip-categories]").click();
    const tipOpen = await page.locator("#tip-categories").evaluate((tip) => tip.matches(":popover-open"));
    check(tipOpen, "the Red List categories info button opens its help text");
    await page.keyboard.press("Escape");
    const tipClosed = await page.locator("#tip-categories").evaluate((tip) => !tip.matches(":popover-open"));
    check(tipClosed, "and Escape closes it");

    check(posts.length === 0, "only GET requests" + (posts.length ? ": " + posts.join(", ") : ""));
    await browser.close();
    if (failures.length > 0) {
        console.log(`\n${failures.length} check(s) failed.`);
        process.exit(1);
    }
    console.log("\nAll checks passed.");
})().catch((error) => {
    console.error(error);
    process.exit(1);
});
