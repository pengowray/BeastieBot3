// Read-only smoke tests for the BeastieBot3 local web UI.
//
// SAFETY: these tests must never run a CLI command. Command execution is the
// only path that mutates data, downloads sources, or does destructive work, and
// it always goes through POST /api/jobs. beforeEach installs a hard network
// guard that ABORTS any POST to /api/jobs (and the file-mutating editor
// endpoints) and records the attempt; afterEach fails the test if anything was
// blocked. So even an accidental click cannot trigger real work — the tests
// only ever issue read-only GETs.

const { test, expect } = require('@playwright/test');

// Endpoints that change state on POST. We allow their GETs (read-only) through
// but abort POSTs so the smoke run is incapable of mutating anything.
const MUTATING_ENDPOINTS = [
  '**/api/jobs',                 // command execution (run/download/destructive)
  '**/api/jobs/*/cancel',
  '**/api/rules/apply',          // write edited rules back to source
  '**/api/rules-draft/write',
  '**/api/rules-draft/revert',
  '**/api/grouping/children',    // write sub-groups into the draft yaml
];

test.beforeEach(async ({ page }) => {
  /** @type {string[]} */
  const blocked = [];
  page.__blocked = blocked;
  for (const pattern of MUTATING_ENDPOINTS) {
    await page.route(pattern, async (route) => {
      if (route.request().method() === 'POST') {
        blocked.push(`${route.request().method()} ${route.request().url()}`);
        return route.abort();
      }
      return route.continue();
    });
  }
});

test.afterEach(async ({ page }) => {
  expect(page.__blocked, 'smoke tests must not issue any mutating/job request').toEqual([]);
});

test('dashboard loads by default with summary tiles and no running job', async ({ page }) => {
  await page.goto('/');
  await expect(page.locator('.sidebar')).toBeVisible();
  await expect(page.locator('.nav-item[data-view="dashboard"]')).toHaveClass(/active/);

  const grid = page.locator('#dash-grid');
  await expect(grid.locator('.dash-tile').first()).toBeVisible();
  // The Sources and Recent-jobs tiles always render regardless of data state.
  await expect(grid.getByRole('heading', { name: 'Data sources' })).toBeVisible();
  await expect(grid.getByRole('heading', { name: 'Recent jobs' })).toBeVisible();

  // Nothing is running, so the job dock stays hidden.
  await expect(page.locator('#job-dock')).toBeHidden();
});

test('sidebar navigates between every view and updates the hash', async ({ page }) => {
  await page.goto('/');
  const views = ['run', 'workflows', 'jobs', 'grouping', 'rules', 'settings', 'sources', 'dashboard'];
  for (const view of views) {
    await page.locator(`.nav-item[data-view="${view}"]`).click();
    await expect(page.locator(`.view[data-view="${view}"]`)).toHaveClass(/active/);
    await expect(page.locator(`.nav-item[data-view="${view}"]`)).toHaveClass(/active/);
    await expect(page).toHaveURL(new RegExp(`#/${view}$`));
  }
});

test('hash deep-link opens the target view directly', async ({ page }) => {
  await page.goto('/#/workflows');
  await expect(page.locator('.view[data-view="workflows"]')).toHaveClass(/active/);
  await expect(page.locator('.view[data-view="dashboard"]')).not.toHaveClass(/active/);
});

test('run view loads the command tree and the filter narrows it', async ({ page }) => {
  await page.goto('/#/run');
  const tree = page.locator('#command-tree');
  await expect(tree.locator('.cmd-row').first()).toBeVisible();

  const before = await tree.locator('.cmd-row').count();
  expect(before).toBeGreaterThan(1);

  await page.locator('#cmd-search').fill('generate-lists');
  await expect(tree.locator('.cmd-row', { hasText: 'generate-lists' }).first()).toBeVisible();

  const after = await tree.locator('.cmd-row').count();
  expect(after).toBeGreaterThan(0);
  expect(after).toBeLessThan(before);
});

test('expanding a command shows its auto-generated form without running it', async ({ page }) => {
  await page.goto('/#/run');
  await page.locator('#cmd-search').fill('generate-lists');
  const row = page.locator('#command-tree .cmd-row', { hasText: 'generate-lists' }).first();
  await row.click();
  // The form renders with a Run button — but we never click it.
  const form = page.locator('#command-tree .cmd-form').first();
  await expect(form).toBeVisible();
  await expect(form.locator('button.run-btn')).toBeVisible();
  // The new --status / --taxa-group filters surface as labelled fields in the
  // form. Match the field label exactly: the option descriptions also mention
  // "--status", so a substring match would be ambiguous.
  await expect(form.locator('label.field-label', { hasText: /^--status$/ })).toBeVisible();
  await expect(form.locator('label.field-label', { hasText: /^--taxa-group$/ })).toBeVisible();
});

test('data sources view renders status items (read-only)', async ({ page }) => {
  await page.goto('/#/sources');
  await expect(page.locator('#status-list .status-item').first()).toBeVisible({ timeout: 25_000 });
});

test('workflows view renders flow tabs and steps (read-only)', async ({ page }) => {
  await page.goto('/#/workflows');
  await expect(page.locator('#flow-tabs .flow-tab').first()).toBeVisible({ timeout: 25_000 });
  await expect(page.locator('#flow-content .flow-step').first()).toBeVisible({ timeout: 25_000 });
});

test('settings view loads resolved paths (read-only GET)', async ({ page }) => {
  await page.goto('/#/settings');
  await page.locator('#load-paths').click();
  await expect(page.locator('#paths-table')).toBeVisible();
  await expect(page.locator('#paths-table tbody tr').first()).toBeVisible();
});
