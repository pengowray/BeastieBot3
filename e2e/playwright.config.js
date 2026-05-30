// @ts-check
const { defineConfig, devices } = require('@playwright/test');
const path = require('path');

// Dedicated test port + isolated job-history DB so the smoke run never touches
// a real serve instance's state. The server is launched read-only — these tests
// never POST /api/jobs, so no command (read-only, mutating, or destructive) is
// ever executed and nothing is downloaded.
const PORT = Number(process.env.BEASTIE_E2E_PORT || 8123);
const BASE_URL = `http://127.0.0.1:${PORT}`;
const REPO_ROOT = path.resolve(__dirname, '..');

module.exports = defineConfig({
  testDir: __dirname,
  fullyParallel: false,
  workers: 1,
  forbidOnly: !!process.env.CI,
  retries: 0,
  timeout: 30_000,
  expect: { timeout: 10_000 },
  reporter: process.env.CI ? [['list'], ['html', { open: 'never' }]] : [['list']],
  use: {
    baseURL: BASE_URL,
    trace: 'on-first-retry',
  },
  projects: [
    { name: 'chromium', use: { ...devices['Desktop Chrome'] } },
  ],
  webServer: {
    // --no-launch-profile: ignore launchSettings.json so our --port/--host win.
    // --job-history: write the (benign, local) job DB into the gitignored tmp dir.
    command:
      `dotnet run --project BeastieBot3/BeastieBot3.csproj --no-launch-profile -- ` +
      `serve --host 127.0.0.1 --port ${PORT} --job-history ./e2e/.tmp/web_jobs.sqlite`,
    cwd: REPO_ROOT,
    url: BASE_URL,
    reuseExistingServer: !process.env.CI,
    timeout: 180_000,
    stdout: 'pipe',
    stderr: 'pipe',
  },
});
