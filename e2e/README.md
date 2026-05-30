# Web UI smoke tests (Playwright)

Read-only end-to-end smoke tests for the BeastieBot3 local web UI (the `serve`
command). They check that the shell loads, the sidebar navigation and hash
routing work, the dashboard tiles render, the command browser + filter work, and
the data-sources / workflows / settings views populate from their read-only APIs.

## Safety: these tests never run a command

Running a CLI command is the only path that mutates data, downloads sources, or
does destructive work — and it always goes through `POST /api/jobs`. Each test
installs a network guard (`beforeEach` in `smoke.spec.js`) that **aborts** any
`POST` to `/api/jobs` and the file-mutating editor endpoints, and `afterEach`
fails the test if anything was blocked. So the suite is structurally incapable of
triggering real work — it only issues read-only `GET`s.

The server is launched on a dedicated port (`8123`) with an isolated, gitignored
job-history database (`e2e/.tmp/web_jobs.sqlite`), so it never touches a real
`serve` instance's state.

## Running

Requires the .NET SDK (to build/run `serve`) and Node.js. Chromium is pinned to
the revision bundled with `@playwright/test@1.60.0`.

```bash
cd e2e
npm install                 # installs @playwright/test (browser already cached)
npx playwright install chromium   # one-time, no-op if the browser is present
npm test                    # builds + serves on :8123, runs the smoke tests
```

`npm test` starts the server automatically via Playwright's `webServer` config
and shuts it down afterwards. Use `npm run test:headed` to watch it in a browser,
and `npm run report` to open the last HTML report.
