# Job-dock terminal benchmark

Times the browser cost of rendering job output in `BeastieBot3/Web/wwwroot/terminal.js`
against the pre-September-2026 code in `old-terminal.js`, for the three shapes a
long job produces: a 200k-line replay in one chunk, a 60k carriage-return
redraw in one chunk, and 100 events of 1,000 lines streamed at 20 per second.

```bash
cd e2e/bench
NODE_PATH=$(npm root -g) node run.js      # uses the global Playwright install
```

Runs in Chromium and, if `npx playwright install firefox` has been done, Firefox.
Firefox is the one that died on the original bug, so check it there too.
