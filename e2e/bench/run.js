const { chromium, firefox } = require('playwright');
(async () => {
  for (const [bname, b] of [['chromium', chromium], ['firefox', firefox]]) {
    let browser;
    try { browser = await b.launch(); } catch (e) { console.log(bname, 'unavailable:', e.message.split('\n')[0]); continue; }
    const page = await browser.newPage();
    page.on('pageerror', e => console.log('PAGEERROR', e.message));
    await page.goto('file:///' + __dirname.replace(/\\/g,'/') + '/bench.html');
    for (const which of ['new', 'old']) {
      const r = await page.evaluate(async (which) => run(which === 'new' ? newTerminal : oldTerminal, which), which);
      console.log(bname, JSON.stringify(r));
    }
    await browser.close();
  }
})();
