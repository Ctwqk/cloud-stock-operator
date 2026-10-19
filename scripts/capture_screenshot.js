// Capture the local demo's web UI (docs/screenshot.png).
//
//   npm i --no-save playwright-core@1.57          # one-off, any directory on NODE_PATH
//   WEB_URL=http://localhost:9090 node scripts/capture_screenshot.js
//
// Uses the Chromium that Playwright downloaded (or CHROMIUM_PATH). Waits until the demo has
// produced trades, generates the net-worth plot and writes a full-page PNG.
const path = require("path");
const fs = require("fs");
const { chromium } = require("playwright-core");

const url = process.env.WEB_URL || "http://localhost:9090";
const out = process.env.OUT || path.join(__dirname, "..", "docs", "screenshot.png");
const minTrades = Number(process.env.MIN_TRADES || 6);

(async () => {
  const browser = await chromium.launch({
    executablePath: process.env.CHROMIUM_PATH || undefined,
  });
  try {
    const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });
    const errors = [];
    page.on("pageerror", error => errors.push(error.message));

    const deadline = Date.now() + 120_000;
    for (;;) {
      const response = await page.goto(url);
      if (!response.ok()) throw new Error(`web app returned HTTP ${response.status()}`);
      const rows = await page.locator("h2:has-text('Recent virtual trades') ~ table tbody tr").count();
      if (rows >= minTrades) break;
      if (Date.now() > deadline) throw new Error(`only ${rows} trades after 2 minutes`);
      await page.waitForTimeout(3000);
    }

    // Wait for several scheduled snapshots so the plot verifies a time series.
    for (;;) {
      await page.getByRole("button", { name: "Generate plot" }).click();
      const status = await page.locator(".msg-ok, .msg-err").textContent();
      const points = Number(status.match(/from (\d+) snapshot/)?.[1] || 0);
      if (points >= 3) break;
      if (Date.now() > deadline) throw new Error(`plot never reached 3 snapshots: ${status}`);
      await page.waitForTimeout(5000);
    }
    await page.waitForSelector("img[alt='Net-worth plot']");
    await page.waitForFunction(() => {
      const img = document.querySelector("img[alt='Net-worth plot']");
      return img && img.complete && img.naturalWidth > 0;
    });
    if (errors.length) throw new Error(`browser errors: ${errors.join("; ")}`);
    fs.mkdirSync(path.dirname(out), { recursive: true });
    await page.screenshot({ path: out, fullPage: true });
    console.log(`wrote ${out}`);
  } finally {
    await browser.close();
  }
})().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
