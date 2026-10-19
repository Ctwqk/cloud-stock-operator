// Capture the local demo's web UI (docs/screenshot.png).
//
//   npm i --no-save playwright-core@1.57          # one-off, any directory on NODE_PATH
//   WEB_URL=http://localhost:9090 node scripts/capture_screenshot.js
//
// Uses the Chromium that Playwright downloaded (or CHROMIUM_PATH). Waits until the demo has
// produced trades, generates the net-worth plot and writes a full-page PNG.
const path = require("path");
const { chromium } = require("playwright-core");

const url = process.env.WEB_URL || "http://localhost:9090";
const out = process.env.OUT || path.join(__dirname, "..", "docs", "screenshot.png");
const minTrades = Number(process.env.MIN_TRADES || 6);

(async () => {
  const browser = await chromium.launch({
    executablePath: process.env.CHROMIUM_PATH || undefined,
  });
  const page = await browser.newPage({ viewport: { width: 1280, height: 900 } });

  const deadline = Date.now() + 120_000;
  for (;;) {
    await page.goto(url);
    const rows = await page.locator("h2:has-text('Recent virtual trades') ~ table tbody tr").count();
    if (rows >= minTrades) break;
    if (Date.now() > deadline) throw new Error(`only ${rows} trades after 2 minutes`);
    await page.waitForTimeout(3000);
  }

  await page.getByRole("button", { name: "Generate plot" }).click();
  await page.waitForSelector("img[alt='Net-worth plot']");
  await page.waitForFunction(() => {
    const img = document.querySelector("img[alt='Net-worth plot']");
    return img && img.complete && img.naturalWidth > 0;
  });
  await page.screenshot({ path: out, fullPage: true });
  await browser.close();
  console.log(`wrote ${out}`);
})();
