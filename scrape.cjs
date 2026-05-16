const { chromium } = require('playwright');
(async () => {
  const browser = await chromium.launch();
  const page = await browser.newPage();
  await page.goto('https://documenter.getpostman.com/view/48018954/2sB3HksMLT');
  await page.waitForTimeout(3000);
  const content = await page.evaluate(() => document.body.innerText);
  require('fs').writeFileSync('postman_docs.txt', content);
  await browser.close();
})();
