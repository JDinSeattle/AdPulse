// Fresh browser context; real local Grafana/Flink pages, no user profile or mock data.
import { createRequire } from 'node:module';
import { mkdir, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
const require = createRequire(import.meta.url);
const { chromium } = require(process.env.ADPULSE_PLAYWRIGHT_MODULE || 'playwright');
const output = resolve(process.env.ADPULSE_RECORDING_OUTPUT || 'artifacts/recording');
await mkdir(output, {recursive: true});
const browser = await chromium.launch({headless: true,
  ...(process.env.ADPULSE_CHROME_PATH ? {executablePath: process.env.ADPULSE_CHROME_PATH} : {})});
const context = await browser.newContext({viewport:{width:1440,height:1000},
  recordVideo:{dir:output,size:{width:1440,height:1000}}});
const page = await context.newPage();
page.setDefaultTimeout(30000);
const started = Date.now();
const chapters = [];
async function chapter(name) {
  chapters.push({name,at_seconds:Math.round((Date.now()-started)/100)/10,url:page.url()});
  await page.screenshot({path:resolve(output,`${chapters.length}.png`)});
}
try {
  await page.goto('http://localhost:13000/login');
  await page.locator('input[name=user]').fill('admin');
  await page.locator('input[name=password]').fill('adpulse-local');
  await page.getByRole('button',{name:'Log in',exact:true}).click();
  await page.waitForURL(url=>!url.pathname.endsWith('/login'));
  await page.goto('http://localhost:13000/d/adpulse-overview?from=now-15m&to=now&refresh=10s&kiosk');
  await page.locator('canvas').first().waitFor({timeout:30000});
  await chapter('Live synthetic advertising metrics');
  await page.waitForTimeout(10000);
  await page.mouse.move(1210,810);
  await page.mouse.wheel(0,680);
  await page.waitForTimeout(2500);
  await chapter('Worker health, consumer lag and checkpoints');
  await page.waitForTimeout(8000);
  await page.mouse.wheel(0,780);
  await page.waitForTimeout(2000);
  await chapter('Governance and bounded query telemetry');
  await page.waitForTimeout(7000);
  await page.goto('http://localhost:18081/#/job/running');
  await page.getByText('AdPulse · cleaning and quality · rules-v1',{exact:true}).waitFor();
  await chapter('Two real Flink jobs after state migration');
  await page.waitForTimeout(7000);
  await page.getByText('AdPulse · attribution and metrics · explicit-click-v1',{exact:true}).click();
  await page.getByText('Checkpoints',{exact:true}).first().waitFor();
  await chapter('Attribution operator topology');
  await page.waitForTimeout(7000);
  await page.getByText('Checkpoints',{exact:true}).first().click();
  await page.getByText('Latest Completed Checkpoint',{exact:true}).first().waitFor();
  await page.waitForTimeout(2000);
  await chapter('Persistent checkpoints and restored state');
  await page.waitForTimeout(8000);
} finally {
  const video = page.video();
  await context.close();
  const videoPath = await video.path();
  await browser.close();
  await writeFile(resolve(output,'chapters.json'),JSON.stringify({
    environment:'local Docker services; fresh headless Chrome browser',synthetic_data:true,
    captured_at:new Date(started).toISOString(),chapters,video:videoPath,
    raw_recording:true,narration:false},null,2)+'\n');
  console.log(videoPath);
}
