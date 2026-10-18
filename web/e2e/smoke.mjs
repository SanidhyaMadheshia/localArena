// Smoke test against a running stack (make play): guest play, Keycloak login,
// idempotent purchase. Needs Chrome/Chromium: CHROME=/path/to/chrome.
import { chromium } from 'playwright-core';
const out = process.argv[2] || '/tmp';
const base = process.env.BASE_URL || 'http://localhost:8080/';
const browser = await chromium.launch({ executablePath: process.env.CHROME || '/usr/bin/google-chrome', args: ['--use-angle=swiftshader', '--enable-unsafe-swiftshader', '--ignore-gpu-blocklist'] });
const page = await browser.newPage({ viewport: { width: 1400, height: 850 } });
const errors = [];
page.on('pageerror', e => errors.push('pageerror: ' + e.message));
page.on('console', m => { if (m.type() === 'error') errors.push('console: ' + m.text()); });
await page.goto(base);
await page.waitForFunction(() => document.getElementById('conn').dataset.state === 'open', null, { timeout: 15000 });
await page.keyboard.down('KeyD'); await page.waitForTimeout(1500); await page.keyboard.up('KeyD');
await page.waitForTimeout(1500);
console.log('guest:', await page.textContent('#conn'), '| players', await page.textContent('#count'), '| rtt', await page.textContent('#rtt'), '| mode', await page.textContent('#mode'));
console.log('labels:', await page.$$eval('.label', els => els.slice(0, 4).map(e => e.textContent)));
console.log('leaderboard:', await page.$$eval('#leaderboard li', els => els.slice(0, 3).map(e => e.textContent)));
await page.screenshot({ path: out + '/guest.png' });
await page.click('#login');
await page.waitForSelector('#username', { timeout: 15000 });
await page.fill('#username', 'alice'); await page.fill('#password', 'alice-dev-password');
await page.click('#kc-login');
await page.waitForFunction(() => !document.getElementById('account').hidden && document.getElementById('who').textContent, null, { timeout: 20000 });
await page.waitForFunction(() => document.getElementById('conn').dataset.state === 'open', null, { timeout: 15000 });
await page.waitForTimeout(2000);
console.log('account:', await page.textContent('#who'), '| credits', await page.textContent('#credits'), '| items', await page.textContent('#items'), '| url', page.url());
console.log('me label:', await page.$$eval('.label.me', els => els.map(e => e.textContent)));
const buy = await page.$$eval('#shop li', els => els.map(e => e.textContent));
console.log('shop:', buy);
const btn = await page.$('#shop li:has-text("Basic Hat") button:not([disabled])');
if (btn) { await btn.click(); await page.waitForSelector('.toast', { timeout: 10000 }); console.log('toast:', await page.textContent('.toast')); await page.waitForTimeout(1500);
  console.log('after buy: credits', await page.textContent('#credits'), '| items', await page.textContent('#items')); }
await page.keyboard.down('KeyW'); await page.waitForTimeout(1200); await page.keyboard.up('KeyW');
await page.waitForTimeout(800);
await page.screenshot({ path: out + '/account.png' });
console.log('errors:', errors.length ? errors : 'none');
if (errors.length) process.exitCode = 1;
await browser.close();
