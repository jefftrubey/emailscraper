import { Actor } from 'apify';
import { PlaywrightCrawler } from 'crawlee';
import Papa from 'papaparse';
import fs from 'fs/promises';
import path from 'path';

await Actor.init();

const {
    csvUrl,
    useKvInput,
    urlCol = 'Staff Page URL',
    waitMs = 45000,
    headful = false,
    limit = 0,
    force = false
} = await Actor.getInput() || {};

let csvData = '';
if (useKvInput) {
    csvData = await Actor.getValue('INPUT.csv');
} else if (csvUrl) {
    const { body } = await Actor.fetch(csvUrl);
    csvData = await body.text();
} else {
    throw new Error("No CSV source specified. Provide 'csvUrl' or set 'useKvInput' to true.");
}

const parsed = Papa.parse(csvData, { header: true });
const rows = parsed.data.filter(r => r[urlCol]?.trim());
const output = [];

const crawler = new PlaywrightCrawler({
    headless: !headful,
    maxRequestsPerCrawl: limit > 0 ? limit : rows.length,
    requestHandlerTimeoutSecs: waitMs / 1000 + 10,
    requestHandler: async ({ page, request }) => {
        const row = request.userData.row;
        const found = [];

        try {
            await page.goto(request.url, { timeout: waitMs });
            await page.waitForTimeout(2000);

            const mailtos = await page.$$eval('a[href^="mailto:"]', els =>
                els.map(el => ({
                    email: el.href.replace(/^mailto:/, '').split('?')[0],
                    text: el.textContent?.trim() || ''
                }))
            );

            const nameFields = ['Name', 'First Name', 'Contact', 'Title', 'Role'];
            const clues = nameFields.flatMap(f => row[f] ? row[f].toLowerCase().split(/[\s,]+/) : []);

            for (const m of mailtos) {
                const text = m.text.toLowerCase();
                if (clues.some(clue => text.includes(clue))) {
                    found.push(m.email);
                }
            }

            if (found.length === 0 && mailtos.length === 1) {
                found.push(mailtos[0].email);
            }

            row['Found Emails'] = found.join(', ');
        } catch (err) {
            row['Found Emails'] = `ERROR: ${err.message}`;
        }

        output.push(row);
    }
});

await crawler.run(rows.map(row => ({
    url: row[urlCol],
    userData: { row }
})));

const csv = Papa.unparse(output);
await Actor.setValue('OUTPUT.csv', csv, { contentType: 'text/csv' });

await Actor.exit();