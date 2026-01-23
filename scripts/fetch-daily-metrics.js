const fs = require('fs');
const https = require('https');

const COSMOS_REST = 'https://rest.cosmos.directory/cosmoshub';
const OUTPUT_FILE = 'data/daily-metrics.json';

// Helper function to make HTTPS GET requests
function fetchJSON(url) {
    return new Promise((resolve, reject) => {
        https.get(url, (res) => {
            let data = '';
            res.on('data', (chunk) => data += chunk);
            res.on('end', () => {
                try {
                    resolve(JSON.parse(data));
                } catch (e) {
                    reject(e);
                }
            });
        }).on('error', reject);
    });
}

async function main() {
    console.log('Fetching daily on-chain metrics...');

    try {
        // Fetch staking pool data, supply, and inflation
        const [poolRes, supplyRes, inflationRes] = await Promise.all([
            fetchJSON(`${COSMOS_REST}/cosmos/staking/v1beta1/pool`),
            fetchJSON(`${COSMOS_REST}/cosmos/bank/v1beta1/supply/uatom`),
            fetchJSON(`${COSMOS_REST}/cosmos/mint/v1beta1/inflation`)
        ]);

        // Calculate staking ratio
        const bondedTokens = parseInt(poolRes.pool.bonded_tokens) / 1e6;
        const totalSupply = parseInt(supplyRes.amount.amount) / 1e6;
        const stakingRatio = (bondedTokens / totalSupply) * 100;

        // Calculate APR: inflation * (total_supply / bonded_tokens)
        const inflation = parseFloat(inflationRes.inflation) * 100;
        const apr = inflation * (totalSupply / bondedTokens);

        console.log(`Staking Ratio: ${stakingRatio.toFixed(2)}%`);
        console.log(`Inflation: ${inflation.toFixed(2)}%`);
        console.log(`Calculated APR: ${apr.toFixed(2)}%`);

        // Get today's date in YYYY-MM-DD format
        const today = new Date().toISOString().split('T')[0];

        // Prepare new entry
        const newEntry = {
            date: today,
            ratio: parseFloat(stakingRatio.toFixed(2)),
            apr: parseFloat(apr.toFixed(2)),
            timestamp: new Date().toISOString()
        };

        // Read existing data or create new array
        let dailyMetrics = [];
        if (fs.existsSync(OUTPUT_FILE)) {
            const fileContent = fs.readFileSync(OUTPUT_FILE, 'utf8');
            dailyMetrics = JSON.parse(fileContent);
        }

        // Check if today's entry already exists
        const existingIndex = dailyMetrics.findIndex(entry => entry.date === today);

        if (existingIndex >= 0) {
            // Update existing entry
            dailyMetrics[existingIndex] = newEntry;
            console.log(`✅ Updated existing entry for ${today}`);
        } else {
            // Append new entry
            dailyMetrics.push(newEntry);
            console.log(`✅ Added new entry for ${today}`);
        }

        // Sort by date (oldest to newest)
        dailyMetrics.sort((a, b) => new Date(a.date) - new Date(b.date));

        // Ensure data directory exists
        if (!fs.existsSync('data')) {
            fs.mkdirSync('data', { recursive: true });
        }

        // Write updated data
        fs.writeFileSync(OUTPUT_FILE, JSON.stringify(dailyMetrics, null, 2));
        console.log(`✅ Saved to ${OUTPUT_FILE} (${dailyMetrics.length} total entries)`);

    } catch (err) {
        console.error('Fatal error:', err);
        process.exit(1);
    }
}

main();
