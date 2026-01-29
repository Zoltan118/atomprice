const fs = require('fs');
const https = require('https');

const COSMOS_REST = 'https://rest.cosmos.directory/cosmoshub';
const OUTPUT_FILE = 'data/historical-total-staked.json';

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
    console.log('📊 Fetching daily total staked ATOM...');

    try {
        // Fetch staking pool data
        const poolRes = await fetchJSON(`${COSMOS_REST}/cosmos/staking/v1beta1/pool`);

        console.log('Pool response:', JSON.stringify(poolRes, null, 2));

        // Get bonded tokens (total staked)
        const bondedTokensUatom = parseInt(poolRes.pool.bonded_tokens);
        const bondedTokensAtom = bondedTokensUatom / 1e6; // Convert uatom to ATOM
        const bondedTokensInMillions = bondedTokensAtom / 1e6; // Convert to millions

        console.log(`💰 Total Bonded (Staked): ${bondedTokensAtom.toLocaleString()} ATOM`);
        console.log(`   = ${bondedTokensInMillions.toFixed(2)}M ATOM`);
        console.log(`   (${bondedTokensUatom.toLocaleString()} uatom)`);

        // Get today's date in YYYY-MM-DD format
        const today = new Date().toISOString().split('T')[0];

        // Prepare new entry (store in millions for display)
        const newEntry = {
            date: today,
            total: parseFloat(bondedTokensInMillions.toFixed(2))
        };

        // Read existing data or create new array
        let historicalData = [];
        if (fs.existsSync(OUTPUT_FILE)) {
            const fileContent = fs.readFileSync(OUTPUT_FILE, 'utf8');
            historicalData = JSON.parse(fileContent);
            console.log(`📂 Loaded ${historicalData.length} existing records`);
        }

        // Check if today's entry already exists
        const existingIndex = historicalData.findIndex(entry => entry.date === today);

        if (existingIndex >= 0) {
            // Update existing entry
            const oldValue = historicalData[existingIndex].total;
            historicalData[existingIndex] = newEntry;
            console.log(`🔄 Updated entry for ${today}: ${oldValue}M → ${newEntry.total}M ATOM`);
        } else {
            // Append new entry
            historicalData.push(newEntry);
            console.log(`✅ Added new entry for ${today}: ${newEntry.total}M ATOM`);
        }

        // Sort by date (oldest to newest)
        historicalData.sort((a, b) => new Date(a.date) - new Date(b.date));

        // Ensure data directory exists
        if (!fs.existsSync('data')) {
            fs.mkdirSync('data', { recursive: true });
        }

        // Write updated data
        fs.writeFileSync(OUTPUT_FILE, JSON.stringify(historicalData, null, 2));
        console.log(`💾 Saved to ${OUTPUT_FILE} (${historicalData.length} total entries)`);

        // Show summary stats
        if (historicalData.length > 1) {
            const firstEntry = historicalData[0];
            const lastEntry = historicalData[historicalData.length - 1];
            const growth = ((lastEntry.total - firstEntry.total) / firstEntry.total) * 100;

            console.log(`\n📈 Historical Summary:`);
            console.log(`   First: ${firstEntry.date} - ${firstEntry.total}M ATOM`);
            console.log(`   Latest: ${lastEntry.date} - ${lastEntry.total}M ATOM`);
            console.log(`   Growth: ${growth > 0 ? '+' : ''}${growth.toFixed(2)}%`);
        }

    } catch (err) {
        console.error('❌ Fatal error:', err);
        process.exit(1);
    }
}

main();
