const fs = require('fs');
const https = require('https');

const COSMOS_REST = 'https://rest.cosmos.directory/cosmoshub';
const MIN_DELEGATION = 25000; // 25K ATOM minimum
const OUTPUT_FILE = 'data/whales.json';

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

// Get timestamp of the most recent delegation for a delegator
async function getLastDelegationTimestamp(delegatorAddress) {
    try {
        // Query transactions for this delegator's delegate events
        const url = `${COSMOS_REST}/cosmos/tx/v1beta1/txs?events=delegate.delegator='${delegatorAddress}'&order_by=ORDER_BY_DESC&limit=1`;
        const data = await fetchJSON(url);

        if (data.txs && data.txs.length > 0) {
            // Get timestamp from the transaction
            const timestamp = data.txs[0].timestamp || data.tx_responses?.[0]?.timestamp;
            return timestamp || new Date().toISOString();
        }

        return new Date().toISOString(); // Fallback to current time
    } catch (e) {
        console.error(`Error fetching timestamp for ${delegatorAddress}:`, e.message);
        return new Date().toISOString(); // Fallback
    }
}

// Add delay between requests to avoid rate limiting
function delay(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function main() {
    console.log('Fetching whale delegations...');

    try {
        // 1. Get top validators
        const validatorsRes = await fetchJSON(
            `${COSMOS_REST}/cosmos/staking/v1beta1/validators?status=BOND_STATUS_BONDED&pagination.limit=180`
        );

        const topValidators = validatorsRes.validators
            .sort((a, b) => parseInt(b.tokens) - parseInt(a.tokens))
            .slice(0, 15); // Top 15 validators

        console.log(`Found ${topValidators.length} top validators`);

        let allDelegations = [];

        // 2. Get delegations for each top validator
        for (const validator of topValidators) {
            try {
                console.log(`Fetching delegations for validator ${validator.operator_address.slice(0, 20)}...`);

                const delegationsRes = await fetchJSON(
                    `${COSMOS_REST}/cosmos/staking/v1beta1/validators/${validator.operator_address}/delegations?pagination.limit=100`
                );

                if (delegationsRes.delegation_responses) {
                    const bigDelegations = delegationsRes.delegation_responses
                        .filter(d => parseInt(d.balance.amount) / 1e6 >= MIN_DELEGATION)
                        .map(d => ({
                            addr: d.delegation.delegator_address,
                            amount: parseInt(d.balance.amount) / 1e6,
                            validator: validator.operator_address
                        }));

                    allDelegations = allDelegations.concat(bigDelegations);
                    console.log(`  Found ${bigDelegations.length} delegations over ${MIN_DELEGATION}`);
                }

                await delay(500); // Wait 500ms between validator queries
            } catch (e) {
                console.error(`Error fetching delegations for validator:`, e.message);
            }
        }

        // 3. Deduplicate by delegator address (keep highest amount)
        const delegationMap = new Map();
        allDelegations.forEach(d => {
            if (!delegationMap.has(d.addr) || delegationMap.get(d.addr).amount < d.amount) {
                delegationMap.set(d.addr, d);
            }
        });

        let whales = Array.from(delegationMap.values());
        console.log(`Found ${whales.length} unique whale delegators`);

        // 4. Fetch timestamps for each whale (this is the time-consuming part)
        console.log('Fetching delegation timestamps...');
        for (let i = 0; i < whales.length; i++) {
            const whale = whales[i];
            console.log(`  [${i + 1}/${whales.length}] Fetching timestamp for ${whale.addr.slice(0, 20)}...`);

            whale.lastDelegationTime = await getLastDelegationTimestamp(whale.addr);
            await delay(1000); // Wait 1 second between timestamp queries to avoid rate limiting
        }

        // 5. Sort by amount (highest first)
        whales.sort((a, b) => b.amount - a.amount);

        // 6. Save to file
        const outputData = {
            lastUpdated: new Date().toISOString(),
            count: whales.length,
            whales: whales
        };

        // Ensure data directory exists
        if (!fs.existsSync('data')) {
            fs.mkdirSync('data', { recursive: true });
        }

        fs.writeFileSync(OUTPUT_FILE, JSON.stringify(outputData, null, 2));
        console.log(`✅ Saved ${whales.length} whale delegations to ${OUTPUT_FILE}`);

    } catch (e) {
        console.error('Fatal error:', e);
        process.exit(1);
    }
}

main();
