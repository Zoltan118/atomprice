import fs from "node:fs/promises";

const OUT_FILE = "data/fx_rates.json";
// ECB daily EUR reference rates (XML)
const ECB_XML = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml";

function getAttr(tag, name) {
  const m = tag.match(new RegExp(`${name}="([^"]+)"`));
  return m ? m[1] : null;
}

async function main() {
  await fs.mkdir("data", { recursive: true });

  const res = await fetch(ECB_XML, { headers: { "accept": "application/xml,text/xml,*/*" } });
  if (!res.ok) throw new Error(`HTTP ${res.status} for ECB XML`);
  const xml = await res.text();

  // Find the Cube time + all Cube currency/rate entries
  const timeMatch = xml.match(/<Cube time="([^"]+)">/);
  const time = timeMatch ? timeMatch[1] : null;

  const rateTags = [...xml.matchAll(/<Cube currency="[^"]+" rate="[^"]+"\/>/g)].map(m => m[0]);

  const rates = { EUR: 1 };
  for (const t of rateTags) {
    const c = getAttr(t, "currency");
    const r = getAttr(t, "rate");
    if (c && r) rates[c] = Number(r);
  }

  // We need: USD, GBP, TRY, JPY, CNY (ECB uses CNY, JPY, TRY, GBP, USD)
  const need = ["USD", "GBP", "TRY", "JPY", "CNY"];
  for (const k of need) {
    if (!rates[k]) throw new Error(`Missing ${k} in ECB feed`);
  }

  const out = {
    generated_at: new Date().toISOString(),
    ecb_date: time,
    base: "EUR",
    rates
  };

  await fs.writeFile(OUT_FILE, JSON.stringify(out, null, 2));
  console.log(`✅ Wrote ${OUT_FILE} (ECB date ${time})`);
}

main().catch(async (e) => {
  await fs.mkdir("data", { recursive: true });
  await fs.writeFile(OUT_FILE, JSON.stringify({
    generated_at: new Date().toISOString(),
    error: String(e?.message || e)
  }, null, 2));
  console.error("❌ FX updater failed:", e?.message || e);
  process.exit(1);
});
