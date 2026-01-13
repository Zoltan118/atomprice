import fs from "node:fs/promises";

const OUT_FILE = "data/fx_rates.json";
const ECB_XML = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml";

async function main() {
  await fs.mkdir("data", { recursive: true });

  const res = await fetch(ECB_XML, {
    headers: { accept: "application/xml,text/xml,*/*" },
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} for ECB XML`);
  const xml = await res.text();

  // Robust parse for the daily cube section:
  // <Cube time="YYYY-MM-DD"><Cube currency="USD" rate="..." /> ... </Cube>
  const timeMatch = xml.match(/<Cube\s+time="([^"]+)">/);
  const ecbDate = timeMatch ? timeMatch[1] : null;

  const rates = { EUR: 1 };
  const re = /<Cube\s+currency="([^"]+)"\s+rate="([^"]+)"\s*\/>/g;

  let m;
  while ((m = re.exec(xml)) !== null) {
    const c = m[1];
    const r = Number(m[2]);
    if (c && Number.isFinite(r)) rates[c] = r;
  }

  // Required currencies you asked for
  const need = ["USD", "GBP", "TRY", "JPY", "CNY"];
  const missing = need.filter((k) => !rates[k]);

  // If any missing, still write file (so site doesn't break), but include error for debugging.
  const out = {
    generated_at: new Date().toISOString(),
    ecb_date: ecbDate,
    base: "EUR",
    rates,
    ...(missing.length ? { error: `Missing in ECB feed: ${missing.join(", ")}` } : {}),
  };

  await fs.writeFile(OUT_FILE, JSON.stringify(out, null, 2));

  if (missing.length) {
    console.error(`❌ FX updater failed: Missing ${missing.join(", ")} in ECB feed`);
    process.exit(1);
  }

  console.log(`✅ Wrote ${OUT_FILE} (ECB date ${ecbDate})`);
}

main().catch(async (e) => {
  await fs.mkdir("data", { recursive: true });
  await fs.writeFile(
    OUT_FILE,
    JSON.stringify(
      {
        generated_at: new Date().toISOString(),
        error: String(e?.message || e),
      },
      null,
      2
    )
  );
  console.error("❌ FX updater failed:", e?.message || e);
  process.exit(1);
});
