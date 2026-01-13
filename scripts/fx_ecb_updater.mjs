import fs from "node:fs/promises";

const OUT_FILE = "data/fx_rates.json";

const ECB_XML = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml";
// Frankfurter uses ECB rates (very stable fallback)
const FRANKFURTER = "https://api.frankfurter.app/latest?from=EUR&to=USD,GBP,TRY,JPY,CNY";

const NEED = ["USD", "GBP", "TRY", "JPY", "CNY"];

function parseEcbXml(xml) {
  // If we received HTML (blocked/proxy/error), bail early
  if (!xml.includes("<Cube") || !xml.includes("eurofxref")) {
    return { rates: null, ecb_date: null, reason: "ECB response did not look like eurofxref XML" };
  }

  const timeMatch = xml.match(/<Cube\s+time="([^"]+)">/);
  const ecb_date = timeMatch ? timeMatch[1] : null;

  // Accept both attribute orders:
  // <Cube currency="USD" rate="1.23" />
  // <Cube rate="1.23" currency="USD" />
  const rates = { EUR: 1 };
  const re = /<Cube\b[^>]*\bcurrency="([A-Z]{3})"[^>]*\brate="([0-9.]+)"[^>]*\/>|<Cube\b[^>]*\brate="([0-9.]+)"[^>]*\bcurrency="([A-Z]{3})"[^>]*\/>/g;

  let m;
  while ((m = re.exec(xml)) !== null) {
    // Two possible shapes due to alternation:
    // 1) currency then rate => m[1], m[2]
    // 2) rate then currency => m[3], m[4]
    const c = m[1] || m[4];
    const rStr = m[2] || m[3];
    const r = Number(rStr);
    if (c && Number.isFinite(r)) rates[c] = r;
  }

  const missing = NEED.filter((k) => !rates[k]);
  return { rates, ecb_date, missing, reason: null };
}

async function fetchText(url) {
  const res = await fetch(url, {
    headers: {
      "accept": "application/xml,text/xml,text/plain,*/*",
      // A simple UA helps sometimes with strict CDNs
      "user-agent": "atomprice-fx-updater/1.0 (+github-actions)",
    },
  });
  const body = await res.text();
  return { ok: res.ok, status: res.status, body };
}

async function fetchJson(url) {
  const res = await fetch(url, {
    headers: {
      "accept": "application/json,*/*",
      "user-agent": "atomprice-fx-updater/1.0 (+github-actions)",
    },
  });
  const body = await res.text();
  let json = null;
  try {
    json = JSON.parse(body);
  } catch (_) {}
  return { ok: res.ok, status: res.status, body, json };
}

async function main() {
  await fs.mkdir("data", { recursive: true });

  // 1) Try ECB
  let ecbInfo = null;
  try {
    const r = await fetchText(ECB_XML);
    if (!r.ok) {
      ecbInfo = { error: `ECB HTTP ${r.status}`, details: r.body.slice(0, 200) };
    } else {
      const parsed = parseEcbXml(r.body);
      if (!parsed.rates) {
        ecbInfo = { error: parsed.reason || "ECB parse failed", details: r.body.slice(0, 200) };
      } else if (parsed.missing?.length) {
        ecbInfo = { error: `ECB missing: ${parsed.missing.join(", ")}`, ecb_date: parsed.ecb_date };
      } else {
        const out = {
          generated_at: new Date().toISOString(),
          base: "EUR",
          source: { name: "ECB", url: ECB_XML, ecb_date: parsed.ecb_date },
          rates: parsed.rates,
        };
        await fs.writeFile(OUT_FILE, JSON.stringify(out, null, 2));
        console.log(`✅ Wrote ${OUT_FILE} from ECB (date ${parsed.ecb_date})`);
        return;
      }
    }
  } catch (e) {
    ecbInfo = { error: `ECB fetch failed: ${String(e?.message || e)}` };
  }

  // 2) Fallback: Frankfurter (ECB-derived)
  try {
    const r = await fetchJson(FRANKFURTER);
    if (!r.ok || !r.json || !r.json.rates) {
      throw new Error(`Frankfurter HTTP ${r.status}`);
    }

    const rates = { EUR: 1, ...r.json.rates };
    const missing = NEED.filter((k) => !rates[k]);
    if (missing.length) {
      throw new Error(`Frankfurter missing: ${missing.join(", ")}`);
    }

    const out = {
      generated_at: new Date().toISOString(),
      base: "EUR",
      source: {
        name: "Frankfurter (ECB-derived fallback)",
        url: FRANKFURTER,
        note: "Used because ECB XML fetch/parse failed on runner",
        ecb_issue: ecbInfo,
      },
      rates,
      date: r.json.date || null,
    };

    await fs.writeFile(OUT_FILE, JSON.stringify(out, null, 2));
    console.log(`✅ Wrote ${OUT_FILE} from Frankfurter fallback`);
    return;
  } catch (e) {
    // If both fail, write an error file and fail workflow
    const out = {
      generated_at: new Date().toISOString(),
      base: "EUR",
      error: `Both ECB and fallback failed: ${String(e?.message || e)}`,
      ecb_issue: ecbInfo,
      fallback_url: FRANKFURTER,
    };
    await fs.writeFile(OUT_FILE, JSON.stringify(out, null, 2));
    console.error("❌ FX updater failed:", out.error);
    process.exit(1);
  }
}

main();
