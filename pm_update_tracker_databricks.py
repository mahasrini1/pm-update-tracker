# Databricks notebook source
# MAGIC %md
# MAGIC # AstraZeneca PM Update Tracker (Databricks)
# MAGIC
# MAGIC Lists every AstraZeneca product named as the Canadian Reference Product (CRP) in a generic drug submission in
# MAGIC Canada, with the CRP's own Notices of Compliance (NOCs), DINs and product details, set against the generic
# MAGIC products filed against it and whether those generics are on the market.
# MAGIC To track another company, change `COMPANY` and `COMPANY_NAMES` in section 2.
# MAGIC
# MAGIC This one notebook replaces the GitHub project (`fetch_data.py`, `build.py`, `assemble.py`, `template.html`
# MAGIC and the nightly GitHub Actions workflow). Running it top to bottom:
# MAGIC
# MAGIC 1. downloads the Health Canada NOC and Drug Product Database (DPD) API data into a Unity Catalog volume,
# MAGIC 2. joins it into one record per CRP (same logic as `scripts/build.py`),
# MAGIC 3. saves the result as Delta tables (`pm_tracker_crps`, `pm_tracker_nocs`) for SQL and dashboards,
# MAGIC 4. writes the interactive tracker page to the volume as `astrazeneca_pm_update_tracker.html` and shows it below the last cell.
# MAGIC
# MAGIC **Nightly refresh:** create a Databricks Job that runs this notebook on the schedule `0 0 9 * * ?` (09:00 UTC,
# MAGIC after Health Canada's nightly refresh). The cluster needs outbound internet access to `health-products.canada.ca`.
# MAGIC
# MAGIC Data: Health Canada, [Open Government Licence – Canada](https://open.canada.ca/en/open-government-licence-canada).

# COMMAND ----------

# Settings. Change them in the widgets at the top of the notebook, or pass them as Job parameters.
dbutils.widgets.text("catalog", "main", "Catalog")
dbutils.widgets.text("schema", "pm_update_tracker", "Schema")
dbutils.widgets.text("volume", "raw_data", "Volume")
dbutils.widgets.dropdown("download", "yes", ["yes", "no"], "Download fresh data")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VOLUME = dbutils.widgets.get("volume")
DOWNLOAD = dbutils.widgets.get("download") == "yes"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.`{VOLUME}`")

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
RAW_DIR = f"{VOLUME_PATH}/raw"  # downloaded API JSON (the old raw/ folder)
HTML_PATH = f"{VOLUME_PATH}/astrazeneca_pm_update_tracker.html"  # the old site/index.html
CRPS_TABLE = f"`{CATALOG}`.`{SCHEMA}`.pm_tracker_crps"
NOCS_TABLE = f"`{CATALOG}`.`{SCHEMA}`.pm_tracker_nocs"
print("raw data:", RAW_DIR, "| page:", HTML_PATH, "| tables:", CRPS_TABLE, NOCS_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Download the Health Canada data (was `scripts/fetch_data.py`)

# COMMAND ----------

import json
import os
import time
import urllib.request

NOC_API = "https://health-products.canada.ca/api/notice-of-compliance"
DPD_API = "https://health-products.canada.ca/api/drug"
DOWNLOADS = [
    (NOC_API, "noticeofcompliancemain", ""),
    (NOC_API, "drugproduct", ""),
    (NOC_API, "medicinalingredient", ""),
    (NOC_API, "dosageform", ""),
    (NOC_API, "route", ""),
    (DPD_API, "drugproduct", "dpd"),
    (DPD_API, "status", "dpd"),
]


def fetch(url, attempts=4):
    req = urllib.request.Request(url, headers={"User-Agent": "pm-update-tracker"})
    for i in range(attempts):
        try:
            with urllib.request.urlopen(req, timeout=600) as r:
                body = r.read()
            data = json.loads(body)  # fail fast on an error page instead of saving it
            if not isinstance(data, list) or not data:
                raise ValueError(f"unexpected response: {body[:200]!r}")
            return body
        except Exception as e:  # network hiccups on large downloads: retry with backoff
            if i == attempts - 1:
                raise
            print(f"  retrying {url} after error: {e}")
            time.sleep(15 * (i + 1))


def download_all(out_dir):
    for base, endpoint, sub in DOWNLOADS:
        folder = os.path.join(out_dir, sub)
        os.makedirs(folder, exist_ok=True)
        url = f"{base}/{endpoint}/?lang=en&type=json"
        print(f"Downloading {url}")
        body = fetch(url)
        with open(os.path.join(folder, f"{endpoint}.json"), "wb") as f:
            f.write(body)
        print(f"  {len(body) / 1e6:.1f} MB")

# COMMAND ----------

if DOWNLOAD:
    download_all(RAW_DIR)
else:
    print("Skipping download; using the files already in", RAW_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Build one record per AstraZeneca CRP (was `scripts/build.py`)

# COMMAND ----------

import collections as C
import difflib
import re

COMPANY = "AstraZeneca"
# spellings that identify the company (and its predecessors) in NOC company names; matched by close spelling
COMPANY_NAMES = ["ASTRAZENECA", "ZENECA", "ASTRA PHARMA"]

INNOVATOR_TYPES = {"New Drug Submission", "Supplement to a New"}
MARKET_ORDER = ["Marketed", "Approved", "Dormant"]
RANK = {s: i for i, s in enumerate(MARKET_ORDER)}
FORMWORDS = (r"\b(TABLETS?|CAPLETS?|CAPSULES?|CAPS?|TABS?|LIQUI-?GELS?|INJECTION|INJ|SOLUTION|"
             r"SUSPENSION|CREAM|OINTMENT|SYRUP|ORAL|FOR|XR|SR)\b")
_COMPANY_KEYS = [re.sub(r"[^A-Z]", "", n) for n in COMPANY_NAMES]


def norm(s):
    return re.sub(r"\s+", " ", (s or "").strip().upper())


def is_company(name):
    """True when a company name is the tracked company, allowing typos such as ASTRZENECA or ASTRA ZENECA."""
    words = re.sub(r"[^A-Z ]", " ", norm(name)).split()
    cands = words + [a + b for a, b in zip(words, words[1:])]
    return any(difflib.SequenceMatcher(None, w, k).ratio() >= 0.85 for w in cands for k in _COMPANY_KEYS)


def core(s):  # drop strengths such as "50 MG/ML"
    s = re.sub(r"\d+(\.\d+)?\s*(MG|MCG|G|ML|%|IU|UNITS?)(/\S+)?", "", norm(re.sub(r"[®™]", "", s)))
    return re.sub(r"\s+", " ", s).strip(" ,-/")


def stripped(s):  # also drop dosage-form words and punctuation
    s = core(s).replace("&", " AND ").replace("+", " AND ")
    s = re.sub(r"[^A-Z0-9 ]", " ", re.sub(FORMWORDS, " ", s))
    return re.sub(r"\s+", " ", s).strip()


def first_word(s):
    return norm(s).split(" ")[0] if s else ""


def build(src):
    # tables have no row order, so fix one: ties below (most common company, product order) stay stable
    main = sorted(src["noticeofcompliancemain"], key=lambda r: (r["noc_date"], str(r["noc_number"])))
    inactive = {r["noc_number"] for r in main if str(r["noc_active_status"]).strip() != "1"}
    main = [r for r in main if r["noc_number"] not in inactive]  # keep active NOC records only
    prods = sorted((p for p in src["drugproduct"] if p["noc_number"] not in inactive),
                   key=lambda p: (str(p["noc_number"]), str(p["noc_br_product_id"])))
    ingr = sorted(src["medicinalingredient"],
                  key=lambda r: (str(r["noc_pi_din_product_id"]), norm(r["noc_pi_medic_ingr_name"])))
    dpd_prods = sorted(src["dpd/drugproduct"], key=lambda p: str(p["drug_code"]))
    dpd_status = {s["drug_code"]: s["status"] for s in src["dpd/status"]}

    noc = {r["noc_number"]: r for r in main}

    ing_by_prod = C.defaultdict(list)
    for r in ingr:
        s = r["noc_pi_strength"]
        s = ("%g" % s) if isinstance(s, (int, float)) else (s or "")
        ing_by_prod[r["noc_pi_din_product_id"]].append(
            (norm(r["noc_pi_medic_ingr_name"]), f"{s} {r['noc_pi_unit'] or ''}".strip()))
    form_by_prod = C.defaultdict(set)
    for r in src["dosageform"]:
        form_by_prod[r["noc_pf_din_product_id"]].add(r["noc_pf_form_name"].strip())
    route_by_prod = C.defaultdict(set)
    for r in src["route"]:
        route_by_prod[r["noc_pr_din_product_id"]].add(r["noc_pr_route"].strip())

    prods_by_noc = C.defaultdict(list)
    nocs_by_brand = C.defaultdict(set)
    for p in prods:
        prods_by_noc[p["noc_number"]].append(p)
        nocs_by_brand[norm(p["noc_br_brandname"])].add(p["noc_number"])

    # current Drug Product Database status of every DIN (a DIN listed twice keeps its most active status)
    din_status = {}
    for p in dpd_prods:
        stat = dpd_status.get(p["drug_code"], "")
        din = p["drug_identification_number"]
        if din not in din_status or RANK.get(stat, 9) < RANK.get(din_status[din], 9):
            din_status[din] = stat

    def products(n):
        """Product rows for one NOC: [brand, din, ingredients, form, route, DPD status of the DIN]."""
        rows = []
        for p in prods_by_noc.get(n, []):
            pid = p["noc_br_product_id"]
            ings = "; ".join(f"{i} {s}".strip() for i, s in ing_by_prod.get(pid, []))
            din = (p["noc_br_din"] or "").strip()
            din = "" if din.upper() in ("N/A", "") else din
            rows.append([p["noc_br_brandname"].strip(), din, ings, ", ".join(sorted(form_by_prod.get(pid, []))),
                         ", ".join(sorted(route_by_prod.get(pid, []))), din_status.get(din, "")])
        return rows

    def ingredient_names(nocs):
        return {i for n in nocs for p in prods_by_noc.get(n, []) for i, _ in ing_by_prod.get(p["noc_br_product_id"], [])}

    crp_refs = C.defaultdict(list)  # generic submissions grouped by the CRP they reference
    for r in main:
        name = norm(r["noc_crp_product_name"])
        if name:
            crp_refs[name].append(r)

    used_nocs, crps = set(), []
    for name, refs in crp_refs.items():
        # the CRP's own (innovator) NOCs: brand name matches, NDS / SNDS submission
        own = sorted((n for n in nocs_by_brand.get(name, ()) if noc[n]["noc_on_submission_type"] in INNOVATOR_TYPES),
                     key=lambda n: (noc[n]["noc_date"], str(n)))
        # keep only the company's CRPs: a generic filing names it as CRP company, or it holds the CRP's NOCs
        if not (any(is_company(r["noc_crp_company_name"]) for r in refs)
                or any(is_company(noc[n]["noc_manufacturer_name"]) for n in own)):
            continue
        # most common spelling; on a tie, the one in the earliest filing (refs are in date order)
        company = C.Counter(norm(r["noc_crp_company_name"]) for r in refs).most_common(1)[0][0]
        country = C.Counter(norm(r["noc_crp_country_name"]) for r in refs).most_common(1)[0][0]
        gen = [r["noc_number"] for r in refs]  # generic NOCs, oldest first
        generic_makers = {norm(r["noc_manufacturer_name"]) for r in refs}

        # product details come from the CRP's own NOCs; fall back to the generics that reference it
        detail_src = own or gen
        ings = ingredient_names(detail_src)
        dins, pforms, proutes = set(), set(), set()
        for n in own:
            for b, din, *_ in products(n):
                if norm(b) == name and din:
                    dins.add(din)
        for n in detail_src:
            for p in prods_by_noc.get(n, []):
                pforms |= form_by_prod.get(p["noc_br_product_id"], set())
                proutes |= route_by_prod.get(p["noc_br_product_id"], set())

        tclass = next((noc[n]["noc_therapeutic_class"].strip() for n in reversed(own)
                       if noc[n]["noc_therapeutic_class"].strip()), "")
        ptype = C.Counter(noc[n]["noc_product_type"] for n in detail_src).most_common(1)[0][0]
        gen_prods = [p for n in gen for p in products(n)]
        gen_dins = sorted({p[1] for p in gen_prods if p[1]})
        gen_status = {din_status.get(x, "") for x in gen_dins}
        status = ("market" if "Marketed" in gen_status else
                  "approved" if gen_status & {"Approved", "Dormant"} else "none")

        used_nocs.update(own, gen)
        crps.append({
            "n": name, "co": company, "ct": country, "pt": ptype, "tc": tclass,
            "ing": sorted(ings), "fm": sorted(pforms), "rt": sorted(proutes),
            "din": sorted(dins), "own": own, "gen": gen, "st": status,
            "gb": sorted({p[0] for p in gen_prods}), "gd": gen_dins,
            "gdm": sorted(x for x in gen_dins if din_status.get(x) == "Marketed"),
            "gn": len(refs), "gm": len(generic_makers), "gc": sorted(generic_makers),
            "first": min(r["noc_date"] for r in refs),
        })

    nocs_out = {}
    for n in used_nocs:
        r = noc[n]
        nocs_out[n] = [r["noc_date"], r["noc_manufacturer_name"].strip(), r["noc_on_submission_type"].strip(),
                       r["noc_submission_class"].strip(), r["noc_reason_submission"].strip(),
                       r["noc_reason_supplement"].strip(), r["noc_status_with_conditions"], products(n)]

    # ---- CRP market status from the Drug Product Database ----
    by_din, by_name, by_core, by_strip = (C.defaultdict(list) for _ in range(4))
    for p in dpd_prods:
        n = norm(p["brand_name"])
        by_din[p["drug_identification_number"]].append(p)
        by_name[n].append(p)
        by_core[core(n)].append(p)
        by_strip[stripped(n)].append(p)
    strip_keys = sorted(by_strip)

    def same_company(p, c):
        return (first_word(p["company_name"]) == first_word(c["co"])
                or (is_company(p["company_name"]) and is_company(c["co"])))

    def dpd_match(c):
        """Find the CRP's products in the DPD: by DIN, then brand name, then looser name matches."""
        m = [p for din in c["din"] for p in by_din.get(din, [])]
        if m:
            return m, "DIN"
        m = by_name.get(c["n"], [])
        how = "brand name"
        if not m:
            for part in (x.strip() for x in re.split(r"[,/;]", c["n"]) if x.strip()):
                m = m + (by_name.get(part, []) or by_core.get(core(part), []))
        if not m:
            k = core(c["n"])
            m = [p for n, ps in by_name.items() if n.startswith(k + " ") for p in ps if same_company(p, c)]
        if not m:
            m = by_strip.get(stripped(c["n"]), [])
        if not m:
            k = stripped(c["n"])
            if len(k) >= 5:
                cand = difflib.get_close_matches(k, [x for x in strip_keys if x[:1] == k[:1]], n=1, cutoff=0.86)
                if cand:
                    m, how = by_strip[cand[0]], "close spelling"
        # a generic-style name can hit several companies: prefer the CRP company's own products
        same_co = [p for p in m if same_company(p, c)]
        return (same_co or m), (how if m else "")

    for c in crps:
        m, how = dpd_match(c)
        statuses = {dpd_status.get(p["drug_code"], "") for p in m}
        c["ms"] = next((s for s in MARKET_ORDER if s in statuses), "Cancelled" if m else "Not found")
        c["mh"] = how
        c["mp"] = sorted({(p["drug_identification_number"], p["brand_name"].strip(),
                           dpd_status.get(p["drug_code"], ""), p["company_name"].strip()) for p in m},
                         key=lambda x: (x[2] != "Marketed", x[0]))[:40]
        if not c["din"]:  # pre-1994 CRPs have no NOC DINs; use the DPD ones
            c["din"] = sorted({x[0] for x in c["mp"]})

    crps.sort(key=lambda c: c["n"])
    latest = max(r["noc_date"] for r in main)

    # precomputed fields for the page
    for c in crps:
        c["hay"] = " ".join([c["n"], c["co"], c["tc"], " ".join(c["ing"]), " ".join(c["din"]), " ".join(c["gb"]),
                             " ".join(c["gd"]), " ".join(c["gc"])]).lower()
        c["latest"] = nocs_out[c["own"][-1]][0] if c["own"] else ""
        c["genLatest"] = nocs_out[c["gen"][-1]][0] if c["gen"] else ""
    lc = {n: (r[4].lower(), r[5].lower()) for n, r in nocs_out.items()}
    return {"asOf": latest, "company": COMPANY, "crps": crps, "nocs": nocs_out, "lc": lc}

# COMMAND ----------

SOURCES = ["noticeofcompliancemain", "drugproduct", "medicinalingredient", "dosageform", "route",
           "dpd/drugproduct", "dpd/status"]
data = build({k: json.load(open(f"{RAW_DIR}/{k}.json", encoding="utf-8")) for k in SOURCES})
print(f"{COMPANY} CRPs:", len(data["crps"]), dict(C.Counter(c["st"] for c in data["crps"])),
      "| CRP market status:", dict(C.Counter(c["ms"] for c in data["crps"])), "| NOCs:", len(data["nocs"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Save as Delta tables
# MAGIC
# MAGIC `pm_tracker_crps` has one row per AstraZeneca CRP; `pm_tracker_nocs` has one row per NOC: the CRP's own NOCs
# MAGIC and the generic NOCs that reference it. Join them with `explode(crp_nocs)` or `explode(generic_nocs)` on
# MAGIC `noc_number`.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (ArrayType, IntegerType, LongType, StringType, StructField, StructType)

STATUS_LABEL = {"market": "Generic on market", "approved": "Generic approved, not marketed", "none": "No active generic"}
strs = ArrayType(StringType())

crp_schema = StructType([
    StructField("crp_product", StringType()),
    StructField("crp_company", StringType()),
    StructField("crp_country", StringType()),
    StructField("product_type", StringType()),
    StructField("therapeutic_class", StringType()),
    StructField("ingredients", strs),
    StructField("dosage_forms", strs),
    StructField("routes", strs),
    StructField("crp_dins", strs),
    StructField("crp_nocs", ArrayType(LongType())),
    StructField("generic_nocs", ArrayType(LongType())),
    StructField("generic_status_code", StringType()),
    StructField("generic_status", StringType()),
    StructField("generic_brands", strs),
    StructField("generic_companies", strs),
    StructField("generic_dins", strs),
    StructField("generic_dins_marketed", strs),
    StructField("generic_noc_count", IntegerType()),
    StructField("generic_filers", IntegerType()),
    StructField("first_generic_noc_date", StringType()),
    StructField("market_status", StringType()),
    StructField("market_matched_by", StringType()),
    StructField("dpd_products", ArrayType(StructType([
        StructField("din", StringType()), StructField("brand_name", StringType()),
        StructField("status", StringType()), StructField("company", StringType())]))),
])
crp_rows = [(
    c["n"], c["co"], c["ct"], c["pt"], c["tc"], c["ing"], c["fm"], c["rt"], c["din"],
    [int(n) for n in c["own"]], [int(n) for n in c["gen"]], c["st"], STATUS_LABEL[c["st"]],
    c["gb"], c["gc"], c["gd"], c["gdm"], c["gn"], c["gm"], c["first"], c["ms"], c["mh"], [list(p) for p in c["mp"]],
) for c in data["crps"]]

noc_schema = StructType([
    StructField("noc_number", LongType()),
    StructField("noc_date", StringType()),
    StructField("manufacturer", StringType()),
    StructField("submission_type", StringType()),
    StructField("submission_class", StringType()),
    StructField("reason_for_submission", StringType()),
    StructField("reason_for_supplement", StringType()),
    StructField("with_conditions", StringType()),
    StructField("products", ArrayType(StructType([
        StructField("brand_name", StringType()), StructField("din", StringType()),
        StructField("ingredients", StringType()), StructField("dosage_form", StringType()),
        StructField("route", StringType()), StructField("dpd_status", StringType())]))),
])
noc_rows = [(int(n), *r[:7], [list(p) for p in r[7]]) for n, r in data["nocs"].items()]

as_of = F.to_date(F.lit(data["asOf"]))
crps_df = (spark.createDataFrame(crp_rows, crp_schema)
           .withColumn("first_generic_noc_date", F.to_date("first_generic_noc_date"))
           .withColumn("company", F.lit(COMPANY))
           .withColumn("data_as_of", as_of))
nocs_df = (spark.createDataFrame(noc_rows, noc_schema)
           .withColumn("noc_date", F.to_date("noc_date"))
           .withColumn("data_as_of", as_of))

crps_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(CRPS_TABLE)
nocs_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(NOCS_TABLE)
print(f"Wrote {crps_df.count():,} rows to {CRPS_TABLE} and {nocs_df.count():,} rows to {NOCS_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Build and show the tracker page (was `scripts/assemble.py` + `src/template.html`)
# MAGIC
# MAGIC The page is the same one that was published on GitHub Pages: filters, status tiles, expandable NOC history and
# MAGIC "Copy results as table". It is saved to the volume as `astrazeneca_pm_update_tracker.html`, so you can download it from
# MAGIC Catalog Explorer and share it, and it is shown in the cell below.

# COMMAND ----------

TEMPLATE_HTML = r'''<title>PM Update Tracker</title>
<meta name="claude:viewport-fit-sides" content="cover">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500&family=IBM+Plex+Sans+Condensed:wght@500;600&family=IBM+Plex+Sans:wght@400;500;600&display=swap">
<style>
/* Layout: a regulatory ledger. Status summary, filter bench, then one dense row per CRP that unfolds into its NOC history. */
:root {
  --bg: #f5f7f8;
  --surface: #ffffff;
  --sunken: #eef2f3;
  --fg: #17242a;
  --muted: #5b6b72;
  --line: #d6dee1;
  --accent: #0d5f6d;
  --accent-soft: #dcecef;
  --mark: #ffe58a;
  --mark-fg: #17242a;
  --ok: #1d7a46; --ok-soft: #dcf1e4;
  --warn: #946200; --warn-soft: #fbefd2;
  --no: #a8362b; --no-soft: #f8e1de;
  --font-display: "IBM Plex Sans Condensed", "Arial Narrow", system-ui, sans-serif;
  --font-body: "IBM Plex Sans", system-ui, -apple-system, "Segoe UI", sans-serif;
  --font-data: "IBM Plex Mono", ui-monospace, "Cascadia Mono", Consolas, monospace;
}
@media (prefers-color-scheme: dark) { :root:not([data-theme="light"]) {
  --bg: #0f171a; --surface: #152126; --sunken: #1b2a30; --fg: #e3ebee; --muted: #93a6ad; --line: #2a3b42;
  --accent: #5cc3d3; --accent-soft: #173b43; --mark: #6b5410; --mark-fg: #fff3c4;
  --ok: #5fd08f; --ok-soft: #173826; --warn: #f0bd55; --warn-soft: #3a2e12; --no: #f08a7f; --no-soft: #40201c;
  color-scheme: dark; } }
:root[data-theme="dark"] {
  --bg: #0f171a; --surface: #152126; --sunken: #1b2a30; --fg: #e3ebee; --muted: #93a6ad; --line: #2a3b42;
  --accent: #5cc3d3; --accent-soft: #173b43; --mark: #6b5410; --mark-fg: #fff3c4;
  --ok: #5fd08f; --ok-soft: #173826; --warn: #f0bd55; --warn-soft: #3a2e12; --no: #f08a7f; --no-soft: #40201c;
  color-scheme: dark; }

* { box-sizing: border-box; }
body { background: var(--bg); color: var(--fg); font: 14px/1.45 var(--font-body); }
.wrap { max-width: 1440px; margin: 0 auto; padding-block: 24px 48px;
  padding-left: max(16px, env(safe-area-inset-left, 0px)); padding-right: max(16px, env(safe-area-inset-right, 0px));
  display: grid; gap: 20px; }
h1, h2 { font-family: var(--font-display); font-weight: 600; text-wrap: balance; margin: 0; }
h1 { font-size: 28px; letter-spacing: -0.01em; }
h2 { font-size: 15px; letter-spacing: 0.04em; text-transform: uppercase; color: var(--muted); }
.mono { font-family: var(--font-data); font-variant-numeric: tabular-nums; }
.muted { color: var(--muted); }
a { color: var(--accent); }
button, input, select { font: inherit; color: inherit; }
:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }

header { display: flex; flex-wrap: wrap; justify-content: space-between; align-items: end; gap: 12px 24px; }
header p { margin: 4px 0 0; color: var(--muted); max-width: 72ch; }
.src { font-size: 12px; color: var(--muted); text-align: right; }

/* status summary */
.summary { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 10px; }
.stat { background: var(--surface); border: 1px solid var(--line); border-radius: 6px; padding: 12px 14px;
  text-align: left; cursor: pointer; display: grid; gap: 2px; border-left-width: 4px; }
.stat[aria-pressed="true"] { background: var(--accent-soft); border-color: var(--accent); }
.stat .k { font-size: 12px; color: var(--muted); letter-spacing: 0.02em; }
.stat .v { font: 600 24px/1.1 var(--font-display); font-variant-numeric: tabular-nums; }
.stat .s { font-size: 12px; color: var(--muted); }
.stat.all { border-left-color: var(--accent); }
.stat.market { border-left-color: var(--no); }
.stat.approved { border-left-color: var(--warn); }
.stat.none { border-left-color: var(--ok); }
.bar { display: flex; height: 6px; border-radius: 3px; overflow: hidden; background: var(--sunken); gap: 2px; }
.bar span { display: block; }

/* filters */
.filters { background: var(--surface); border: 1px solid var(--line); border-radius: 6px; padding: 16px;
  display: grid; grid-template-columns: 1.1fr 1fr 1fr; gap: 14px 18px; }
.field { display: grid; gap: 5px; min-width: 0; align-content: start; }
.field label { font-size: 12px; font-weight: 600; letter-spacing: 0.03em; text-transform: uppercase; color: var(--muted); }
.field input, .field select { width: 100%; padding: 8px 10px; border: 1px solid var(--line); border-radius: 4px; background: var(--bg); }
.field input:focus, .field select:focus { border-color: var(--accent); outline: none; box-shadow: 0 0 0 3px var(--accent-soft); }
.chips { display: flex; flex-wrap: wrap; gap: 5px; }
.chip { font-size: 12px; padding: 2px 8px; border: 1px solid var(--line); border-radius: 999px; background: var(--bg); cursor: pointer; color: var(--muted); }
.chip:hover { border-color: var(--accent); color: var(--accent); }
.hint { font-size: 12px; color: var(--muted); }
.row2 { grid-column: 1 / -1; display: flex; flex-wrap: wrap; gap: 14px 18px; align-items: end; }
.row2 .field { flex: 1 1 170px; }
.btn { padding: 8px 12px; border: 1px solid var(--line); border-radius: 4px; background: var(--bg); cursor: pointer; white-space: nowrap; }
.btn:hover { border-color: var(--accent); color: var(--accent); }
.btn.primary { background: var(--accent); border-color: var(--accent); color: var(--surface); }
.btn.primary:hover { color: var(--surface); filter: brightness(1.08); }

/* results */
.resultbar { display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; gap: 8px 16px; }
.tablebox { overflow-x: auto; background: var(--surface); border: 1px solid var(--line); border-radius: 6px; }
table { width: 100%; border-collapse: collapse; }
th, td { text-align: left; vertical-align: top; padding: 10px 12px; border-bottom: 1px solid var(--line); }
thead th { position: sticky; top: 0; background: var(--sunken); font-size: 12px; font-weight: 600; letter-spacing: 0.03em;
  text-transform: uppercase; color: var(--muted); white-space: nowrap; z-index: 1; }
#main-table { min-width: 1080px; }
#main-table > tbody > tr.crp { cursor: pointer; }
#main-table > tbody > tr.crp:hover td { background: var(--sunken); }
tr.crp.open td { background: var(--accent-soft); border-bottom-color: transparent; }
.name { font-weight: 600; display: flex; align-items: baseline; gap: 6px; }
.caret { color: var(--muted); font-size: 11px; width: 10px; display: inline-block; transition: transform .15s; }
tr.open .caret { transform: rotate(90deg); }
@media (prefers-reduced-motion: reduce) { .caret { transition: none; } }
.sub { font-size: 12px; color: var(--muted); }
.ing { font-weight: 500; }
.dins { display: flex; flex-wrap: wrap; gap: 3px 8px; font-family: var(--font-data); font-size: 12px; }
.num { font-family: var(--font-data); font-variant-numeric: tabular-nums; }
.pill { display: inline-block; font-size: 12px; font-weight: 600; padding: 3px 9px; border-radius: 999px; white-space: nowrap; }
.pill.market { background: var(--no-soft); color: var(--no); }
.pill.approved { background: var(--warn-soft); color: var(--warn); }
.pill.none { background: var(--ok-soft); color: var(--ok); }
.mk { display: inline-flex; align-items: center; gap: 5px; font-size: 12px; font-weight: 500; margin-top: 4px; }
.mk::before { content: ""; width: 7px; height: 7px; border-radius: 50%; background: currentColor; }
.mk.Marketed { color: var(--ok); }
.mk.Approved { color: var(--accent); }
.mk.Dormant { color: var(--warn); }
.mk.Cancelled { color: var(--no); }
.mk.Notfound { color: var(--muted); }
.mk.Notfound::before { background: transparent; border: 1.5px solid currentColor; }
mark { background: var(--mark); color: var(--mark-fg); padding: 0 1px; border-radius: 2px; }

/* detail panel */
tr.detail > td { background: var(--bg); padding: 16px 16px 20px; }
.detail-grid { display: grid; gap: 18px; }
.detail-head { display: flex; flex-wrap: wrap; gap: 6px 24px; font-size: 13px; }
.detail-head b { font-weight: 600; }
.nocs { width: 100%; background: var(--surface); border: 1px solid var(--line); border-radius: 4px; font-size: 13px; }
.nocs th { position: static; background: var(--sunken); }
.nocs td { padding: 8px 10px; }
.nocs tr.dim td { opacity: 0.5; }
.nocs tr.hit td:first-child { box-shadow: inset 3px 0 0 var(--accent); }
.prod { display: grid; gap: 2px; }
.prod div { white-space: nowrap; }
.sec-title { display: flex; align-items: baseline; gap: 10px; flex-wrap: wrap; margin-bottom: 8px; }
.empty { padding: 32px; text-align: center; color: var(--muted); }
.pager { display: flex; gap: 6px; align-items: center; flex-wrap: wrap; }
.note { font-size: 12px; color: var(--muted); max-width: 110ch; display: grid; gap: 4px; }
.note p { margin: 0; }

@media (max-width: 900px) {
  .summary { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .filters { grid-template-columns: 1fr; }
  .src { text-align: left; }
}
</style>

<div class="wrap">
  <header>
    <div>
      <h1>PM Update Tracker</h1>
      <p>Every <span class="co"></span> product named as the Canadian Reference Product (CRP) in a generic submission, with its own NOCs, DINs and product details, set against the generic products filed against it.</p>
    </div>
    <div class="src">Source: Health Canada NOC and Drug Product Database<br>Data current to <span class="mono" id="asof"></span></div>
  </header>

  <section class="summary" id="summary" aria-label="Generic status"></section>

  <section class="filters" aria-label="Filters">
    <div class="field">
      <label for="q">Search products</label>
      <input id="q" type="search" placeholder="CRP name, ingredient, DIN, generic brand or company" autocomplete="off">
      <span class="hint">Searches CRP name, ingredients, therapeutic class, DINs, and generic brands, DINs and companies.</span>
    </div>
    <div class="field">
      <label for="rsub">Reason for submission</label>
      <input id="rsub" type="search" placeholder="e.g. name change" autocomplete="off">
      <div class="chips" data-for="rsub"></div>
    </div>
    <div class="field">
      <label for="rsupp">Reason for supplement</label>
      <input id="rsupp" type="search" placeholder="e.g. new strength" autocomplete="off">
      <div class="chips" data-for="rsupp"></div>
    </div>
    <div class="row2">
      <div class="field">
        <label for="scope">Apply reason filters to</label>
        <select id="scope">
          <option value="both">CRP and generic NOCs</option>
          <option value="crp">CRP NOCs only</option>
          <option value="gen">Generic NOCs only</option>
        </select>
      </div>
      <div class="field">
        <label for="ptype">Product type</label>
        <select id="ptype"><option value="">All product types</option></select>
      </div>
      <div class="field">
        <label for="market">CRP market status</label>
        <select id="market">
          <option value="Marketed">Active (marketed) only</option>
          <option value="Approved">Approved, not yet marketed</option>
          <option value="Dormant">Dormant</option>
          <option value="Cancelled">Cancelled</option>
          <option value="Not found">Not found in Drug Product Database</option>
          <option value="">All market statuses</option>
        </select>
      </div>
      <div class="field">
        <label for="sort">Sort by</label>
        <select id="sort">
          <option value="name">CRP name, A to Z</option>
          <option value="filers">Most generic filers</option>
          <option value="latest">Latest CRP NOC</option>
          <option value="genlatest">Latest generic NOC</option>
          <option value="gap">Generic on market first, most filers</option>
        </select>
      </div>
      <button class="btn" id="reset" type="button">Clear filters</button>
    </div>
    <div class="hint" style="grid-column:1/-1">Reason filters match every word you type, in any order. Put a phrase in "quotes" to match it exactly, and separate alternatives with a comma: <span class="mono">strength, "new indication"</span>.</div>
  </section>

  <section style="display:grid;gap:10px">
    <div class="resultbar">
      <h2 id="count"></h2>
      <div class="pager">
        <button class="btn" id="copy" type="button">Copy results as table</button>
        <span id="copymsg" class="hint" role="status"></span>
      </div>
    </div>
    <div class="tablebox">
      <table id="main-table">
        <thead><tr>
          <th style="width:17%">CRP product</th>
          <th style="width:22%">Product details</th>
          <th style="width:17%">CRP NOC / DIN</th>
          <th style="width:9%">Generic filers</th>
          <th style="width:22%">Generic products</th>
          <th style="width:13%">Generic status</th>
        </tr></thead>
        <tbody id="rows"></tbody>
      </table>
    </div>
    <div class="resultbar">
      <span class="hint" id="pageinfo"></span>
      <div class="pager" id="pager"></div>
    </div>
  </section>

  <section class="note">
    <p><b>Which products are listed.</b> A CRP is listed when a generic submission names <span class="co"></span> (or a predecessor such as Astra Pharma or Zeneca Pharma, allowing for typos) as the CRP company, or when <span class="co"></span> holds the CRP's own NOCs. Generic filers often misspell the CRP name, so one product can appear under several spellings.</p>
    <p><b>Generic status</b> uses the current Drug Product Database (DPD) status of the DINs in the generic NOCs that name the CRP. <i>Generic on market</i>: at least one generic DIN is marketed. <i>Generic approved, not marketed</i>: none is marketed, but at least one is approved or dormant. <i>No active generic</i>: every generic DIN is cancelled or not in the DPD.</p>
    <p><b>CRP market status</b> also comes from the DPD. A CRP is active when at least one of its DINs is currently marketed. Products are matched by DIN first, then by brand name, then by close spelling to catch typos in the CRP names filed with generic submissions. Where a name matches several companies, the CRP company's own products are used. CRPs with no match are listed under "Not found in Drug Product Database". Inactive NOC records are excluded throughout.</p>
    <p><b>Limits.</b> The NOC database lists approved submissions only, so filings still under review do not appear. CRP NOCs are matched by brand name on New Drug Submissions and their supplements. Products approved before 1994 often have no NOC record, and their details are taken from the generic NOCs that reference them.</p>
  </section>
</div>

<script id="data" type="application/json">__DATA__</script>
<script>
(() => {
  const D = JSON.parse(document.getElementById('data').textContent);
  const NOC = D.nocs; // n -> [date, maker, subType, subClass, reasonSub, reasonSupp, conditions, products[[brand,din,ings,form,route,dpdStatus]]]
  const CRPS = D.crps;
  const STATUS = {
    market: 'Generic on market',
    approved: 'Generic approved, not marketed',
    none: 'No active generic'
  };
  document.querySelectorAll('.co').forEach(e => e.textContent = D.company);
  document.title = `${D.company} PM Update Tracker`;
  document.querySelector('h1').textContent = document.title;
  const MARKET = {
    Marketed: 'Active (marketed)', Approved: 'Approved, not marketed', Dormant: 'Dormant',
    Cancelled: 'Cancelled', 'Not found': 'Not found in DPD'
  };
  const PAGE = 50;
  const dpdClass = s => ['Marketed', 'Approved', 'Dormant'].includes(s) ? s : 'Cancelled';
  const $ = id => document.getElementById(id);
  const esc = s => String(s ?? '').replace(/[&<>"]/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));

  // precompute lowercase search haystacks
  for (const c of CRPS) {
    c.hay = [c.n, c.co, c.tc, c.ing.join(' '), c.din.join(' '), c.gb.join(' '), c.gd.join(' '), c.gc.join(' ')].join(' ').toLowerCase();
    c.latest = c.own.length ? NOC[c.own[c.own.length - 1]][0] : '';
    c.genLatest = c.gen.length ? NOC[c.gen[c.gen.length - 1]][0] : '';
  }
  const lc = {};
  for (const n in NOC) lc[n] = [NOC[n][4].toLowerCase(), NOC[n][5].toLowerCase()];

  $('asof').textContent = D.asOf;
  const mkCount = {}; CRPS.forEach(c => mkCount[c.ms] = (mkCount[c.ms] || 0) + 1);
  [...$('market').options].forEach(o => o.textContent += ` (${(o.value ? mkCount[o.value] || 0 : CRPS.length).toLocaleString()})`);
  [...new Set(CRPS.map(c => c.pt))].sort().forEach(p => $('ptype').insertAdjacentHTML('beforeend', `<option>${esc(p)}</option>`));

  const CHIPS = {
    rsub: ['name change', 'merger', 'licensing', 'product monograph', 'labelling', 'new manufacturer'],
    rsupp: ['product monograph', 'safety', 'new indication', 'new strength', 'manufacturing', 'site', 'dosage form', 'formulation', 'pediatric', 'CRP']
  };
  document.querySelectorAll('.chips').forEach(box => {
    const id = box.dataset.for;
    box.innerHTML = CHIPS[id].map(t => `<button type="button" class="chip" data-t="${esc(t)}">${esc(t)}</button>`).join('');
    box.addEventListener('click', e => {
      const t = e.target.closest('.chip')?.dataset.t; if (!t) return;
      const inp = $(id); const term = t.includes(' ') ? `"${t}"` : t;
      inp.value = inp.value.trim() ? `${inp.value.trim()}, ${term}` : term;
      update();
    });
  });

  // query parser: comma = OR groups; inside a group every word/"phrase" must appear
  function parse(q) {
    return q.split(',').map(g => {
      const terms = []; g.replace(/"([^"]+)"|(\S+)/g, (_, p, w) => { terms.push((p || w).toLowerCase()); });
      return terms;
    }).filter(g => g.length);
  }
  const matches = (text, groups) => groups.some(g => g.every(t => text.includes(t)));
  const allTerms = groups => [...new Set(groups.flat())].sort((a, b) => b.length - a.length);
  function hl(text, terms) {
    if (!terms.length || !text) return esc(text);
    const re = new RegExp(terms.map(t => t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|'), 'gi');
    let out = '', i = 0;
    text.replace(re, (m, off) => { out += esc(text.slice(i, off)) + '<mark>' + esc(m) + '</mark>'; i = off + m.length; });
    return out + esc(text.slice(i));
  }

  const state = { status: '', page: 0, open: new Set() };
  let view = [], qSub = [], qSupp = [], qText = [];

  function nocPool(c, scope) {
    return scope === 'crp' ? c.own : scope === 'gen' ? c.gen : c.own.concat(c.gen);
  }
  function filterBase() {
    qText = parse($('q').value.replace(/,/g, ' '));
    qSub = parse($('rsub').value); qSupp = parse($('rsupp').value);
    const scope = $('scope').value, pt = $('ptype').value, mk = $('market').value;
    return CRPS.filter(c => {
      if (pt && c.pt !== pt) return false;
      if (mk && c.ms !== mk) return false;
      if (qText.length && !matches(c.hay, qText)) return false;
      if (qSub.length || qSupp.length) {
        const pool = nocPool(c, scope);
        if (qSub.length && !pool.some(n => matches(lc[n][0], qSub))) return false;
        if (qSupp.length && !pool.some(n => matches(lc[n][1], qSupp))) return false;
      }
      return true;
    });
  }
  const sorters = {
    name: (a, b) => a.n.localeCompare(b.n),
    filers: (a, b) => b.gm - a.gm || a.n.localeCompare(b.n),
    latest: (a, b) => b.latest.localeCompare(a.latest) || a.n.localeCompare(b.n),
    genlatest: (a, b) => b.genLatest.localeCompare(a.genLatest) || a.n.localeCompare(b.n),
    gap: (a, b) => (a.st === 'market' ? 0 : a.st === 'approved' ? 1 : 2) - (b.st === 'market' ? 0 : b.st === 'approved' ? 1 : 2) || b.gm - a.gm
  };

  function renderSummary(base) {
    const cnt = { market: 0, approved: 0, none: 0 };
    base.forEach(c => cnt[c.st]++);
    const tot = base.length || 1;
    const pct = k => Math.round(cnt[k] / tot * 100);
    const tile = (k, label, v, sub) => `<button type="button" class="stat ${k || 'all'}" data-s="${k}" aria-pressed="${state.status === k}">
      <span class="k">${label}</span><span class="v">${v.toLocaleString()}</span><span class="s">${sub}</span></button>`;
    $('summary').innerHTML =
      tile('', 'CRP products', base.length, `<span class="bar" aria-hidden="true">
        <span style="width:${pct('market')}%;background:var(--no)"></span><span style="width:${pct('approved')}%;background:var(--warn)"></span><span style="width:${pct('none')}%;background:var(--ok)"></span></span>`) +
      tile('market', STATUS.market, cnt.market, `${pct('market')}% of CRPs shown`) +
      tile('approved', STATUS.approved, cnt.approved, `${pct('approved')}% of CRPs shown`) +
      tile('none', STATUS.none, cnt.none, `${pct('none')}% of CRPs shown`);
  }

  const more = (arr, k, fmt = x => esc(x)) => arr.slice(0, k).map(fmt).join('') + (arr.length > k ? `<span class="muted">+${arr.length - k} more</span>` : '');

  function rowHtml(c) {
    const terms = allTerms(qText);
    const lastOwn = c.own.length ? c.own[c.own.length - 1] : null;
    const lastGen = c.gen.length ? c.gen[c.gen.length - 1] : null;
    const open = state.open.has(c.n);
    const details = [c.fm.join(', '), c.rt.join(', ')].filter(Boolean).join(' · ');
    return `<tr class="crp${open ? ' open' : ''}" data-n="${esc(c.n)}" tabindex="0" aria-expanded="${open}">
      <td><div class="name"><span class="caret" aria-hidden="true">▶</span><span>${hl(c.n, terms)}</span></div>
        <div class="sub">${hl(c.co, terms)}${c.ct && c.ct !== 'CANADA' ? ` · ${esc(c.ct)}` : ''}</div>
        <div class="mk ${c.ms.replace(' ', '')}">${MARKET[c.ms]}</div></td>
      <td><div class="ing">${hl(c.ing.join(' + ') || '—', terms)}</div>
        <div class="sub">${esc(details || '—')}</div>
        <div class="sub">${esc(c.pt)}${c.tc ? ' · ' + hl(c.tc, terms) : ''}</div></td>
      <td>${lastOwn ? `<div><span class="num">NOC ${lastOwn}</span> <span class="sub">${NOC[lastOwn][0]}</span></div>
        <div class="sub">${c.own.length} NOC${c.own.length > 1 ? 's' : ''} on record</div>` : '<div class="sub">No CRP NOC on record (pre-1994 or foreign)</div>'}
        ${c.din.length ? `<div class="dins">${more(c.din, 4, d => `<span>${hl(d, terms)}</span>`)}</div>` : ''}</td>
      <td><div class="num">${c.gm}</div><div class="sub">${c.gn} generic NOC${c.gn === 1 ? '' : 's'}</div></td>
      <td>${c.gb.length ? `<div class="ing">${more(c.gb, 2, b => `<div>${hl(b, terms)}</div>`)}</div>` : '<div class="sub">—</div>'}
        ${lastGen ? `<div class="sub">Latest <span class="num">NOC ${lastGen}</span> · ${NOC[lastGen][0]}</div>` : ''}
        <div class="sub">${c.gdm.length} of ${c.gd.length} generic DIN${c.gd.length === 1 ? '' : 's'} marketed</div></td>
      <td><span class="pill ${c.st}">${STATUS[c.st]}</span></td>
    </tr>${open ? detailHtml(c) : ''}`;
  }

  function nocTable(list, scopeOn) {
    if (!list.length) return '<div class="sub">None on record.</div>';
    const active = scopeOn && (qSub.length || qSupp.length);
    const tSub = allTerms(qSub), tSupp = allTerms(qSupp);
    const rows = list.slice().reverse().map(n => {
      const r = NOC[n];
      const hit = active && ((qSub.length && matches(lc[n][0], qSub)) || (qSupp.length && matches(lc[n][1], qSupp)));
      const prods = r[7].length ? r[7].map(p => `<div>${esc(p[0])} · <span class="num">${p[1] || 'no DIN'}</span> · ${esc(p[2] || '')}${p[3] ? ' · ' + esc(p[3]) : ''}${p[5] ? ` · <span class="mk ${dpdClass(p[5])}">${esc(p[5])}</span>` : ''}</div>`).join('') : '<span class="muted">—</span>';
      return `<tr class="${active ? (hit ? 'hit' : 'dim') : ''}">
        <td class="num">${n}</td><td class="num" style="white-space:nowrap">${r[0]}</td>
        <td>${esc(r[2])}<div class="sub">${esc(r[3])}${r[6] === 'Y' ? ' · with conditions' : ''}</div><div class="sub">${esc(r[1])}</div></td>
        <td>${r[4] ? hl(r[4], scopeOn ? tSub : []) : '<span class="muted">—</span>'}</td>
        <td>${r[5] ? hl(r[5], scopeOn ? tSupp : []) : '<span class="muted">—</span>'}</td>
        <td class="prod">${prods}</td></tr>`;
    }).join('');
    return `<div style="overflow-x:auto"><table class="nocs"><thead><tr><th>NOC</th><th>Date</th><th>Submission</th><th>Reason for submission</th><th>Reason for supplement</th><th>Products · DIN · strength</th></tr></thead><tbody>${rows}</tbody></table></div>`;
  }

  function detailHtml(c) {
    const scope = $('scope').value;
    return `<tr class="detail"><td colspan="6"><div class="detail-grid">
      <div class="detail-head">
        <span><b>CRP company</b> ${esc(c.co)}${c.ct ? ' (' + esc(c.ct) + ')' : ''}</span>
        <span><b>CRP DINs</b> <span class="mono">${c.din.length ? esc(c.din.join(', ')) : 'none on record'}</span></span>
        <span><b>Generic DINs marketed</b> <span class="mono">${c.gdm.length ? esc(c.gdm.join(', ')) : 'none'}</span></span>
        <span><b>First generic NOC</b> <span class="mono">${esc(c.first)}</span></span>
      </div>
      <div class="detail-head"><span><b>Generic companies (${c.gc.length})</b> ${esc(c.gc.join(' · '))}</span></div>
      <div><div class="sec-title"><h2>CRP in the Drug Product Database</h2><span class="hint">${c.mp.length ? 'Matched by ' + esc(c.mh) : 'No matching product found'}</span></div>
        ${c.mp.length ? `<div style="overflow-x:auto"><table class="nocs"><thead><tr><th>DIN</th><th>Brand name</th><th>Current status</th><th>Company</th></tr></thead><tbody>${
          c.mp.map(p => `<tr><td class="num">${esc(p[0])}</td><td>${esc(p[1])}</td><td><span class="mk ${dpdClass(p[2])}">${esc(p[2])}</span></td><td>${esc(p[3])}</td></tr>`).join('')
        }</tbody></table></div>` : '<div class="sub">The CRP name did not match any DIN or brand name in the DPD.</div>'}</div>
      <div><div class="sec-title"><h2>CRP NOCs</h2><span class="hint">${c.own.length} on record, newest first</span></div>${nocTable(c.own, scope !== 'gen')}</div>
      <div><div class="sec-title"><h2>Generic NOCs referencing this CRP</h2><span class="hint">${c.gen.length} on record, newest first · DIN status from the DPD</span></div>${nocTable(c.gen, scope !== 'crp')}</div>
    </div></td></tr>`;
  }

  function render() {
    const pages = Math.max(1, Math.ceil(view.length / PAGE));
    state.page = Math.min(state.page, pages - 1);
    const slice = view.slice(state.page * PAGE, state.page * PAGE + PAGE);
    $('rows').innerHTML = slice.length ? slice.map(rowHtml).join('') :
      '<tr><td colspan="6" class="empty">No CRP products match these filters. Try fewer words, or set "Apply reason filters to" to both.</td></tr>';
    $('count').textContent = `${view.length.toLocaleString()} CRP product${view.length === 1 ? '' : 's'}${state.status ? ' · ' + STATUS[state.status] : ''}`;
    $('pageinfo').textContent = view.length ? `Showing ${state.page * PAGE + 1}–${state.page * PAGE + slice.length} of ${view.length.toLocaleString()}. Select a row to see its NOC history.` : '';
    const btn = (p, label, dis) => `<button class="btn" type="button" data-p="${p}" ${dis ? 'disabled' : ''}>${label}</button>`;
    $('pager').innerHTML = pages > 1 ? btn(state.page - 1, 'Previous', state.page === 0) +
      `<span class="hint mono">${state.page + 1} / ${pages}</span>` + btn(state.page + 1, 'Next', state.page >= pages - 1) : '';
  }

  function update(keepPage) {
    const base = filterBase();
    renderSummary(base);
    view = (state.status ? base.filter(c => c.st === state.status) : base).sort(sorters[$('sort').value]);
    if (!keepPage) state.page = 0;
    render();
  }

  let t;
  ['q', 'rsub', 'rsupp'].forEach(id => $(id).addEventListener('input', () => { clearTimeout(t); t = setTimeout(update, 150); }));
  ['scope', 'ptype', 'market', 'sort'].forEach(id => $(id).addEventListener('change', () => update()));
  $('summary').addEventListener('click', e => {
    const b = e.target.closest('.stat'); if (!b) return;
    state.status = state.status === b.dataset.s ? '' : b.dataset.s; update();
  });
  $('pager').addEventListener('click', e => {
    const b = e.target.closest('[data-p]'); if (!b || b.disabled) return;
    state.page = +b.dataset.p; render(); $('main-table').scrollIntoView({ block: 'start' });
  });
  function toggle(tr) {
    const n = tr.dataset.n;
    state.open.has(n) ? state.open.delete(n) : state.open.add(n);
    render();
  }
  $('rows').addEventListener('click', e => { const tr = e.target.closest('tr.crp'); if (tr) toggle(tr); });
  $('rows').addEventListener('keydown', e => {
    const tr = e.target.closest('tr.crp');
    if (tr && (e.key === 'Enter' || e.key === ' ')) { e.preventDefault(); toggle(tr); document.querySelector(`tr.crp[data-n="${CSS.escape(tr.dataset.n)}"]`)?.focus(); }
  });
  $('reset').addEventListener('click', () => {
    ['q', 'rsub', 'rsupp'].forEach(id => $(id).value = '');
    $('scope').value = 'both'; $('ptype').value = ''; $('market').value = 'Marketed'; $('sort').value = 'name';
    state.status = ''; state.open.clear(); update();
  });
  $('copy').addEventListener('click', () => {
    const head = ['CRP product', 'CRP company', 'Ingredients', 'Dosage form', 'Route', 'Product type', 'CRP market status', 'CRP NOCs', 'Latest CRP NOC', 'CRP DINs', 'Generic filers', 'Generic companies', 'Generic brands', 'Generic NOCs', 'Latest generic NOC', 'Generic DINs', 'Generic DINs marketed', 'Generic status'];
    const lines = [head.join('\t')].concat(view.map(c => [c.n, c.co, c.ing.join(' + '), c.fm.join(', '), c.rt.join(', '), c.pt, MARKET[c.ms],
      c.own.join(' '), c.latest, c.din.join(' '), c.gm, c.gc.join('; '), c.gb.join(', '), c.gen.join(' '), c.genLatest, c.gd.join(' '), c.gdm.join(' '), STATUS[c.st]].join('\t')));
    const text = lines.join('\n');
    const done = msg => { $('copymsg').textContent = msg; setTimeout(() => $('copymsg').textContent = '', 3000); };
    try {
      navigator.clipboard.writeText(text).then(() => done(`Copied ${view.length} rows. Paste into Excel.`), () => done('Copy was blocked by the browser.'));
    } catch { done('Copy was blocked by the browser.'); }
  });

  update();
})();
</script>
'''


def assemble_page(data):
    page = {"asOf": data["asOf"], "company": data["company"], "nocs": data["nocs"],
            "crps": [{k: v for k, v in c.items() if k not in ("hay", "latest", "genLatest")} for c in data["crps"]]}
    payload = json.dumps(page, separators=(",", ":"), ensure_ascii=False).replace("</", r"<\/")  # keep </script> out
    head = '<!doctype html>\n<meta charset="utf-8">\n<meta name="viewport" content="width=device-width, initial-scale=1">\n'
    return head + TEMPLATE_HTML.replace("__DATA__", payload)


html = assemble_page(data)
with open(HTML_PATH, "w", encoding="utf-8") as f:
    f.write(html)
print(f"Saved {len(html.encode('utf-8')) / 1e6:.1f} MB page to {HTML_PATH}")

# COMMAND ----------

displayHTML(html)
