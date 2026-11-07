"""Build the AstraZeneca CRP vs generic products dataset from the Health Canada NOC and DPD API JSON downloads.

Usage: python scripts/build.py raw data.json
(raw/ is the folder written by scripts/fetch_data.py)
"""
import json, sys, collections as C, re, difflib

COMPANY = "AstraZeneca"
# spellings that identify the company (and its predecessors) in NOC company names; matched by close spelling
COMPANY_NAMES = ["ASTRAZENECA", "ZENECA", "ASTRA PHARMA"]

d, out = sys.argv[1], sys.argv[2]
load = lambda n: json.load(open(f"{d}/{n}.json", encoding="utf-8"))
main = load("noticeofcompliancemain")
inactive = {r["noc_number"] for r in main if str(r["noc_active_status"]).strip() != "1"}
main = [r for r in main if r["noc_number"] not in inactive]  # keep active NOC records only
prods = [p for p in load("drugproduct") if p["noc_number"] not in inactive]
ingr = load("medicinalingredient")
forms = load("dosageform")
routes = load("route")
dpd_prods = load("dpd/drugproduct")
dpd_status = {s["drug_code"]: s["status"] for s in load("dpd/status")}

norm = lambda s: re.sub(r"\s+", " ", (s or "").strip().upper())
INNOVATOR_TYPES = {"New Drug Submission", "Supplement to a New"}
MARKET_ORDER = ["Marketed", "Approved", "Dormant"]
_company_keys = [re.sub(r"[^A-Z]", "", n) for n in COMPANY_NAMES]


def is_company(name):
    """True when a company name is the tracked company, allowing typos such as ASTRZENECA or ASTRA ZENECA."""
    words = re.sub(r"[^A-Z ]", " ", norm(name)).split()
    cands = words + [a + b for a, b in zip(words, words[1:])]
    return any(difflib.SequenceMatcher(None, w, k).ratio() >= 0.85 for w in cands for k in _company_keys)


noc = {r["noc_number"]: r for r in main}

# product-level lookups
ing_by_prod = C.defaultdict(list)
for r in ingr:
    s = r["noc_pi_strength"]
    s = ("%g" % s) if isinstance(s, (int, float)) else (s or "")
    ing_by_prod[r["noc_pi_din_product_id"]].append(
        (norm(r["noc_pi_medic_ingr_name"]), f"{s} {r['noc_pi_unit'] or ''}".strip())
    )
form_by_prod = C.defaultdict(set)
for r in forms:
    form_by_prod[r["noc_pf_din_product_id"]].add(r["noc_pf_form_name"].strip())
route_by_prod = C.defaultdict(set)
for r in routes:
    route_by_prod[r["noc_pr_din_product_id"]].add(r["noc_pr_route"].strip())

prods_by_noc = C.defaultdict(list)
nocs_by_brand = C.defaultdict(set)
for p in prods:
    prods_by_noc[p["noc_number"]].append(p)
    nocs_by_brand[norm(p["noc_br_brandname"])].add(p["noc_number"])

# current Drug Product Database status of every DIN (a DIN listed twice keeps its most active status)
RANK = {s: i for i, s in enumerate(MARKET_ORDER)}
din_status = {}
for p in dpd_prods:
    st = dpd_status.get(p["drug_code"], "")
    din = p["drug_identification_number"]
    if din not in din_status or RANK.get(st, 9) < RANK.get(din_status[din], 9):
        din_status[din] = st


def products(n):
    """Product rows for one NOC: [brand, din, ingredients, form, route, DPD status of the DIN]."""
    rows = []
    for p in prods_by_noc.get(n, []):
        pid = p["noc_br_product_id"]
        ings = "; ".join(f"{i} {s}".strip() for i, s in ing_by_prod.get(pid, []))
        din = (p["noc_br_din"] or "").strip()
        din = "" if din.upper() in ("N/A", "") else din
        rows.append([
            p["noc_br_brandname"].strip(),
            din,
            ings,
            ", ".join(sorted(form_by_prod.get(pid, []))),
            ", ".join(sorted(route_by_prod.get(pid, []))),
            din_status.get(din, ""),
        ])
    return rows


def ingredient_names(nocs):
    s = set()
    for n in nocs:
        for p in prods_by_noc.get(n, []):
            for i, _ in ing_by_prod.get(p["noc_br_product_id"], []):
                s.add(i)
    return s


# group generic submissions by the CRP they reference
crp_refs = C.defaultdict(list)
for r in main:
    name = norm(r["noc_crp_product_name"])
    if name:
        crp_refs[name].append(r)

used_nocs = set()
crps = []
for name, refs in crp_refs.items():
    # the CRP's own (innovator) NOCs: brand name matches, NDS / SNDS submission
    own = sorted(
        (n for n in nocs_by_brand.get(name, ()) if noc[n]["noc_on_submission_type"] in INNOVATOR_TYPES),
        key=lambda n: noc[n]["noc_date"],
    )
    # keep only the tracked company's CRPs: a generic filing names it as the CRP company, or it holds the CRP's NOCs
    if not (any(is_company(r["noc_crp_company_name"]) for r in refs)
            or any(is_company(noc[n]["noc_manufacturer_name"]) for n in own)):
        continue
    company = C.Counter(norm(r["noc_crp_company_name"]) for r in refs).most_common(1)[0][0]
    country = C.Counter(norm(r["noc_crp_country_name"]) for r in refs).most_common(1)[0][0]

    gen = sorted((r["noc_number"] for r in refs), key=lambda n: noc[n]["noc_date"])  # generic NOCs
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
        "gn": len(refs), "gm": len(generic_makers),
        "gc": sorted(generic_makers),
        "first": min(r["noc_date"] for r in refs),
    })

nocs_out = {}
for n in used_nocs:
    r = noc[n]
    nocs_out[n] = [
        r["noc_date"], r["noc_manufacturer_name"].strip(), r["noc_on_submission_type"].strip(),
        r["noc_submission_class"].strip(), r["noc_reason_submission"].strip(),
        r["noc_reason_supplement"].strip(), r["noc_status_with_conditions"], products(n),
    ]

# ---- CRP market status from the Drug Product Database ----
FORMWORDS = (r"\b(TABLETS?|CAPLETS?|CAPSULES?|CAPS?|TABS?|LIQUI-?GELS?|INJECTION|INJ|SOLUTION|"
             r"SUSPENSION|CREAM|OINTMENT|SYRUP|ORAL|FOR|XR|SR)\b")


def core(s):  # drop strengths such as "50 MG/ML"
    s = re.sub(r"\d+(\.\d+)?\s*(MG|MCG|G|ML|%|IU|UNITS?)(/\S+)?", "", norm(re.sub(r"[®™]", "", s)))
    return re.sub(r"\s+", " ", s).strip(" ,-/")


def stripped(s):  # also drop dosage-form words and punctuation
    s = core(s).replace("&", " AND ").replace("+", " AND ")
    s = re.sub(r"[^A-Z0-9 ]", " ", re.sub(FORMWORDS, " ", s))
    return re.sub(r"\s+", " ", s).strip()


by_din, by_name, by_core, by_strip = (C.defaultdict(list) for _ in range(4))
for p in dpd_prods:
    n = norm(p["brand_name"])
    by_din[p["drug_identification_number"]].append(p)
    by_name[n].append(p)
    by_core[core(n)].append(p)
    by_strip[stripped(n)].append(p)
strip_keys = list(by_strip)
first_word = lambda s: norm(s).split(" ")[0] if s else ""


def same_company(p, c):
    return first_word(p["company_name"]) == first_word(c["co"]) or (is_company(p["company_name"]) and is_company(c["co"]))


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
    c["mp"] = sorted(
        {(p["drug_identification_number"], p["brand_name"].strip(), dpd_status.get(p["drug_code"], ""),
          p["company_name"].strip()) for p in m},
        key=lambda x: (x[2] != "Marketed", x[0]))[:40]
    if not c["din"]:  # pre-1994 CRPs have no NOC DINs; use the DPD ones
        c["din"] = sorted({x[0] for x in c["mp"]})

crps.sort(key=lambda c: c["n"])
latest = max(r["noc_date"] for r in main)
json.dump({"asOf": latest, "company": COMPANY, "crps": crps, "nocs": nocs_out}, open(out, "w", encoding="utf-8"),
          separators=(",", ":"), ensure_ascii=False)

print("CRP market status", dict(C.Counter(c["ms"] for c in crps)),
      "| matched by", dict(C.Counter(c["mh"] for c in crps)))
st = C.Counter(c["st"] for c in crps)
print("inactive NOCs removed", len(inactive), f"| {COMPANY} crps", len(crps), dict(st), "nocs", len(nocs_out),
      "no own noc", sum(1 for c in crps if not c["own"]))
