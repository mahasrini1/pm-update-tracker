"""Build the CRP vs Apotex dataset from the Health Canada NOC and DPD API JSON downloads.

Usage: python scripts/build.py raw data.json
(raw/ is the folder written by scripts/fetch_data.py)
"""
import json, sys, collections as C, re

d, out = sys.argv[1], sys.argv[2]
load = lambda n: json.load(open(f"{d}/{n}.json", encoding="utf-8"))
main = load("noticeofcompliancemain")
inactive = {r["noc_number"] for r in main if str(r["noc_active_status"]).strip() != "1"}
main = [r for r in main if r["noc_number"] not in inactive]  # keep active NOC records only
prods = [p for p in load("drugproduct") if p["noc_number"] not in inactive]
ingr = load("medicinalingredient")
forms = load("dosageform")
routes = load("route")

norm = lambda s: re.sub(r"\s+", " ", (s or "").strip().upper())
is_apo = lambda r: "APOTEX" in r["noc_manufacturer_name"].upper()
INNOVATOR_TYPES = {"New Drug Submission", "Supplement to a New"}

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


def products(n):
    """Product rows for one NOC: [brand, din, ingredients, form, route]."""
    rows = []
    for p in prods_by_noc.get(n, []):
        pid = p["noc_br_product_id"]
        ings = "; ".join(f"{i} {s}".strip() for i, s in ing_by_prod.get(pid, []))
        din = (p["noc_br_din"] or "").strip()
        rows.append([
            p["noc_br_brandname"].strip(),
            "" if din.upper() in ("N/A", "") else din,
            ings,
            ", ".join(sorted(form_by_prod.get(pid, []))),
            ", ".join(sorted(route_by_prod.get(pid, []))),
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

apo_all = [r["noc_number"] for r in main if is_apo(r)]
apo_ings = C.defaultdict(set)  # frozenset of ingredients (one product) -> apotex noc numbers
for n in apo_all:
    for p in prods_by_noc.get(n, []):
        key = frozenset(i for i, _ in ing_by_prod.get(p["noc_br_product_id"], []))
        if key:
            apo_ings[key].add(n)

used_nocs = set()
crps = []
for name, refs in crp_refs.items():
    company = C.Counter(norm(r["noc_crp_company_name"]) for r in refs).most_common(1)[0][0]
    country = C.Counter(norm(r["noc_crp_country_name"]) for r in refs).most_common(1)[0][0]

    # the CRP's own (innovator) NOCs: brand name matches, NDS / SNDS submission
    own = sorted(
        (n for n in nocs_by_brand.get(name, ())
         if noc[n]["noc_on_submission_type"] in INNOVATOR_TYPES and not is_apo(noc[n])),
        key=lambda n: noc[n]["noc_date"],
    )
    apo = sorted((r["noc_number"] for r in refs if is_apo(r)), key=lambda n: noc[n]["noc_date"])
    generic_makers = {norm(r["noc_manufacturer_name"]) for r in refs}

    # product details come from the CRP's own NOCs; fall back to the generics that reference it
    detail_src = own or [r["noc_number"] for r in refs]
    ings = ingredient_names(detail_src)
    dins, pforms, proutes = set(), set(), set()
    for n in own:
        for b, din, _, f, rt in products(n):
            if norm(b) == name and din:
                dins.add(din)
    for n in detail_src:
        for p in prods_by_noc.get(n, []):
            pforms |= form_by_prod.get(p["noc_br_product_id"], set())
            proutes |= route_by_prod.get(p["noc_br_product_id"], set())

    same_ing = set()
    if not apo:
        same_ing = set(apo_ings.get(frozenset(ings), set()))
    status = "filed" if apo else ("ingredient" if same_ing else "none")
    apo_list = apo or sorted(same_ing, key=lambda n: noc[n]["noc_date"])

    tclass = next((noc[n]["noc_therapeutic_class"].strip() for n in reversed(own)
                   if noc[n]["noc_therapeutic_class"].strip()), "")
    ptype = C.Counter(noc[n]["noc_product_type"] for n in detail_src).most_common(1)[0][0]
    apo_brands = sorted({b for n in apo_list for b, *_ in products(n)})
    apo_dins = sorted({din for n in apo_list for _, din, *_ in products(n) if din})

    used_nocs.update(own, apo_list)
    crps.append({
        "n": name, "co": company, "ct": country, "pt": ptype, "tc": tclass,
        "ing": sorted(ings), "fm": sorted(pforms), "rt": sorted(proutes),
        "din": sorted(dins), "own": own, "apo": apo_list, "st": status,
        "ab": apo_brands, "ad": apo_dins,
        "gn": len(refs), "gm": len(generic_makers),
        "first": refs and min(r["noc_date"] for r in refs),
    })

nocs_out = {}
for n in used_nocs:
    r = noc[n]
    nocs_out[n] = [
        r["noc_date"], r["noc_manufacturer_name"].strip(), r["noc_on_submission_type"].strip(),
        r["noc_submission_class"].strip(), r["noc_reason_submission"].strip(),
        r["noc_reason_supplement"].strip(), r["noc_status_with_conditions"], products(n),
    ]

# ---- CRP market status from the Drug Product Database (dpd/drugproduct.json + dpd/status.json) ----
import difflib

dpd_prods = load("dpd/drugproduct")
dpd_status = {s["drug_code"]: s["status"] for s in load("dpd/status")}
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
        m = [p for n, ps in by_name.items() if n.startswith(k + " ") for p in ps
             if first_word(p["company_name"]) == first_word(c["co"])]
    if not m:
        m = by_strip.get(stripped(c["n"]), [])
    if not m:
        k = stripped(c["n"])
        if len(k) >= 5:
            cand = difflib.get_close_matches(k, [x for x in strip_keys if x[:1] == k[:1]], n=1, cutoff=0.86)
            if cand:
                m, how = by_strip[cand[0]], "close spelling"
    # a generic-style name can hit several companies: prefer the CRP company's own products
    same_co = [p for p in m if first_word(p["company_name"]) == first_word(c["co"])]
    return (same_co or m), (how if m else "")


MARKET_ORDER = ["Marketed", "Approved", "Dormant"]
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
json.dump({"asOf": latest, "crps": crps, "nocs": nocs_out}, open(out, "w", encoding="utf-8"),
          separators=(",", ":"), ensure_ascii=False)

print("CRP market status", dict(C.Counter(c["ms"] for c in crps)),
      "| matched by", dict(C.Counter(c["mh"] for c in crps)))
st = C.Counter(c["st"] for c in crps)
print("inactive NOCs removed", len(inactive), "| crps", len(crps), dict(st), "nocs", len(nocs_out), "no own noc", sum(1 for c in crps if not c["own"]))
