"""PM Update Tracker: a Streamlit app for Databricks Apps.

Lists every Canadian Reference Product (CRP) named in a generic drug submission in Canada, with the CRP's own
Notices of Compliance (NOCs), DINs and product details, and whether Apotex holds a NOC for the same product.

It reads the 7 Health Canada tables (NOC API: noticeofcompliancemain, drugproduct, medicinalingredient,
dosageform, route; DPD API: drugproduct, status) through a SQL warehouse, joins them with the same rules as the
original scripts/build.py, and shows the tracker with filters, status counts, NOC history and a CSV download.

Setup
-----
1. Set TABLES below to your 7 table names (catalog.schema.table). The columns are expected to use the API's JSON
   field names (see COLUMNS); change a name in COLUMNS if your table differs.
2. Set WAREHOUSE_ID below, or set the DATABRICKS_WAREHOUSE_ID environment variable.
3. Create a Databricks App (Compute > Apps > Create app > Custom), upload this file as app.py and deploy.
4. Give the app's service principal CAN USE on the warehouse and SELECT on the 7 tables.

The file starts Streamlit itself when run as `python app.py`, so no app.yaml is needed.
"""
import os
import sys

if __name__ == "__main__":
    try:
        from streamlit.runtime import exists as _streamlit_running
    except ImportError:
        def _streamlit_running():
            return False
    if not _streamlit_running():  # started as `python app.py`: hand over to `streamlit run`
        port = os.getenv("DATABRICKS_APP_PORT") or os.getenv("STREAMLIT_SERVER_PORT") or "8501"
        os.execvp(sys.executable, [sys.executable, "-m", "streamlit", "run", os.path.abspath(__file__),
                                   "--server.port", port, "--server.address", "0.0.0.0",
                                   "--server.headless", "true", "--browser.gatherUsageStats", "false"])

import collections as C
import datetime as dt
import difflib
import io
import json
import re
from decimal import Decimal

import pandas as pd
import streamlit as st

# ----------------------------------------------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------------------------------------------
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "")  # or paste the SQL warehouse ID here

TABLES = {  # source -> fully qualified table name
    "noticeofcompliancemain": "main.health_canada.noc_noticeofcompliancemain",
    "drugproduct": "main.health_canada.noc_drugproduct",
    "medicinalingredient": "main.health_canada.noc_medicinalingredient",
    "dosageform": "main.health_canada.noc_dosageform",
    "route": "main.health_canada.noc_route",
    "dpd/drugproduct": "main.health_canada.dpd_drugproduct",
    "dpd/status": "main.health_canada.dpd_status",
}

COLUMNS = {  # only the columns the tracker uses
    "noticeofcompliancemain": [
        "noc_number", "noc_date", "noc_manufacturer_name", "noc_status_with_conditions", "noc_on_submission_type",
        "noc_submission_class", "noc_product_type", "noc_crp_product_name", "noc_crp_company_name",
        "noc_crp_country_name", "noc_active_status", "noc_reason_supplement", "noc_reason_submission",
        "noc_therapeutic_class"],
    "drugproduct": ["noc_number", "noc_br_product_id", "noc_br_brandname", "noc_br_din"],
    "medicinalingredient": ["noc_pi_din_product_id", "noc_pi_medic_ingr_name", "noc_pi_strength", "noc_pi_unit"],
    "dosageform": ["noc_pf_din_product_id", "noc_pf_form_name"],
    "route": ["noc_pr_din_product_id", "noc_pr_route"],
    "dpd/drugproduct": ["drug_code", "drug_identification_number", "brand_name", "company_name"],
    "dpd/status": ["drug_code", "status"],
}

CACHE_SECONDS = 3600  # reload the tables at most once an hour (they refresh daily)

# For local testing only: a folder of the raw API JSON files (<dir>/<source>.json) used instead of the tables.
LOCAL_JSON_DIR = os.getenv("PM_TRACKER_JSON_DIR", "")

# ----------------------------------------------------------------------------------------------------------------
# Loading
# ----------------------------------------------------------------------------------------------------------------
ID_COLS = {"noc_number", "noc_br_product_id", "noc_pi_din_product_id", "noc_pf_din_product_id",
           "noc_pr_din_product_id", "drug_code"}
DIN_COLS = {"noc_br_din", "drug_identification_number"}


def clean(rows):
    """Make table rows look like the API JSON: no NULLs, ISO date strings, integer IDs, 8-digit DINs."""
    out = []
    for r in rows:
        c = {}
        for k, v in r.items():
            if v is None:
                v = ""
            elif isinstance(v, (dt.date, dt.datetime)):
                v = v.isoformat()[:10]
            elif isinstance(v, Decimal):
                v = float(v)
            if k in ID_COLS:
                try:
                    v = int(v)
                except (TypeError, ValueError):
                    pass
            elif k in DIN_COLS and isinstance(v, int):
                v = f"{v:08d}"  # a DIN stored as a number loses its leading zeros
            elif k == "noc_active_status" and not isinstance(v, str):
                v = "1" if v == 1 else "0"
            c[k] = v
        out.append(c)
    return out


def load_sources():
    if LOCAL_JSON_DIR:
        return {k: clean(json.load(open(os.path.join(LOCAL_JSON_DIR, f"{k}.json"), encoding="utf-8")))
                for k in TABLES}
    if not WAREHOUSE_ID:
        raise RuntimeError("No SQL warehouse set. Set DATABRICKS_WAREHOUSE_ID or WAREHOUSE_ID in app.py.")
    from databricks import sql
    from databricks.sdk.core import Config

    cfg = Config()  # in Databricks Apps this authenticates as the app's service principal
    host = re.sub(r"^https?://", "", cfg.host).rstrip("/")
    src = {}
    with sql.connect(server_hostname=host, http_path=f"/sql/1.0/warehouses/{WAREHOUSE_ID}",
                     credentials_provider=lambda: cfg.authorize) as conn:
        with conn.cursor() as cur:
            for key, table in TABLES.items():
                cur.execute(f"SELECT {', '.join(COLUMNS[key])} FROM {table}")
                src[key] = clean(cur.fetchall_arrow().to_pylist())
    return src


# ----------------------------------------------------------------------------------------------------------------
# Build: one record per CRP (same rules as scripts/build.py)
# ----------------------------------------------------------------------------------------------------------------
INNOVATOR_TYPES = {"New Drug Submission", "Supplement to a New"}
MARKET_ORDER = ["Marketed", "Approved", "Dormant"]
FORMWORDS = (r"\b(TABLETS?|CAPLETS?|CAPSULES?|CAPS?|TABS?|LIQUI-?GELS?|INJECTION|INJ|SOLUTION|"
             r"SUSPENSION|CREAM|OINTMENT|SYRUP|ORAL|FOR|XR|SR)\b")


def norm(s):
    return re.sub(r"\s+", " ", (s or "").strip().upper())


def is_apo(r):
    return "APOTEX" in r["noc_manufacturer_name"].upper()


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

    def products(n):
        """Product rows for one NOC: [brand, din, ingredients, form, route]."""
        rows = []
        for p in prods_by_noc.get(n, []):
            pid = p["noc_br_product_id"]
            ings = "; ".join(f"{i} {s}".strip() for i, s in ing_by_prod.get(pid, []))
            din = (p["noc_br_din"] or "").strip()
            rows.append([p["noc_br_brandname"].strip(), "" if din.upper() in ("N/A", "") else din, ings,
                         ", ".join(sorted(form_by_prod.get(pid, []))), ", ".join(sorted(route_by_prod.get(pid, [])))])
        return rows

    def ingredient_names(nocs):
        return {i for n in nocs for p in prods_by_noc.get(n, []) for i, _ in ing_by_prod.get(p["noc_br_product_id"], [])}

    crp_refs = C.defaultdict(list)  # generic submissions grouped by the CRP they reference
    for r in main:
        name = norm(r["noc_crp_product_name"])
        if name:
            crp_refs[name].append(r)

    apo_ings = C.defaultdict(set)  # frozenset of ingredients (one product) -> Apotex NOC numbers
    for r in main:
        if is_apo(r):
            for p in prods_by_noc.get(r["noc_number"], []):
                key = frozenset(i for i, _ in ing_by_prod.get(p["noc_br_product_id"], []))
                if key:
                    apo_ings[key].add(r["noc_number"])

    used_nocs, crps = set(), []
    for name, refs in crp_refs.items():
        # most common spelling; on a tie, the one in the earliest filing (refs are in date order)
        company = C.Counter(norm(r["noc_crp_company_name"]) for r in refs).most_common(1)[0][0]
        country = C.Counter(norm(r["noc_crp_country_name"]) for r in refs).most_common(1)[0][0]
        # the CRP's own (innovator) NOCs: brand name matches, NDS / SNDS submission
        own = sorted((n for n in nocs_by_brand.get(name, ())
                      if noc[n]["noc_on_submission_type"] in INNOVATOR_TYPES and not is_apo(noc[n])),
                     key=lambda n: (noc[n]["noc_date"], str(n)))
        apo = sorted((r["noc_number"] for r in refs if is_apo(r)), key=lambda n: (noc[n]["noc_date"], str(n)))
        generic_makers = {norm(r["noc_manufacturer_name"]) for r in refs}

        # product details come from the CRP's own NOCs; fall back to the generics that reference it
        detail_src = own or [r["noc_number"] for r in refs]
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

        same_ing = set() if apo else set(apo_ings.get(frozenset(ings), set()))
        status = "filed" if apo else ("ingredient" if same_ing else "none")
        apo_list = apo or sorted(same_ing, key=lambda n: (noc[n]["noc_date"], str(n)))

        tclass = next((noc[n]["noc_therapeutic_class"].strip() for n in reversed(own)
                       if noc[n]["noc_therapeutic_class"].strip()), "")
        ptype = C.Counter(noc[n]["noc_product_type"] for n in detail_src).most_common(1)[0][0]
        used_nocs.update(own, apo_list)
        crps.append({
            "n": name, "co": company, "ct": country, "pt": ptype, "tc": tclass,
            "ing": sorted(ings), "fm": sorted(pforms), "rt": sorted(proutes),
            "din": sorted(dins), "own": own, "apo": apo_list, "st": status,
            "ab": sorted({b for n in apo_list for b, *_ in products(n)}),
            "ad": sorted({din for n in apo_list for _, din, *_ in products(n) if din}),
            "gn": len(refs), "gm": len(generic_makers), "first": min(r["noc_date"] for r in refs),
        })

    nocs_out = {}
    for n in used_nocs:
        r = noc[n]
        nocs_out[n] = [r["noc_date"], r["noc_manufacturer_name"].strip(), r["noc_on_submission_type"].strip(),
                       r["noc_submission_class"].strip(), r["noc_reason_submission"].strip(),
                       r["noc_reason_supplement"].strip(), r["noc_status_with_conditions"], products(n)]

    # ---- CRP market status from the Drug Product Database ----
    dpd_status = {s["drug_code"]: s["status"] for s in src["dpd/status"]}
    by_din, by_name, by_core, by_strip = (C.defaultdict(list) for _ in range(4))
    for p in sorted(src["dpd/drugproduct"], key=lambda p: str(p["drug_code"])):
        n = norm(p["brand_name"])
        by_din[p["drug_identification_number"]].append(p)
        by_name[n].append(p)
        by_core[core(n)].append(p)
        by_strip[stripped(n)].append(p)
    strip_keys = sorted(by_strip)

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
        c["hay"] = " ".join([c["n"], c["co"], c["tc"], " ".join(c["ing"]), " ".join(c["din"]),
                             " ".join(c["ab"]), " ".join(c["ad"])]).lower()
        c["latest"] = nocs_out[c["own"][-1]][0] if c["own"] else ""
        c["apoLatest"] = nocs_out[c["apo"][-1]][0] if c["apo"] else ""
    lc = {n: (r[4].lower(), r[5].lower()) for n, r in nocs_out.items()}
    return {"asOf": latest, "crps": crps, "nocs": nocs_out, "lc": lc}


@st.cache_resource(ttl=CACHE_SECONDS, show_spinner="Loading the Health Canada tables...")
def get_data():
    return build(load_sources())


# ----------------------------------------------------------------------------------------------------------------
# Page
# ----------------------------------------------------------------------------------------------------------------
STATUS = {"filed": "Apotex NOC on file", "ingredient": "Same molecule, other CRP", "none": "No Apotex NOC"}
STATUS_DOT = {"filed": "🟢", "ingredient": "🟠", "none": "🔴"}
MARKET = {"Marketed": "Active (marketed)", "Approved": "Approved, not marketed", "Dormant": "Dormant",
          "Cancelled": "Cancelled", "Not found": "Not found in DPD"}
MARKET_OPTIONS = ["Marketed", "Approved", "Dormant", "Cancelled", "Not found", ""]
SCOPES = {"both": "CRP and Apotex NOCs", "crp": "CRP NOCs only", "apo": "Apotex NOCs only"}
SORTS = {"name": "CRP name, A to Z", "filers": "Most generic filers", "latest": "Latest CRP NOC",
         "apolatest": "Latest Apotex NOC", "gap": "Unfiled first, most filers"}
CHIPS = {
    "rsub": ["name change", "merger", "licensing", "product monograph", "labelling", "new manufacturer"],
    "rsupp": ["product monograph", "safety", "new indication", "new strength", "manufacturing", "site",
              "dosage form", "formulation", "pediatric", "CRP"],
}
# full-width elements: Streamlit 1.50+ takes width="stretch", older versions use_container_width=True
STRETCH = ({"width": "stretch"} if tuple(map(int, re.findall(r"\d+", st.__version__)[:2])) >= (1, 50)
           else {"use_container_width": True})
DEFAULTS = {"q": "", "rsub": "", "rsupp": "", "rsub_pick": [], "rsupp_pick": [], "scope": "both", "ptype": "",
            "market": "Marketed", "sort": "name", "status": ""}


def parse(q):
    """Comma separates alternatives; inside one, every word or "quoted phrase" must appear."""
    groups = []
    for g in q.split(","):
        terms = [(p or w).lower() for p, w in re.findall(r'"([^"]+)"|(\S+)', g)]
        if terms:
            groups.append(terms)
    return groups


def matches(text, groups):
    return any(all(t in text for t in g) for g in groups)


def reset_filters():
    for k, v in DEFAULTS.items():
        st.session_state[k] = list(v) if isinstance(v, list) else v


def noc_frame(nocs, lc, numbers, highlight, q_sub, q_supp):
    rows = []
    for n in reversed(numbers):  # newest first
        r = nocs[n]
        row = {
            "NOC": str(n), "Date": r[0], "Submission": r[2], "Class": r[3] + (" · with conditions" if r[6] == "Y" else ""),
            "Company": r[1], "Reason for submission": r[4] or "—", "Reason for supplement": r[5] or "—",
            "Products · DIN · strength · form": " | ".join(
                " · ".join(x for x in (p[0], p[1] or "no DIN", p[2], p[3]) if x) for p in r[7]) or "—",
        }
        if highlight:
            hit = (q_sub and matches(lc[n][0], q_sub)) or (q_supp and matches(lc[n][1], q_supp))
            row = {"Matches reason filter": "✔" if hit else "", **row}
        rows.append(row)
    return pd.DataFrame(rows)


def main_page():
    st.set_page_config(page_title="PM Update Tracker", page_icon="💊", layout="wide")
    for k, v in DEFAULTS.items():
        st.session_state.setdefault(k, list(v) if isinstance(v, list) else v)

    try:
        D = get_data()
    except Exception as e:  # show setup problems on the page instead of a stack trace
        st.title("PM Update Tracker")
        st.error(f"Could not load the data: {e}")
        st.info("Check TABLES and WAREHOUSE_ID at the top of app.py, and that the app's service principal has "
                "CAN USE on the SQL warehouse and SELECT on the 7 tables.")
        st.stop()
    CRPS, NOC, LC = D["crps"], D["nocs"], D["lc"]

    head, src = st.columns([4, 1])
    head.title("PM Update Tracker")
    head.caption("Every Canadian Reference Product (CRP) named in a generic submission, with its own NOCs, DINs and "
                 "product details, set against Apotex's NOCs for the same product.")
    src.caption(f"Source: Health Canada NOC and Drug Product Database  \nData current to **{D['asOf']}**")
    if src.button("Reload data", help="Read the tables again now instead of waiting for the hourly refresh"):
        get_data.clear()
        st.rerun()

    # ---- filters ----
    with st.container(border=True):
        c1, c2, c3 = st.columns([1.1, 1, 1])
        c1.text_input("Search products", key="q", placeholder="CRP name, ingredient, company, DIN or APO- brand",
                      help="Searches CRP name, company, ingredients, therapeutic class, DINs and Apotex brands.")
        c2.text_input("Reason for submission", key="rsub", placeholder="e.g. name change")
        c2.multiselect("Common terms", CHIPS["rsub"], key="rsub_pick", label_visibility="collapsed",
                       placeholder="Add common terms")
        c3.text_input("Reason for supplement", key="rsupp", placeholder="e.g. new strength")
        c3.multiselect("Common terms", CHIPS["rsupp"], key="rsupp_pick", label_visibility="collapsed",
                       placeholder="Add common terms")

        mk_count = C.Counter(c["ms"] for c in CRPS)
        ptypes = [""] + sorted({c["pt"] for c in CRPS})
        d1, d2, d3, d4, d5 = st.columns([1, 1, 1, 1, 0.6], vertical_alignment="bottom")
        d1.selectbox("Apply reason filters to", list(SCOPES), key="scope", format_func=SCOPES.get)
        d2.selectbox("Product type", ptypes, key="ptype", format_func=lambda p: p or "All product types")
        d3.selectbox("CRP market status", MARKET_OPTIONS, key="market",
                     format_func=lambda m: (f"{'Active (marketed) only' if m == 'Marketed' else MARKET[m]} "
                                            f"({mk_count.get(m, 0):,})") if m else f"All market statuses ({len(CRPS):,})")
        d4.selectbox("Sort by", list(SORTS), key="sort", format_func=SORTS.get)
        d5.button("Clear filters", on_click=reset_filters, **STRETCH)
        st.caption('Reason filters match every word you type, in any order. Put a phrase in "quotes" to match it '
                   'exactly, and separate alternatives with a comma: `strength, "new indication"`. '
                   'Common terms you add are alternatives too.')

    ss = st.session_state
    q_text = parse(ss.q.replace(",", " "))
    q_sub = parse(ss.rsub) + [[t.lower()] for t in ss.rsub_pick]
    q_supp = parse(ss.rsupp) + [[t.lower()] for t in ss.rsupp_pick]

    def pool(c):
        return c["own"] if ss.scope == "crp" else c["apo"] if ss.scope == "apo" else c["own"] + c["apo"]

    def keep(c):
        if ss.ptype and c["pt"] != ss.ptype:
            return False
        if ss.market and c["ms"] != ss.market:
            return False
        if q_text and not matches(c["hay"], q_text):
            return False
        if q_sub and not any(matches(LC[n][0], q_sub) for n in pool(c)):
            return False
        if q_supp and not any(matches(LC[n][1], q_supp) for n in pool(c)):
            return False
        return True

    base = [c for c in CRPS if keep(c)]

    # ---- status summary ----
    cnt = C.Counter(c["st"] for c in base)
    tot = len(base) or 1
    m0, m1, m2, m3 = st.columns(4)
    m0.metric("CRP products", f"{len(base):,}")
    for col, k in zip((m1, m2, m3), STATUS):
        col.metric(f"{STATUS_DOT[k]} {STATUS[k]}", f"{cnt[k]:,}", f"{round(cnt[k] / tot * 100)}% of CRPs shown",
                   delta_color="off")
    st.radio("Apotex status", ["", *STATUS], key="status", horizontal=True,
             format_func=lambda k: f"{STATUS_DOT[k]} {STATUS[k]} ({cnt[k]:,})" if k else f"All ({len(base):,})")

    view = [c for c in base if c["st"] == ss.status] if ss.status else list(base)
    view.sort(key=lambda c: c["n"])
    if ss.sort == "filers":
        view.sort(key=lambda c: -c["gm"])
    elif ss.sort == "latest":
        view.sort(key=lambda c: c["latest"], reverse=True)
    elif ss.sort == "apolatest":
        view.sort(key=lambda c: c["apoLatest"], reverse=True)
    elif ss.sort == "gap":
        rank = {"none": 0, "ingredient": 1, "filed": 2}
        view.sort(key=lambda c: (rank[c["st"]], -c["gm"]))

    # ---- results ----
    r1, r2 = st.columns([3, 1], vertical_alignment="bottom")
    r1.subheader(f"{len(view):,} CRP product{'' if len(view) == 1 else 's'}"
                 + (f" · {STATUS[ss.status]}" if ss.status else ""))

    export = pd.DataFrame([{
        "CRP product": c["n"], "CRP company": c["co"], "Ingredients": " + ".join(c["ing"]),
        "Dosage form": ", ".join(c["fm"]), "Route": ", ".join(c["rt"]), "Product type": c["pt"],
        "CRP market status": MARKET[c["ms"]], "CRP NOCs": " ".join(map(str, c["own"])), "Latest CRP NOC": c["latest"],
        "CRP DINs": " ".join(c["din"]), "Generic filers": c["gm"], "Apotex brands": ", ".join(c["ab"]),
        "Apotex NOCs": " ".join(map(str, c["apo"])), "Apotex DINs": " ".join(c["ad"]), "Apotex status": STATUS[c["st"]],
    } for c in view])
    buf = io.StringIO()
    export.to_csv(buf, index=False)
    r2.download_button("Download results (CSV)", buf.getvalue().encode("utf-8-sig"),
                       file_name=f"pm_update_tracker_{D['asOf']}.csv", mime="text/csv", **STRETCH)

    if not view:
        st.info('No CRP products match these filters. Try fewer words, or set "Apply reason filters to" to both.')
        return

    table = pd.DataFrame([{
        "CRP product": c["n"],
        "CRP company": c["co"] + (f" · {c['ct']}" if c["ct"] and c["ct"] != "CANADA" else ""),
        "Market status": MARKET[c["ms"]],
        "Ingredients": " + ".join(c["ing"]) or "—",
        "Form · route": " · ".join(x for x in (", ".join(c["fm"]), ", ".join(c["rt"])) if x) or "—",
        "Product type": c["pt"],
        "Therapeutic class": c["tc"],
        "Latest CRP NOC": (f"NOC {c['own'][-1]} · {c['latest']}" if c["own"] else "None (pre-1994 or foreign)"),
        "CRP NOCs": len(c["own"]),
        "CRP DINs": ", ".join(c["din"]),
        "Generic filers": c["gm"],
        "Generic NOCs": c["gn"],
        "Apotex product": ", ".join(c["ab"]) or "—",
        "Latest Apotex NOC": f"NOC {c['apo'][-1]} · {c['apoLatest']}" if c["apo"] else "",
        "Apotex DINs": ", ".join(c["ad"]),
        "Apotex status": f"{STATUS_DOT[c['st']]} {STATUS[c['st']]}",
    } for c in view])

    st.caption("Select a row to see its full NOC history below the table.")
    selected = None
    try:
        event = st.dataframe(table, hide_index=True, **STRETCH, height=520,
                             on_select="rerun", selection_mode="single-row", key="results")
        if event.selection.rows:
            selected = view[event.selection.rows[0]]
    except TypeError:  # Streamlit older than 1.35 has no row selection: pick from a list instead
        st.dataframe(table, hide_index=True, **STRETCH, height=520)
        names = [c["n"] for c in view]
        pick = st.selectbox("Show NOC history for", [""] + names, format_func=lambda n: n or "Choose a CRP")
        selected = view[names.index(pick)] if pick else None

    # ---- detail ----
    if selected:
        c = selected
        country = f" ({c['ct']})" if c["ct"] else ""
        with st.container(border=True):
            st.subheader(c["n"])
            st.markdown(
                f"**CRP company** {c['co']}{country} &nbsp;·&nbsp; "
                f"**Market status** {MARKET[c['ms']]} &nbsp;·&nbsp; **Apotex status** {STATUS_DOT[c['st']]} {STATUS[c['st']]}  \n"
                f"**CRP DINs** `{', '.join(c['din']) or 'none on record'}` &nbsp;·&nbsp; "
                f"**Apotex DINs** `{', '.join(c['ad']) or 'none'}` &nbsp;·&nbsp; **First generic NOC** {c['first']}")

            st.markdown("##### CRP in the Drug Product Database")
            if c["mp"]:
                st.caption(f"Matched by {c['mh']}")
                st.dataframe(pd.DataFrame(c["mp"], columns=["DIN", "Brand name", "Current status", "Company"]),
                             hide_index=True, **STRETCH)
            else:
                st.caption("The CRP name did not match any DIN or brand name in the DPD.")

            reasons_on = bool(q_sub or q_supp)
            st.markdown(f"##### CRP NOCs")
            st.caption(f"{len(c['own'])} on record, newest first")
            if c["own"]:
                st.dataframe(noc_frame(NOC, LC, c["own"], reasons_on and ss.scope != "apo", q_sub, q_supp),
                             hide_index=True, **STRETCH)
            else:
                st.caption("None on record.")

            st.markdown("##### " + ("Apotex NOCs for the same ingredients (filed against another CRP)"
                                    if c["st"] == "ingredient" else "Apotex NOCs referencing this CRP"))
            st.caption(f"{len(c['apo'])} on record, newest first")
            if c["apo"]:
                st.dataframe(noc_frame(NOC, LC, c["apo"], reasons_on and ss.scope != "crp", q_sub, q_supp),
                             hide_index=True, **STRETCH)
            else:
                st.caption("None on record.")

    with st.expander("How status is set, and limits"):
        st.markdown(
            "**Apotex status.** *Apotex NOC on file*: Apotex holds at least one NOC whose submission names this "
            "product as its CRP. *Same molecule, other CRP*: no Apotex NOC names this CRP, but Apotex holds a NOC for "
            "a product with exactly the same medicinal ingredients (for example one filed against an older or foreign "
            "reference). *No Apotex NOC*: neither applies.\n\n"
            "**CRP market status** comes from Health Canada's Drug Product Database (DPD). A CRP is active when at "
            "least one of its DINs is currently marketed. Products are matched by DIN first, then by brand name, then "
            "by close spelling to catch typos in the CRP names filed with generic submissions. Where a name matches "
            "several companies, the CRP company's own products are used. Inactive NOC records are excluded throughout.\n\n"
            "**Limits.** The NOC database lists approved submissions only, so filings still under review do not "
            "appear. CRP NOCs are matched by brand name on New Drug Submissions and their supplements. Products "
            "approved before 1994 often have no NOC record, and their details are taken from the generic NOCs that "
            "reference them.\n\n"
            "Data: Health Canada, [Open Government Licence – Canada](https://open.canada.ca/en/open-government-licence-canada).")


main_page()
