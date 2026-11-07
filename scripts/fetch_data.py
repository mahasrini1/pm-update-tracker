"""Download the Health Canada NOC and Drug Product Database (DPD) data the tracker is built from.

Usage: python scripts/fetch_data.py raw
Writes <dir>/<endpoint>.json for the NOC API and <dir>/dpd/<endpoint>.json for the DPD API.
"""
import json
import os
import sys
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


def main(out_dir):
    for base, endpoint, sub in DOWNLOADS:
        folder = os.path.join(out_dir, sub)
        os.makedirs(folder, exist_ok=True)
        url = f"{base}/{endpoint}/?lang=en&type=json"
        print(f"Downloading {url}")
        body = fetch(url)
        with open(os.path.join(folder, f"{endpoint}.json"), "wb") as f:
            f.write(body)
        print(f"  {len(body) / 1e6:.1f} MB")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "raw")
