# PM Update Tracker

A web page that lists every Canadian Reference Product (CRP) named in a generic drug submission in Canada, with the CRP's own Notices of Compliance (NOCs), DINs and product details, and whether Apotex holds a NOC for the same product.

The data is rebuilt from Health Canada's public APIs every night and published with GitHub Pages.

## What the page shows

For each CRP:

- Brand name, company, medicinal ingredients, dosage form, route, product type and therapeutic class
- The CRP's own NOCs (New Drug Submissions and their supplements) and DINs
- Market status from the Drug Product Database (DPD). The page shows active (marketed) CRPs by default.
- How many generic companies have filed against it
- The matching Apotex products, DINs and NOCs, and an Apotex status:

| Status | Meaning |
|---|---|
| Apotex NOC on file | An Apotex NOC names this product as its CRP |
| Same molecule, other CRP | No Apotex NOC names this CRP, but Apotex has a NOC for a product with exactly the same medicinal ingredients |
| No Apotex NOC | Neither applies |

You can filter by words in the **reason for submission** and **reason for supplement**, by product search, by product type and by market status. Select a row to see its full NOC history.

## Data sources

All data comes from Health Canada under the [Open Government Licence – Canada](https://open.canada.ca/en/open-government-licence-canada):

- [Notice of Compliance API](https://health-products.canada.ca/api/documentation/noc-documentation-en.html): `noticeofcompliancemain`, `drugproduct`, `medicinalingredient`, `dosageform`, `route`
- [Drug Product Database API](https://health-products.canada.ca/api/documentation/dpd-documentation-en.html): `drugproduct`, `status`

Inactive NOC records (`noc_active_status` other than 1) are excluded.

### Limits

- The NOC database lists approved submissions only. Filings still under review do not appear.
- CRP NOCs are matched by brand name. Products approved before 1994 often have no NOC record, so their details come from the generic NOCs that reference them.
- CRPs are matched to the DPD by DIN, then brand name, then close spelling, because many CRP names in generic filings contain typos. CRPs that match nothing are listed as "Not found in DPD".

## How it is built

```
scripts/fetch_data.py   downloads the API data into raw/
scripts/build.py        joins it into data.json (one record per CRP)
scripts/assemble.py     embeds data.json into src/template.html -> site/index.html
.github/workflows/publish.yml   runs all three nightly and on every push, then deploys site/ to GitHub Pages
```

To run it on your own computer (Python 3.10 or later, no extra packages):

```bash
python scripts/fetch_data.py raw
python scripts/build.py raw data.json
python scripts/assemble.py src/template.html data.json site/index.html
```

Then open `site/index.html` in a browser.

To refresh the published page without waiting for the nightly run, open the **Actions** tab on GitHub, choose **Refresh data and publish tracker** and select **Run workflow**.
