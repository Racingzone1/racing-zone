#!/usr/bin/env python3
"""
Sync stock from Ozparts stocklist APIs to Shopify metafields.
Runs via GitHub Actions on a schedule (every 6 hours).

Vendors synced:
  - DBA (Disc Brakes Australia) — stocklist API p=5e109cda...
  - ACS (Xtreme Performance clutch kits) — stocklist API p=5e1eb82b...
  - Whiteline — stocklist API p=5e1eb8b3...

Updates per product:
  - custom.stoc_text   → "Pe stoc european (X buc.)" / "Disponibil la comandă" / "Livrare specială"
  - custom.stoc_livrare → delivery time estimate
  - custom.stoc_color  → green / orange / red
  - custom.stoc_qty    → numeric quantity
"""

import urllib.request
import urllib.error
import json
import os
import sys
import time

# ─── Config from environment (GitHub Secrets) ────────────────────────────────
SHOP          = os.environ.get("SHOPIFY_SHOP", "k21m1k-k0.myshopify.com")
CLIENT_ID     = os.environ["SHOPIFY_CLIENT_ID"]
CLIENT_SECRET = os.environ["SHOPIFY_CLIENT_SECRET"]

STOCKLIST_URL = (
    "https://3cerp.eu/api/stocklist/"
    "?p=5e109cda664d6a4351e6eef6"
    "&u=69c9076cca5cb16255ddf9ac"
    "&f=json"
)

ACS_STOCKLIST_URL = (
    "https://3cerp.eu/api/stocklist/"
    "?p=5e1eb82b86eb633860334f5d"
    "&u=69c9076cca5cb16255ddf9ac"
    "&f=json"
)

WHITELINE_STOCKLIST_URL = (
    "https://3cerp.eu/api/stocklist/"
    "?p=5e1eb8b386eb633860334f5f"
    "&u=69c9076cca5cb16255ddf9ac"
    "&f=json"
)

# ─── Shopify helpers ─────────────────────────────────────────────────────────

def get_token():
    data = (
        f"grant_type=client_credentials"
        f"&client_id={CLIENT_ID}"
        f"&client_secret={CLIENT_SECRET}"
    )
    req = urllib.request.Request(
        f"https://{SHOP}/admin/oauth/access_token",
        data=data.encode(),
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())["access_token"]


def shopify_get(token, path):
    for attempt in range(4):
        req = urllib.request.Request(
            f"https://{SHOP}/admin/api/2024-01/{path}",
            headers={"X-Shopify-Access-Token": token},
        )
        try:
            with urllib.request.urlopen(req) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < 3:
                wait = 10 * (2 ** attempt)  # 10s, 20s, 40s
                print(f"  429 rate limit, retrying in {wait}s... (attempt {attempt+1}/3)")
                time.sleep(wait)
            else:
                raise


def shopify_put(token, path, payload):
    for attempt in range(4):
        body = json.dumps(payload).encode()
        req = urllib.request.Request(
            f"https://{SHOP}/admin/api/2024-01/{path}",
            data=body,
            headers={
                "X-Shopify-Access-Token": token,
                "Content-Type": "application/json",
            },
            method="PUT",
        )
        try:
            with urllib.request.urlopen(req) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < 3:
                wait = 10 * (2 ** attempt)
                print(f"  429 rate limit, retrying in {wait}s... (attempt {attempt+1}/3)")
                time.sleep(wait)
            else:
                raise


def shopify_post(token, path, payload):
    for attempt in range(4):
        body = json.dumps(payload).encode()
        req = urllib.request.Request(
            f"https://{SHOP}/admin/api/2024-01/{path}",
            data=body,
            headers={
                "X-Shopify-Access-Token": token,
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < 3:
                wait = 10 * (2 ** attempt)
                print(f"  429 rate limit, retrying in {wait}s... (attempt {attempt+1}/3)")
                time.sleep(wait)
            else:
                raise


# ─── Stock logic ─────────────────────────────────────────────────────────────

def get_stock_status(stock_item):
    """Return (stoc_text, stoc_livrare, stoc_color, stoc_qty) from stocklist entry."""
    if not stock_item:
        return "Livrare specială", "Termen la cerere", "red", 0

    available = (stock_item.get("available") or 0) + (stock_item.get("available2") or 0)
    mfr       = stock_item.get("manufacturerstock") or 0
    category  = stock_item.get("stockcategory", "")

    if available > 0:
        return f"Pe stoc european ({available} buc.)", "Livrabil în 2-5 zile", "green", available
    elif mfr > 0:
        return "Disponibil la comandă", "Contactați-ne pentru informații despre livrare", "orange", 0
    elif category == "stockeditem":
        return "Disponibil la comandă", "Contactați-ne pentru informații despre livrare", "orange", 0
    else:
        return "Livrare specială", "Termen la cerere", "red", 0


# ─── Main sync ───────────────────────────────────────────────────────────────

def fetch_stock(url):
    req = urllib.request.Request(url, headers={"User-Agent": "RacingZone-Sync/1.0"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return {item["name"]: item for item in json.loads(r.read())}

def sync_vendor(token, vendor, stock_idx, updated_total, skipped_total, errors_total):
    print(f"\nFetching {vendor} products from Shopify...")
    all_products = []
    url = f"products.json?vendor={vendor}&limit=250&fields=id,title,variants"
    resp = shopify_get(token, url)
    all_products = resp.get("products", [])
    print(f"  {len(all_products)} {vendor} products found")

    updated = skipped = errors = 0
    for product in all_products:
        pid   = product["id"]
        variants = product.get("variants", [])
        if not variants: continue
        sku = variants[0].get("sku", "")
        if not sku: continue

        stock_item = stock_idx.get(sku)
        new_text, new_livrare, new_color, new_qty = get_stock_status(stock_item)

        try:
            mf_resp = shopify_get(token, f"products/{pid}/metafields.json?namespace=custom")
            existing = {m["key"]: m for m in mf_resp.get("metafields", [])}
        except Exception as e:
            print(f"  ERROR reading metafields for {sku}: {e}")
            errors += 1
            continue

        current_color = existing.get("stoc_color", {}).get("value", "")
        current_qty   = str(existing.get("stoc_qty", {}).get("value", "-1"))

        if current_color == new_color and current_qty == str(new_qty):
            skipped += 1
            continue

        updates = {
            "stoc_text":    (new_text,     "single_line_text_field"),
            "stoc_livrare": (new_livrare,  "single_line_text_field"),
            "stoc_color":   (new_color,    "single_line_text_field"),
            "stoc_qty":     (str(new_qty), "number_integer"),
        }
        ok = True
        for key, (value, mf_type) in updates.items():
            try:
                if key in existing:
                    mf_id = existing[key]["id"]
                    shopify_put(token, f"metafields/{mf_id}.json",
                        {"metafield": {"id": mf_id, "value": value, "type": mf_type}})
                else:
                    shopify_post(token, f"products/{pid}/metafields.json", {"metafield": {
                        "namespace": "custom", "key": key, "value": value, "type": mf_type}})
                time.sleep(0.2)
            except Exception as e:
                print(f"  ERROR updating {key} for {sku}: {e}")
                ok = False

        if ok:
            updated += 1
            print(f"  UPDATED {sku}: {current_color}({current_qty}) → {new_color}({new_qty})")
        else:
            errors += 1
        time.sleep(0.3)

    print(f"  {vendor}: {updated} updated, {skipped} unchanged, {errors} errors")
    return updated_total + updated, skipped_total + skipped, errors_total + errors


def main():
    print("=== Stock Sync: DBA + ACS + Whiteline ===")

    # 1. Shopify token
    token = get_token()
    print("Shopify token OK")

    # 2. Fetch all stocklists
    print("\nFetching stocklist APIs...")
    dba_stock       = fetch_stock(STOCKLIST_URL)
    acs_stock       = fetch_stock(ACS_STOCKLIST_URL)
    whiteline_stock = fetch_stock(WHITELINE_STOCKLIST_URL)
    print(f"  DBA: {len(dba_stock)} SKUs | ACS: {len(acs_stock)} SKUs | Whiteline: {len(whiteline_stock)} SKUs")

    # 3. Sync each vendor
    updated = 0
    skipped = 0
    errors  = 0

    updated, skipped, errors = sync_vendor(token, "DBA",      dba_stock,       updated, skipped, errors)
    updated, skipped, errors = sync_vendor(token, "ACS",      acs_stock,       updated, skipped, errors)
    updated, skipped, errors = sync_vendor(token, "Whiteline", whiteline_stock, updated, skipped, errors)

    print(f"\n=== Done: {updated} updated, {skipped} unchanged, {errors} errors ===")
    if errors > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
