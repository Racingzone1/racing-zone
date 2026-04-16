#!/usr/bin/env python3
"""
Sync DBA product stock from Ozparts stocklist API to Shopify metafields.
Runs via GitHub Actions on a schedule (every 6 hours).

Updates per product:
  - custom.stoc_text   → "Pe stoc european (X buc.)" / "Disponibil la comandă" / "Livrare specială"
  - custom.stoc_livrare → delivery time estimate
  - custom.stoc_color  → green / orange / red
  - custom.stoc_qty    → numeric quantity
"""

import urllib.request
import json
import os
import sys
import time

# ─── Config from environment (GitHub Secrets) ────────────────────────────────
SHOP         = os.environ.get("SHOPIFY_SHOP", "k21m1k-k0.myshopify.com")
CLIENT_ID    = os.environ["SHOPIFY_CLIENT_ID"]
CLIENT_SECRET = os.environ["SHOPIFY_CLIENT_SECRET"]

STOCKLIST_URL = (
    "https://3cerp.eu/api/stocklist/"
    "?p=5e109cda664d6a4351e6eef6"
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
    req = urllib.request.Request(
        f"https://{SHOP}/admin/api/2024-01/{path}",
        headers={"X-Shopify-Access-Token": token},
    )
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())


def shopify_put(token, path, payload):
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
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())


def shopify_post(token, path, payload):
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
    with urllib.request.urlopen(req) as r:
        return json.loads(r.read())


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
        return "Disponibil la comandă", "Livrabil în 7-14 zile", "orange", 0
    elif category == "stockeditem":
        return "Disponibil la comandă", "Livrabil în 14-21 zile", "orange", 0
    else:
        return "Livrare specială", "Termen la cerere", "red", 0


# ─── Main sync ───────────────────────────────────────────────────────────────

def main():
    print("=== DBA Stock Sync ===")

    # 1. Shopify token
    token = get_token()
    print("Shopify token OK")

    # 2. Fetch stocklist
    print("Fetching stocklist API...")
    req = urllib.request.Request(STOCKLIST_URL, headers={"User-Agent": "RacingZone-Sync/1.0"})
    with urllib.request.urlopen(req, timeout=60) as r:
        stock_data = json.loads(r.read())
    stock_idx = {item["name"]: item for item in stock_data}
    print(f"  {len(stock_idx)} products in stocklist")

    # 3. Get all DBA products from Shopify
    print("Fetching DBA products from Shopify...")
    all_products = []
    url = "products.json?vendor=DBA&limit=250&fields=id,title,variants"
    while True:
        resp = shopify_get(token, url)
        batch = resp.get("products", [])
        all_products.extend(batch)
        if len(batch) < 250:
            break
        # Simple: stop after first page (250 products is plenty for now)
        break
    print(f"  {len(all_products)} DBA products found")

    # 4. Sync each product
    updated = 0
    skipped = 0
    errors  = 0

    for product in all_products:
        pid   = product["id"]
        title = product["title"][:50]

        # Get SKU from first variant
        variants = product.get("variants", [])
        if not variants:
            continue
        sku = variants[0].get("sku", "")
        if not sku:
            continue

        # Look up stock
        stock_item = stock_idx.get(sku)
        new_text, new_livrare, new_color, new_qty = get_stock_status(stock_item)

        # Get current metafields
        try:
            mf_resp = shopify_get(token, f"products/{pid}/metafields.json?namespace=custom")
            existing = {m["key"]: m for m in mf_resp.get("metafields", [])}
        except Exception as e:
            print(f"  ERROR reading metafields for {sku}: {e}")
            errors += 1
            continue

        # Compare current vs new
        current_color = existing.get("stoc_color", {}).get("value", "")
        current_qty   = str(existing.get("stoc_qty", {}).get("value", "-1"))

        if current_color == new_color and current_qty == str(new_qty):
            skipped += 1
            print(f"  SKIP {sku} — {new_color} ({new_qty}) unchanged")
            continue

        # Update changed metafields
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
                    shopify_put(token, f"metafields/{mf_id}.json", {
                        "metafield": {"id": mf_id, "value": value, "type": mf_type}
                    })
                else:
                    shopify_post(token, f"products/{pid}/metafields.json", {
                        "metafield": {
                            "namespace": "custom",
                            "key": key,
                            "value": value,
                            "type": mf_type,
                        }
                    })
                time.sleep(0.2)
            except Exception as e:
                print(f"  ERROR updating {key} for {sku}: {e}")
                ok = False

        if ok:
            updated += 1
            old_info = f"{current_color}({current_qty})"
            new_info = f"{new_color}({new_qty})"
            print(f"  UPDATED {sku}: {old_info} → {new_info} | {new_text}")
        else:
            errors += 1

        time.sleep(0.3)

    print(f"\n=== Done: {updated} updated, {skipped} unchanged, {errors} errors ===")
    if errors > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
