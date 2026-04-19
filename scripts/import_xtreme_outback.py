#!/usr/bin/env python3
"""
Import Xtreme Outback clutch kits (Nissan, EU stock only) to Shopify.
Collection: Kit de ambreiaj (ID: 667266351445)
Price: Base Price (EUR) × 5 (RON) × 1.21 (TVA), ceil to nearest 10 RON
"""

import urllib.request
import urllib.parse
import urllib.error
import json
import csv
import io
import math
import time
import os
import sys

SHOP          = os.environ.get("SHOPIFY_SHOP", "k21m1k-k0.myshopify.com")
CLIENT_ID     = os.environ["SHOPIFY_CLIENT_ID"]
CLIENT_SECRET = os.environ["SHOPIFY_CLIENT_SECRET"]

COLLECTION_ID = 667266351445  # Kit de ambreiaj

STOCKLIST_URL  = "https://3cerp.eu/api/stocklist/?p=5e1eb87486eb633860334f5e&u=69c9076cca5cb16255ddf9ac&f=json"
APPS_URL       = "https://3cerp.eu/api/applications/?p=5e1eb87486eb633860334f5e&u=69c9076cca5cb16255ddf9ac&f=json"
DATAPACK_URL   = "https://ozparts2.usermd.net/API%20-%20ACS%20data%20pack.json"
PRICELIST_PATH = os.path.join(os.path.dirname(__file__), "..", "Downloads", "pricelist-4.csv")

# Fallback: pricelist next to script
if not os.path.exists(PRICELIST_PATH):
    PRICELIST_PATH = os.path.expanduser("~/Downloads/pricelist-4.csv")


# ─── Shopify helpers ──────────────────────────────────────────────────────────

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
                wait = 10 * (2 ** attempt)
                print(f"  429 rate limit, retry in {wait}s...")
                time.sleep(wait)
            else:
                raise


def shopify_post(token, path, payload):
    for attempt in range(4):
        body = json.dumps(payload).encode()
        req = urllib.request.Request(
            f"https://{SHOP}/admin/api/2024-01/{path}",
            data=body,
            headers={"X-Shopify-Access-Token": token, "Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < 3:
                wait = 10 * (2 ** attempt)
                print(f"  429 rate limit, retry in {wait}s...")
                time.sleep(wait)
            else:
                body_text = e.read().decode()
                print(f"  HTTP {e.code}: {body_text[:300]}")
                raise


def shopify_put(token, path, payload):
    for attempt in range(4):
        body = json.dumps(payload).encode()
        req = urllib.request.Request(
            f"https://{SHOP}/admin/api/2024-01/{path}",
            data=body,
            headers={"X-Shopify-Access-Token": token, "Content-Type": "application/json"},
            method="PUT",
        )
        try:
            with urllib.request.urlopen(req) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < 3:
                wait = 10 * (2 ** attempt)
                print(f"  429 rate limit, retry in {wait}s...")
                time.sleep(wait)
            else:
                raise


# ─── Price helpers ─────────────────────────────────────────────────────────────

def calc_price(base_eur: float) -> str:
    """Base EUR × 5 RON × 1.21 TVA, ceil to nearest 10."""
    raw = base_eur * 5 * 1.21
    rounded = math.ceil(raw / 10) * 10
    return f"{rounded:.2f}"


# ─── Stock helpers ─────────────────────────────────────────────────────────────

def get_stock_status(stock_item):
    if not stock_item:
        return "Livrare specială", "Termen la cerere", "red", 0
    available = (stock_item.get("available") or 0) + (stock_item.get("available2") or 0)
    mfr      = stock_item.get("manufacturerstock") or 0
    category = stock_item.get("stockcategory", "")
    if available > 0:
        return f"Pe stoc european ({available} buc.)", "Livrabil în 2-5 zile", "green", available
    elif mfr > 0:
        return "Disponibil la comandă", "Contactați-ne pentru informații despre livrare", "orange", 0
    elif category == "stockeditem":
        return "Disponibil la comandă", "Contactați-ne pentru informații despre livrare", "orange", 0
    else:
        return "Livrare specială", "Termen la cerere", "red", 0


# ─── Description builder ───────────────────────────────────────────────────────

def build_description(dp_entry, applications):
    """Build full HTML description."""
    parts = []

    # 1. Brand intro
    detail = (dp_entry.get("Detail Description") or "").strip()
    if detail:
        parts.append(detail)
    else:
        parts.append(
            "<p>Xtreme Outback este linia de kituri de ambreiaj heavy duty "
            "destinată vehiculelor 4x4 și off-road, produsă de ACS (Australian Clutch Services). "
            "Concepute pentru condiții extreme, oferă durabilitate superioară față de kiturile OEM.</p>"
        )

    # 2. Specs table
    others = (dp_entry.get("Others") or "").strip()
    if others:
        rows = [line.strip().rstrip("|").strip() for line in others.split("|") if line.strip().rstrip("|").strip()]
        spec_rows = []
        for row in rows:
            if ":" in row:
                key, val = row.split(":", 1)
                spec_rows.append(f"<tr><td><strong>{key.strip()}</strong></td><td>{val.strip()}</td></tr>")
        if spec_rows:
            parts.append(
                "<h3>Specificații tehnice</h3>"
                "<table><tbody>"
                + "".join(spec_rows)
                + "</tbody></table>"
            )

    # 3. Weight
    weight = (dp_entry.get("Weight") or "").strip()
    if weight:
        parts.append(f"<p><strong>Greutate:</strong> {weight} kg</p>")

    # 4. Applications table
    if applications:
        parts.append("<h3>Aplicații (vehicule compatibile)</h3>")
        parts.append(
            "<table>"
            "<thead><tr>"
            "<th>Marcă</th><th>Model</th><th>Variantă</th>"
            "<th>An (de)</th><th>An (până)</th><th>Motor</th>"
            "</tr></thead><tbody>"
        )
        for app in applications:
            make    = app.get("make", "")
            model   = app.get("model", "")
            variant = app.get("variant", "—")
            yfrom   = app.get("year_from", "")
            yto     = app.get("year_to", "")
            engine  = app.get("engine_raw", "")
            parts.append(
                f"<tr><td>{make}</td><td>{model}</td><td>{variant}</td>"
                f"<td>{yfrom}</td><td>{yto}</td><td>{engine}</td></tr>"
            )
        parts.append("</tbody></table>")

    return "\n".join(parts)


# ─── Tag builder ──────────────────────────────────────────────────────────────

def build_tags(sku, applications):
    tags = {"Marca_Nissan", "vendor_Xtreme Outback"}
    models_seen = set()
    for app in applications:
        model = app.get("model", "").strip()
        if model and model not in models_seen:
            models_seen.add(model)
            tag = f"Model_{model}"
            tags.add(tag)
    # Suffix-based type tag
    if "-1C" in sku or "-1CX" in sku:
        tags.add("Tip_Ceramic")
    elif "-1AX" in sku or "-1BX" in sku or "-1CX" in sku:
        tags.add("Tip_Extra Heavy Duty")
    elif "-1B" in sku:
        tags.add("Tip_Sprung Ceramic")
    else:
        tags.add("Tip_Heavy Duty Organic")
    return list(tags)


# ─── Fetch helpers ────────────────────────────────────────────────────────────

def fetch_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": "RacingZone-Import/1.0"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read())


def fetch_datapack():
    print("Fetching data pack...")
    items = fetch_json(DATAPACK_URL)
    return {item["Item"]: item for item in items}


def fetch_stocklist():
    print("Fetching stocklist...")
    items = fetch_json(STOCKLIST_URL)
    return {item["name"]: item for item in items}


def fetch_applications():
    print("Fetching applications...")
    items = fetch_json(APPS_URL)
    # Parse into dict: SKU -> list of app dicts
    result = {}
    for item in items:
        # Field names vary — normalize
        sku     = (item.get("partno") or item.get("partnumber") or item.get("part_no")
                   or item.get("sku") or item.get("Item") or "").strip()
        make    = (item.get("make") or "").strip()
        model   = (item.get("model") or "").strip()
        variant = (item.get("variant") or item.get("Variant") or "—").strip()
        yfrom   = (item.get("year_from") or item.get("yearfrom") or "").strip()
        yto     = (item.get("year_to")   or item.get("yearto")   or "").strip()
        # Engine: try several field names
        engine  = (item.get("engine_name") or item.get("engine") or "").strip()
        disp    = str(item.get("displacement") or item.get("cc") or "").strip()
        fuel    = (item.get("fuel_type") or item.get("fuel") or "").strip()
        kw      = str(item.get("power_kw") or item.get("kw") or "").strip()
        bhp     = str(item.get("power_bhp") or item.get("bhp") or "").strip()
        # Build engine raw string
        parts = []
        if engine: parts.append(engine)
        if disp:   parts.append(f"{disp}cc")
        if fuel:   parts.append(fuel)
        if kw:     parts.append(f"{kw}kW")
        if bhp:    parts.append(f"{bhp}BHP")
        engine_raw = " ".join(parts) if parts else (item.get("engine_raw") or "")

        if not sku:
            continue
        if sku not in result:
            result[sku] = []
        result[sku].append({
            "make": make, "model": model, "variant": variant,
            "year_from": yfrom, "year_to": yto, "engine_raw": engine_raw,
        })
    return result


def load_pricelist():
    print("Loading pricelist...")
    prices = {}
    with open(PRICELIST_PATH, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sku   = row["Item"].strip()
            price = row["Base Price"].strip().replace("€", "").replace(",", "")
            try:
                prices[sku] = float(price)
            except ValueError:
                pass
    return prices


# ─── Shopify product exists check ─────────────────────────────────────────────

def get_existing_skus(token):
    """Return set of SKUs already in Shopify for vendor Xtreme Outback."""
    existing = set()
    resp = shopify_get(token, "products.json?vendor=Xtreme%20Outback&limit=250&fields=id,variants")
    for p in resp.get("products", []):
        for v in p.get("variants", []):
            if v.get("sku"):
                existing.add(v["sku"])
    return existing


# ─── Main import ──────────────────────────────────────────────────────────────

# Target SKUs (EU stock, Nissan brand)
TARGET_SKUS = {
    'KNI28014-1A', 'KNI24008-1CX', 'KNI25509-1A', 'KNI28509-1A',
    'KNI28001-1AX', 'KNI28001-1A', 'KNI24018-1A', 'KNI24009-1A',
    'KNI24008-1A', 'KNI28006-1C', 'KNI25009-1A', 'KNI24530-1AX',
    'KNI24030-1A', 'KNI24008-1AX', 'KNI28009-1A', 'KNI24004-1B',
    'KNI28098-1AX', 'KNI23645-1A', 'KNI24044-1A',
    'FNI108DI', 'CNI25121HD', 'DNI2424209',
}


def main():
    print("=== Import Xtreme Outback — Nissan (EU stock) ===\n")

    # 1. Load all data sources
    token      = get_token()
    print("Shopify token OK")
    datapack   = fetch_datapack()
    stocklist  = fetch_stocklist()
    apps       = fetch_applications()
    prices     = load_pricelist()

    # 2. Check existing products to avoid duplicates
    existing = get_existing_skus(token)
    print(f"Already in Shopify (Xtreme Outback): {len(existing)} SKUs")

    created = 0
    skipped = 0
    errors  = 0

    for sku in sorted(TARGET_SKUS):
        if sku in existing:
            print(f"  SKIP {sku} — already exists")
            skipped += 1
            continue

        # --- Price
        base_eur = prices.get(sku)
        if not base_eur:
            print(f"  WARN {sku} — no price in pricelist, skipping")
            skipped += 1
            continue

        price_ron = calc_price(base_eur)

        # --- Data pack entry
        dp = datapack.get(sku, {})
        short_desc = dp.get("Description") or f"Kit ambreiaj Xtreme Outback {sku}"
        barcode    = dp.get("Barcode") or ""
        weight     = dp.get("Weight") or ""
        img_url    = dp.get("Pictures") or ""

        # Skip placeholder images
        if "No_Image" in img_url or "no_image" in img_url.lower():
            img_url = ""

        # --- Applications for this SKU
        sku_apps = apps.get(sku, [])

        # --- Build title
        title = f"Kit ambreiaj Xtreme Outback {short_desc.replace('Clutch Kit - ', '').replace('Clutch Kit - Xtreme Outback', '').strip()} — {sku}"
        # Clean up title
        title = title.replace("  ", " ").strip(" —").strip()
        if not title or title == sku:
            title = f"Kit ambreiaj Xtreme Outback — {sku}"

        # --- Build description
        description = build_description(dp, sku_apps)

        # --- Tags
        tags = build_tags(sku, sku_apps)

        # --- Stock status
        stock_item = stocklist.get(sku)
        stoc_text, stoc_livrare, stoc_color, stoc_qty = get_stock_status(stock_item)

        # --- Weight
        weight_grams = None
        if weight:
            try:
                weight_grams = int(float(weight) * 1000)
            except ValueError:
                pass

        # --- Build Shopify product payload
        variant_payload = {
            "price": price_ron,
            "sku": sku,
            "inventory_management": None,
            "inventory_policy": "continue",
            "requires_shipping": True,
            "taxable": True,
            "barcode": barcode,
        }
        if weight_grams:
            variant_payload["weight"] = weight_grams
            variant_payload["weight_unit"] = "g"

        product_payload = {
            "product": {
                "title": title,
                "body_html": description,
                "vendor": "Xtreme Outback",
                "product_type": "Kit ambreiaj",
                "tags": ", ".join(tags),
                "status": "active",
                "variants": [variant_payload],
            }
        }

        # Add image if available
        if img_url:
            product_payload["product"]["images"] = [{"src": img_url, "alt": title}]

        try:
            print(f"  Creating {sku}  {price_ron} RON  ({stoc_color}, {stoc_qty} buc)...")
            resp = shopify_post(token, "products.json", product_payload)
            product = resp.get("product", {})
            pid = product.get("id")
            if not pid:
                print(f"  ERROR creating {sku}: no product ID returned")
                errors += 1
                continue

            # Add to collection
            shopify_post(token, "collects.json", {
                "collect": {"product_id": pid, "collection_id": COLLECTION_ID}
            })
            time.sleep(0.3)

            # Add metafields
            metafields = [
                ("stoc_text",    stoc_text,         "single_line_text_field"),
                ("stoc_livrare", stoc_livrare,       "single_line_text_field"),
                ("stoc_color",   stoc_color,         "single_line_text_field"),
                ("stoc_qty",     str(stoc_qty),      "number_integer"),
            ]
            for key, value, mf_type in metafields:
                shopify_post(token, f"products/{pid}/metafields.json", {
                    "metafield": {
                        "namespace": "custom",
                        "key": key,
                        "value": value,
                        "type": mf_type,
                    }
                })
                time.sleep(0.15)

            print(f"  ✓ {sku} → product {pid}")
            created += 1

        except Exception as e:
            print(f"  ERROR {sku}: {e}")
            errors += 1

        time.sleep(0.5)

    print(f"\n=== Done: {created} created, {skipped} skipped, {errors} errors ===")


if __name__ == "__main__":
    main()
