import io
import re
import math
import pandas as pd
from slugify import slugify

SHOPIFY_COLUMNS = [
    "Handle","Command","Title","Body (HTML)","Vendor",
    "Category: ID",
    "Variant Option1 Name","Variant Option1 Value",
    "Variant Option2 Name","Variant Option2 Value",
    "Variant Price","Variant SKU","Variant Barcode",
    "Variant Inventory Qty","Country of Origin","HS Code",
    "Metafield: mm-google-shopping.google_product_category"
]

# ---------- HELP DATA ----------
def _read_2col_sheet(help_bytes, sheet_name):
    try:
        df = pd.read_excel(io.BytesIO(help_bytes), sheet_name=sheet_name, header=None)
        df = df.dropna(how="all")
        if df.shape[1] < 2:
            return None
        df = df.iloc[:, :2]
        df.columns = ["key", "value"]
        df["key"] = df["key"].astype(str).str.strip()
        df["value"] = df["value"].astype(str).str.strip()
        return df
    except Exception:
        return None

def _build_map(help_bytes, sheet_name):
    df = _read_2col_sheet(help_bytes, sheet_name)
    if df is None:
        return {}
    return {k.lower(): v for k, v in zip(df["key"], df["value"])}

def _standardize(val, mapping):
    if pd.isna(val):
        return ""
    s = str(val).strip()
    return mapping.get(s.lower(), s)

# ---------- FALLBACK COLOR / SIZE ----------
def _extract_color_size(description):
    if not description:
        return "", ""
    parts = re.split(r"[-,/]", description)
    parts = [p.strip() for p in parts if p.strip()]
    color, size = "", ""

    if len(parts) >= 2:
        last = parts[-1]
        if re.fullmatch(r"(XS|S|M|L|XL|XXL|XXXL|\d+)", last, re.I):
            size = last
            color = parts[-2]
        else:
            color = last
    return color, size

# ---------- CATEGORY MATCH ----------
def _words(text):
    return set(re.findall(r"[a-z0-9]+", str(text).lower()))

def _match_category(description, df):
    if df is None:
        return ""
    desc_words = _words(description)
    best_id = ""
    best_len = 0

    for _, r in df.iterrows():
        kw_words = _words(r["key"])
        if kw_words and kw_words.issubset(desc_words):
            if len(kw_words) > best_len:
                best_len = len(kw_words)
                best_id = r["value"]
    return best_id

# ---------- PRICE ----------
def _round_9_99(price):
    if pd.isna(price):
        return price
    nearest_10 = math.floor(price / 10 + 0.5) * 10
    return round(nearest_10 - 0.01, 2)

# ---------- MAIN ----------
def run_transform(supplier_xlsx_bytes, help_xlsx_bytes, vendor_name):
    df = pd.read_excel(io.BytesIO(supplier_xlsx_bytes))
    warnings = []

    # Help data maps
    color_map = _build_map(help_xlsx_bytes, "Color Standardization")
    size_map = _build_map(help_xlsx_bytes, "Size Standardization")
    country_map = _build_map(help_xlsx_bytes, "Country Abbreviations")

    google_cat = _read_2col_sheet(help_xlsx_bytes, "Google Product Category")
    shopify_cat = _read_2col_sheet(help_xlsx_bytes, "Shopify Product Category")

    for col in ["Color", "Size"]:
        if col not in df.columns:
            df[col] = ""

    # Fill missing color / size
    for i, r in df.iterrows():
        if not r["Color"] or not r["Size"]:
            c, s = _extract_color_size(r["Description"])
            if not r["Color"]:
                df.at[i, "Color"] = c
            if not r["Size"]:
                df.at[i, "Size"] = s

    df["Color"] = df["Color"].apply(lambda x: _standardize(x, color_map))
    df["Size"] = df["Size"].apply(lambda x: _standardize(x, size_map))
    df["Origin"] = df["Origin"].apply(lambda x: _standardize(x, country_map))

    df["Title"] = (df["Description"] + " " + df["Color"]).str.strip()
    df["Handle"] = df.apply(
        lambda r: slugify(f"{vendor_name} {r['Description']} {r['Color']}"),
        axis=1
    )

    df["Variant Price"] = df["Cad MSRP"].apply(_round_9_99)

    df["Google Cat ID"] = df["Description"].apply(lambda d: _match_category(d, google_cat))
    df["Shopify Cat ID"] = df["Description"].apply(lambda d: _match_category(d, shopify_cat))

    out = pd.DataFrame({
        "Handle": df["Handle"],
        "Command": "MERGE",
        "Title": df["Title"],
        "Body (HTML)": "",
        "Vendor": vendor_name,
        "Category: ID": df["Shopify Cat ID"],
        "Variant Option1 Name": "Size",
        "Variant Option1 Value": df["Size"],
        "Variant Option2 Name": "Color",
        "Variant Option2 Value": df["Color"],
        "Variant Price": df["Variant Price"],
        "Variant SKU": df["Product"],
        "Variant Barcode": df["UPC"],
        "Variant Inventory Qty": 0,
        "Country of Origin": df["Origin"],
        "HS Code": df["HTS Code"],
        "Metafield: mm-google-shopping.google_product_category": df["Google Cat ID"]
    })

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        out.to_excel(writer, index=False, sheet_name="shopify_import")
        pd.DataFrame(warnings).to_excel(writer, index=False, sheet_name="warnings")
    buffer.seek(0)

    return buffer.getvalue(), pd.DataFrame(warnings)
