import io
import re
import math
import pandas as pd
import openpyxl
from slugify import slugify
from openpyxl import load_workbook
from openpyxl.styles import PatternFill

YELLOW_FILL = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")

# ---------------------------------------------------------
# ORDRE FINAL DES COLONNES (strict)
# ---------------------------------------------------------
SHOPIFY_OUTPUT_COLUMNS = [
    "Handle",
    "Command",
    "Title",
    "Body (HTML)",
    "Vendor",
    "Custom Product Type",
    "Tags",
    "Published",
    "Published Scope",
    "Option1 Name",
    "Option1 Value",
    "Variant SKU",
    "Variant Barcode",
    "Variant Country of Origin",
    "Variant HS Code",
    "Variant Grams",
    "Variant Inventory Tracker",
    "Variant Inventory Policy",
    "Variant Fulfillment Service",
    "Variant Price",
    "Variant Requires Shipping",
    "Variant Taxable",
    "SEO Title",
    "SEO Description",
    "Variant Weight Unit",
    "Cost per item",
    "Status",
    "Metafield: my_fields.product_use_case [multi_line_text_field]",
    "Metafield: my_fields.product_features [multi_line_text_field]",
    "Metafield: my_fields.behind_the_brand [multi_line_text_field]",
    "Metafield: my_fields.size_comment [single_line_text_field]",
    "Metafield: my_fields.gender [single_line_text_field]",
    "Metafield: my_fields.colour [single_line_text_field]",
    "Metafield: mm-google-shopping.color",
    "Variant Metafield: mm-google-shopping.size",
    "Metafield: mm-google-shopping.size_system",
    "Metafield: mm-google-shopping.condition",
    "Metafield: mm-google-shopping.google_product_category",
    "Metafield: mm-google-shopping.gender",
    "Variant Metafield: mm-google-shopping.mpn",
    "Variant Metafield: mm-google-shopping.gtin",
    "Metafield: theme.siblings [single_line_text_field]",
    "Category: ID",
    "Inventory Available: Boutique",
    "Inventory Available: Le Club",
]

# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------
def _norm(s) -> str:
    return re.sub(r"\s+", " ", str(s or "").strip())


def _strip_reg_for_handle(s: str) -> str:
    """Handle only: remove ® and (r)/[r] to keep URL safe."""
    t = _norm(s)
    t = t.replace("®", "")
    t = re.sub(r"[\(\[\{]\s*r\s*[\)\]\}]", "", t, flags=re.IGNORECASE)
    return _norm(t)


def _convert_r_to_registered(s: str) -> str:
    """Display/SEO: convert (r)/[r] to ®."""
    t = _norm(s)
    t = re.sub(r"[\(\[\{]\s*r\s*[\)\]\}]", "®", t, flags=re.IGNORECASE)
    return t


def _words(s: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", str(s).lower())


def _singularize_token(tok: str) -> str:
    # basic: hats -> hat (only for longer tokens)
    if tok.endswith("s") and len(tok) >= 4:
        return tok[:-1]
    return tok


def _wordset_loose(s: str) -> set[str]:
    return set(_singularize_token(t) for t in _words(s))


def _first_existing_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in cols:
            return cols[c.lower()]
    return None


# ---------------------------------------------------------
# Help data readers (openpyxl)
# ---------------------------------------------------------
def _load_help_wb(help_bytes: bytes):
    return openpyxl.load_workbook(io.BytesIO(help_bytes), data_only=True)


def _read_2col_map(wb, sheet_candidates: list[str]) -> dict[str, str]:
    """Col A raw -> Col B standard"""
    sheet = None
    for name in sheet_candidates:
        if name in wb.sheetnames:
            sheet = wb[name]
            break
    if sheet is None:
        return {}

    m: dict[str, str] = {}
    for r in range(2, sheet.max_row + 1):
        a = sheet.cell(row=r, column=1).value
        b = sheet.cell(row=r, column=2).value
        if a is None or b is None:
            continue
        ra = str(a).strip()
        rb = str(b).strip()
        if not ra or ra.lower() == "nan":
            continue
        m[ra.lower()] = rb
    return m


def _standardize(val: str, mapping: dict[str, str]) -> str:
    s = _norm(val)
    if not s or s.lower() == "nan":
        return ""
    return mapping.get(s.lower(), s)


def _read_list_column(wb, sheet_name: str) -> list[str]:
    if sheet_name not in wb.sheetnames:
        return []
    ws = wb[sheet_name]
    out = []
    for r in range(2, ws.max_row + 1):
        v = ws.cell(row=r, column=1).value
        if v is None:
            continue
        s = str(v).strip()
        if s and s.lower() != "nan":
            out.append(s)
    return out


def _read_category_rows(wb, sheet_name: str):
    """returns list[(name_keywords, id)] from columns A,B"""
    if sheet_name not in wb.sheetnames:
        return None
    ws = wb[sheet_name]
    rows = []
    for r in range(2, ws.max_row + 1):
        a = ws.cell(row=r, column=1).value
        b = ws.cell(row=r, column=2).value
        if a is None:
            continue
        aa = str(a).strip()
        bb = "" if b is None else str(b).strip()
        if aa and aa.lower() != "nan":
            rows.append((aa, bb))
    return rows or None


def _read_brand_line_map(wb, sheet_name: str) -> dict[str, str]:
    """
    Col A = brand
    Col B+ = text parts (concatenated)
    """
    if sheet_name not in wb.sheetnames:
        return {}
    ws = wb[sheet_name]
    m = {}
    for r in range(2, ws.max_row + 1):
        brand = ws.cell(row=r, column=1).value
        if brand is None:
            continue
        b = str(brand).strip()
        if not b or b.lower() == "nan":
            continue

        parts = []
        for c in range(2, ws.max_column + 1):
            v = ws.cell(row=r, column=c).value
            if v is None:
                continue
            s = str(v).strip()
            if s and s.lower() != "nan":
                parts.append(s)

        if parts:
            m[b.lower()] = " ".join(parts).strip()
    return m


def _read_size_reco_map(wb) -> dict[str, str]:
    """Garment -> Comment"""
    if "Size Recommandation" not in wb.sheetnames:
        return {}
    ws = wb["Size Recommandation"]

    headers = {}
    for c in range(1, ws.max_column + 1):
        h = ws.cell(row=1, column=c).value
        if h is None:
            continue
        headers[str(h).strip().lower()] = c

    gcol = headers.get("garment")
    ccol = headers.get("comment")
    if not gcol or not ccol:
        return {}

    m = {}
    for r in range(2, ws.max_row + 1):
        g = ws.cell(row=r, column=gcol).value
        c = ws.cell(row=r, column=ccol).value
        if g is None or c is None:
            continue
        gs = str(g).strip()
        cs = str(c).strip()
        if gs and cs:
            m[gs.lower()] = cs
    return m


# ---------------------------------------------------------
# Matching functions
# ---------------------------------------------------------
def _best_match_id(text: str, cat_rows) -> str:
    """
    Exact-match (loose singular/plural): all words in name must be in text.
    Returns ID (col B).
    """
    if not cat_rows:
        return ""
    tset = _wordset_loose(text)
    best_id = ""
    best_len = 0
    for name, cid in cat_rows:
        nset = _wordset_loose(name)
        if nset and nset.issubset(tset):
            if len(nset) > best_len:
                best_len = len(nset)
                best_id = str(cid or "").strip()
    best_id = re.sub(r"\.0$", "", best_id) if best_id else ""
    return best_id


def _best_match_product_type(text: str, product_types: list[str]) -> str:
    tset = _wordset_loose(text)
    best = ""
    best_len = 0
    for pt in product_types:
        pset = _wordset_loose(pt)
        if pset and pset.issubset(tset):
            if len(pset) > best_len:
                best_len = len(pset)
                best = pt
    return best


# ---------------------------------------------------------
# Parsing & formatting
# ---------------------------------------------------------
def _extract_color_size_from_description(desc: str) -> tuple[str, str]:
    text = _norm(desc)
    if not text:
        return "", ""
    parts = re.split(r"\s*[-,/]\s*|\s*,\s*", text)
    parts = [p.strip() for p in parts if p and p.strip()]
    if len(parts) < 2:
        return "", ""
    last = parts[-1]
    if re.fullmatch(r"(X{0,3}S|X{0,3}L|S|M|L|XL|XXL|XXXL|\d{1,2}([./-]\d{1,2})?)", last, flags=re.IGNORECASE):
        return parts[-2], last
    return parts[-1], ""


def _round_to_nearest_9_99(price) -> float:
    if price is None or (isinstance(price, float) and math.isnan(price)):
        return float("nan")
    p = float(price)
    nearest10 = math.floor(p / 10.0 + 0.5) * 10.0
    return round(nearest10 - 0.01, 2)


def _barcode_keep_zeros(x) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if s == "" or s.lower() == "nan":
        return ""
    if re.fullmatch(r"\d+\.0", s):
        s = s[:-2]
    if re.fullmatch(r"\d+", s):
        return s.zfill(12) if len(s) <= 12 else s
    return s


def _hs_code_clean(x) -> str:
    """Do not append zeros; only clean Excel .0 artifact."""
    if x is None:
        return ""
    s = str(x).strip()
    if s == "" or s.lower() == "nan":
        return ""
    return re.sub(r"\.0$", "", s)


def _title_case_preserve_registered(text: str) -> str:
    """Title Case while preserving ®."""
    text = _norm(text)
    if not text:
        return ""
    parts = text.split(" ")
    out = []
    for w in parts:
        if "®" in w:
            sub = w.split("®")
            sub = [p[:1].upper() + p[1:].lower() if p else "" for p in sub]
            out.append("®".join(sub))
            continue
        if w.isupper() and len(w) <= 4:
            out.append(w)
            continue
        if any(ch.isdigit() for ch in w):
            out.append(w)
            continue
        out.append(w[:1].upper() + w[1:].lower() if w else w)
    return " ".join(out)


# ---------------------------------------------------------
# Excel highlighting
# ---------------------------------------------------------
def _apply_yellow_for_empty(buffer: io.BytesIO, sheet_name: str, cols_to_yellow: list[str]) -> io.BytesIO:
    buffer.seek(0)
    wb = load_workbook(buffer)
    ws = wb[sheet_name]

    header = [cell.value for cell in ws[1]]
    col_index = {name: i + 1 for i, name in enumerate(header) if name}

    for col_name in cols_to_yellow:
        if col_name not in col_index:
            continue
        c = col_index[col_name]
        for r in range(2, ws.max_row + 1):
            cell = ws.cell(row=r, column=c)
            val = cell.value
            if val is None or (isinstance(val, str) and val.strip() == ""):
                cell.fill = YELLOW_FILL

    out = io.BytesIO()
    wb.save(out)
    out.seek(0)
    return out


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
def run_transform(
    supplier_xlsx_bytes: bytes,
    help_xlsx_bytes: bytes,
    vendor_name: str,
    brand_choice: str = "",
):
    sup = pd.read_excel(io.BytesIO(supplier_xlsx_bytes), sheet_name=0, dtype=str).copy()
    warnings: list[dict] = []

    wb = _load_help_wb(help_xlsx_bytes)

    # Standardization
    color_map = _read_2col_map(wb, ["Color Standardization", "Color Variable"])
    size_map = _read_2col_map(wb, ["Size Standardization", "Size Variante"])
    country_map = _read_2col_map(wb, ["Country Abbreviations", "Country of Origin"])
    gender_map = _read_2col_map(wb, ["Gender Standardization", "Gender"])

    # Categories & Product types
    shopify_cat_rows = _read_category_rows(wb, "Shopify Product Category")
    google_cat_rows = _read_category_rows(wb, "Google Product Category")
    product_types = _read_list_column(wb, "Product Types")

    # Brand maps
    brand_desc_map = _read_brand_line_map(wb, "SEO Description Brand Part")
    brand_lines_map = _read_brand_line_map(wb, "Brand lines")

    # Size reco
    size_comment_map = _read_size_reco_map(wb)

    # -------------------------
    # Supplier columns (UPDATED)
    # -------------------------
    # (1) add "Display Name" as a candidate for description
    desc_col = _first_existing_col(
        sup,
        ["description", "Display Name", "product name", "Title", "Product Name", "title"],
    )

    product_col = _first_existing_col(sup, ["Product", "Product Code", "SKU", "sku"])

    # (4) add "Vendor Color" as a candidate
    color_col = _first_existing_col(sup, ["Vendor Color", "Color", "Colour", "vendor color", "color", "colour"])

    # (2)(5) add "Vendor Size1" as a candidate
    size_col = _first_existing_col(sup, ["Vendor Size1", "Size", "vendor size1", "size"])

    upc_col = _first_existing_col(sup, ["UPC", "UPC Code", "upc", "upc code"])
    origin_col = _first_existing_col(sup, ["Country Code", "Origin", "Manufacturing Country"])
    hs_col = _first_existing_col(sup, ["HS Code", "HTS Code"])
    extid_col = _first_existing_col(sup, ["External ID", "ExternalID"])
    msrp_col = _first_existing_col(sup, ["Cad MSRP", "MSRP"])
    landed_col = _first_existing_col(sup, ["Landed"])
    grams_col = _first_existing_col(sup, ["Grams", "Weight (g)", "Weight"])
    gender_col = _first_existing_col(sup, ["Gender"])

    if desc_col is None:
        raise ValueError(
            'Aucune colonne Description trouvée. Colonnes acceptées: "description", "Display Name", "product name", "Title".'
        )
    if msrp_col is None:
        raise ValueError('Colonne "Cad MSRP" / "MSRP" introuvable dans le fichier fournisseur.')

    # Base description
    sup["_desc_raw"] = sup[desc_col].astype(str).fillna("").map(_norm)
    sup["_desc_seo"] = sup["_desc_raw"].apply(_convert_r_to_registered)
    sup["_desc_handle"] = sup["_desc_raw"].apply(_strip_reg_for_handle)

    # Color / Size input (prefer vendor columns)
    sup["_color_raw"] = sup[color_col].astype(str).fillna("").map(_norm) if color_col else ""
    sup["_size_raw"] = sup[size_col].astype(str).fillna("").map(_norm) if size_col else ""

    # Fallback parse from description if missing
    parsed = sup["_desc_raw"].apply(_extract_color_size_from_description)
    sup["_color_fb"] = parsed.map(lambda t: t[0])
    sup["_size_fb"] = parsed.map(lambda t: t[1])

    sup["_color_in"] = sup["_color_raw"]
    sup.loc[sup["_color_in"].eq(""), "_color_in"] = sup["_color_fb"]

    sup["_size_in"] = sup["_size_raw"]
    sup.loc[sup["_size_in"].eq(""), "_size_in"] = sup["_size_fb"]

    # Standardize -> MUST output col B (your mapping)
    sup["_color_std"] = sup["_color_in"].apply(lambda x: _standardize(x, color_map))
    sup["_size_std"] = sup["_size_in"].apply(lambda x: _standardize(x, size_map))

    # Gender (standardize if possible)
    sup["_gender_raw"] = sup[gender_col].astype(str).fillna("").map(_norm) if gender_col else ""
    sup["_gender_std"] = sup["_gender_raw"].apply(lambda x: _standardize(x, gender_map)) if gender_map else sup["_gender_raw"]

    # Vendor / Brand
    sup["_vendor"] = vendor_name
    sup["_brand_choice"] = _norm(brand_choice)

    # Title (for Shopify): Description + Color (standardized)
    sup["_title"] = (sup["_desc_raw"] + " " + sup["_color_std"]).str.strip()

    # -------------------------
    # (1) Handle rule (UPDATED)
    # Vendor + Gender + Description + Color, hyphens (slugify)
    # -------------------------
    def _make_handle(r):
        parts = [
            _strip_reg_for_handle(r["_vendor"]),
            _strip_reg_for_handle(r["_gender_std"]),
            r["_desc_handle"],
            _strip_reg_for_handle(r["_color_std"]),
        ]
        parts = [p for p in parts if p and str(p).strip()]
        return slugify(" ".join(parts))

    sup["_handle"] = sup.apply(_make_handle, axis=1)

    # Custom Product Type (loose singular/plural)
    sup["_product_type"] = sup["_title"].apply(lambda t: _best_match_product_type(t, product_types))

    # Tags
    def _make_tags(r):
        tags = []
        if r["_vendor"]:
            tags.append(r["_vendor"])
        if r["_color_std"]:
            tags.append(r["_color_std"])
        if r["_gender_std"]:
            tags.append(r["_gender_std"])
        tags.append("_badge_new")
        if r["_product_type"]:
            tags.append(r["_product_type"])

        g = str(r["_gender_std"]).lower()
        if "men" in g and "women" in g:
            tags.extend(["Men", "Women"])
        return ", ".join([t for t in tags if t])

    sup["_tags"] = sup.apply(_make_tags, axis=1)

    # SKU: external id else product code else fallback
    sup["_external_id"] = sup[extid_col].astype(str).fillna("").map(_norm) if extid_col else ""
    sup["_product_code"] = sup[product_col].astype(str).fillna("").map(_norm) if product_col else ""

    def _make_sku(r):
        if r["_external_id"]:
            return r["_external_id"]
        if r["_product_code"]:
            return r["_product_code"]
        base = r["_product_code"] or "SKU"
        return f"{base}-{r['_size_std']}-{r['_color_std']}".strip("-")

    sup["_variant_sku"] = sup.apply(_make_sku, axis=1)

    # Barcode
    sup["_barcode"] = sup[upc_col].apply(_barcode_keep_zeros) if upc_col else ""

    # Country (standardize)
    sup["_origin_raw"] = sup[origin_col].astype(str).fillna("").map(_norm) if origin_col else ""
    sup["_origin_std"] = sup["_origin_raw"].apply(lambda x: _standardize(x, country_map))

    # HS Code (3)
    sup["_hs"] = sup[hs_col].apply(_hs_code_clean) if hs_col else ""

    # Grams (2/5 - keep)
    sup["_grams"] = sup[grams_col].astype(str).fillna("").map(_norm) if grams_col else ""

    # Price
    msrp_num = pd.to_numeric(
        sup[msrp_col].astype(str).str.replace("$", "", regex=False).str.replace(",", "", regex=False),
        errors="coerce",
    )
    sup["_price"] = msrp_num.apply(_round_to_nearest_9_99)

    # Cost
    sup["_cost"] = sup[landed_col].astype(str).fillna("").map(_norm) if landed_col else ""

    # Size comment: prefer brand else vendor
    def _size_comment(r):
        key = (r["_brand_choice"] or r["_vendor"]).strip().lower()
        return size_comment_map.get(key, "")

    sup["_size_comment"] = sup.apply(_size_comment, axis=1)

    # Categories
    sup["_shopify_cat_id"] = sup["_title"].apply(lambda t: _best_match_id(t, shopify_cat_rows))
    sup["_google_cat_id"] = sup["_title"].apply(lambda t: _best_match_id(t, google_cat_rows))

    # Siblings
    sup["_siblings"] = sup.apply(lambda r: slugify(f"{r['_vendor']} {r['_desc_handle']}"), axis=1)

    # SEO Title (same rules you had)
    def _seo_title(r):
        main = f"{r['_vendor']} {r['_gender_std']} {r['_desc_seo']}".strip()
        main = _title_case_preserve_registered(main)
        color = _title_case_preserve_registered(r["_color_std"])
        return f"{main} - {color}".strip() if color else main

    sup["_seo_title"] = sup.apply(_seo_title, axis=1)

    # (3) SEO Description rule (UPDATED)
    # Prefix stays identical; we ONLY replace the "[products.]" part if brand exists in SEO Description Brand Part.
    # Vendor name must be present at the end.
    def _seo_desc(r):
        prefix = (
            f"Shop the {r['_seo_title']} with free worldwide shipping, and 30-day returns on leclub.cc. Discover "
        )

        bkey = (r["_brand_choice"] or "").strip().lower()
        if bkey and bkey in brand_desc_map:
            middle = brand_desc_map[bkey].strip()
        else:
            middle = "products."

        # Ensure vendor at end
        vend = r["_vendor"].strip()
        # avoid double punctuation
        middle = middle.rstrip()
        if not middle.endswith("."):
            middle = middle + "."
        return f"{prefix}{middle} {vend}."

    sup["_seo_desc"] = sup.apply(_seo_desc, axis=1)

    # (8) behind the brand
    def _behind_brand(r):
        bkey = (r["_brand_choice"] or "").strip().lower()
        return brand_lines_map.get(bkey, "") if bkey else ""

    sup["_behind_the_brand"] = sup.apply(_behind_brand, axis=1)

    # ---------------------------------------------------------
    # Build output (strict order)
    # ---------------------------------------------------------
    out = pd.DataFrame(columns=SHOPIFY_OUTPUT_COLUMNS)

    out["Handle"] = sup["_handle"]
    out["Command"] = "NEW"
    out["Title"] = sup["_title"]
    out["Body (HTML)"] = ""
    out["Vendor"] = sup["_vendor"]
    out["Custom Product Type"] = sup["_product_type"]
    out["Tags"] = sup["_tags"]

    out["Published"] = False
    out["Published Scope"] = "global"

    out["Option1 Name"] = "Size"
    out["Option1 Value"] = sup["_size_std"]  # (2)(5)

    out["Variant SKU"] = sup["_variant_sku"]
    out["Variant Barcode"] = sup["_barcode"]
    out["Variant Country of Origin"] = sup["_origin_std"]
    out["Variant HS Code"] = sup["_hs"]
    out["Variant Grams"] = sup["_grams"]

    out["Variant Inventory Tracker"] = "shopify"
    out["Variant Inventory Policy"] = "deny"
    out["Variant Fulfillment Service"] = "manual"
    out["Variant Price"] = sup["_price"]

    out["Variant Requires Shipping"] = True
    out["Variant Taxable"] = True

    out["SEO Title"] = sup["_seo_title"]
    out["SEO Description"] = sup["_seo_desc"]

    out["Variant Weight Unit"] = "g"
    out["Cost per item"] = sup["_cost"]
    out["Status"] = "draft"

    out["Metafield: my_fields.product_use_case [multi_line_text_field]"] = ""
    out["Metafield: my_fields.product_features [multi_line_text_field]"] = ""
    out["Metafield: my_fields.behind_the_brand [multi_line_text_field]"] = sup["_behind_the_brand"]
    out["Metafield: my_fields.size_comment [single_line_text_field]"] = sup["_size_comment"]
    out["Metafield: my_fields.gender [single_line_text_field]"] = sup["_gender_std"]

    # (4) color metafields must be standardized
    out["Metafield: my_fields.colour [single_line_text_field]"] = sup["_color_std"]
    out["Metafield: mm-google-shopping.color"] = sup["_color_std"]

    # (5) size metafield must be standardized
    out["Variant Metafield: mm-google-shopping.size"] = sup["_size_std"]

    out["Metafield: mm-google-shopping.size_system"] = "US"
    out["Metafield: mm-google-shopping.condition"] = "new"
    out["Metafield: mm-google-shopping.google_product_category"] = sup["_google_cat_id"]
    out["Metafield: mm-google-shopping.gender"] = sup["_gender_std"]

    out["Variant Metafield: mm-google-shopping.mpn"] = sup["_variant_sku"]
    out["Variant Metafield: mm-google-shopping.gtin"] = sup["_barcode"]

    out["Metafield: theme.siblings [single_line_text_field]"] = sup["_siblings"]

    out["Category: ID"] = sup["_shopify_cat_id"]

    out["Inventory Available: Boutique"] = 0
    out["Inventory Available: Le Club"] = 0

    out = out.reindex(columns=SHOPIFY_OUTPUT_COLUMNS)

    # Yellow rules (kept from your spec)
    yellow_if_empty_cols = [
        "Handle",
        "Title",
        "Body (HTML)",
        "Custom Product Type",
        "Option1 Name",
        "Option1 Value",
        "Variant Price",
        "Variant Grams",
        "SEO Title",
        "SEO Description",
        "Metafield: my_fields.size_comment [single_line_text_field]",
        "Metafield: my_fields.gender [single_line_text_field]",
        "Metafield: my_fields.colour [single_line_text_field]",
        "Metafield: mm-google-shopping.color",
        "Variant Metafield: mm-google-shopping.size",
        "Metafield: mm-google-shopping.google_product_category",
        "Category: ID",
    ]

    # Export
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        out.to_excel(writer, index=False, sheet_name="shopify_import")
        pd.DataFrame(warnings).to_excel(writer, index=False, sheet_name="warnings")

    buffer = _apply_yellow_for_empty(buffer, "shopify_import", yellow_if_empty_cols)
    return buffer.getvalue(), pd.DataFrame(warnings)
