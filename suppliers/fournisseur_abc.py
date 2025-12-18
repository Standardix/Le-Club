import io
import re
import math
import pandas as pd
from slugify import slugify

SHOPIFY_COLUMNS = [
    "Handle",
    "Command",
    "Title",
    "Body (HTML)",
    "Vendor",
    "Category: ID",
    "Variant Option1 Name",
    "Variant Option1 Value",
    "Variant Option2 Name",
    "Variant Option2 Value",
    "Variant Price",
    "Variant SKU",
    "Variant Barcode",
    "Variant Inventory Qty",
    "Country of Origin",
    "HS Code",
    "Metafield: mm-google-shopping.google_product_category",
]

# ---------- HELP DATA ----------
def _read_2col_sheet(help_bytes: bytes, sheet_name: str) -> pd.DataFrame | None:
    """
    Lit 2 colonnes:
      Col A = keyword exact (pour matching)
      Col B = valeur à retourner (ICI: nom de catégorie, puisque tu veux le nom et non le numéro)
    """
    try:
        df = pd.read_excel(io.BytesIO(help_bytes), sheet_name=sheet_name, header=None, dtype=str)
        df = df.dropna(how="all")
        if df.shape[1] < 2:
            return None
        df = df.iloc[:, :2].copy()
        df.columns = ["key", "value"]
        df["key"] = df["key"].astype(str).str.strip()
        df["value"] = df["value"].astype(str).str.strip()
        return df
    except Exception:
        return None

def _build_map(help_bytes: bytes, sheet_name: str) -> dict[str, str]:
    df = _read_2col_sheet(help_bytes, sheet_name)
    if df is None:
        return {}
    return {str(k).strip().lower(): str(v).strip() for k, v in zip(df["key"], df["value"])}

def _standardize(val, mapping: dict[str, str]) -> str:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return ""
    s = str(val).strip()
    if s == "":
        return ""
    return mapping.get(s.lower(), s)

# ---------- FALLBACK COLOR / SIZE ----------
def _extract_color_size(description: str) -> tuple[str, str]:
    """
    Fallback MVP:
    Essaie de lire des patterns simples en fin de description:
      "... - COLOR - SIZE" / "... , COLOR , SIZE" / "... / COLOR / SIZE"
    """
    if not description:
        return "", ""
    text = str(description).strip()

    parts = re.split(r"\s*[-,/]\s*|\s*,\s*", text)
    parts = [p.strip() for p in parts if p and p.strip()]

    color, size = "", ""
    if len(parts) >= 2:
        last = parts[-1]
        # heuristique size
        if re.fullmatch(r"(X{0,3}S|X{0,3}L|S|M|L|XL|XXL|XXXL|\d{1,2}([./-]\d{1,2})?)", last, flags=re.IGNORECASE):
            size = last
            color = parts[-2]
        else:
            color = parts[-1]

    return color, size

# ---------- CATEGORY MATCH ----------
def _words(s: str) -> set[str]:
    return set(re.findall(r"[a-z0-9]+", str(s).lower()))

def _best_keyword_match_value(description: str, keyword_value_df: pd.DataFrame | None) -> str:
    """
    Exact match: tous les mots du keyword (col A) doivent être présents dans la description.
    Retourne la valeur col B (ICI: NOM de catégorie).
    Choisit le match le plus spécifique (le plus de mots).
    """
    if keyword_value_df is None:
        return ""

    desc_words = _words(description)
    best_value = ""
    best_len = 0

    for _, row in keyword_value_df.iterrows():
        kw = str(row["key"]).strip()
        kw_words = _words(kw)
        if not kw_words:
            continue
        if kw_words.issubset(desc_words):
            if len(kw_words) > best_len:
                best_len = len(kw_words)
                best_value = str(row["value"]).strip()

    return best_value

# ---------- PRICE ----------
def _round_to_nearest_9_99(price) -> float:
    if price is None or (isinstance(price, float) and math.isnan(price)):
        return float("nan")
    p = float(price)
    nearest10 = math.floor(p / 10.0 + 0.5) * 10.0  # half-up
    return round(nearest10 - 0.01, 2)

# ---------- BARCODE ----------
def _clean_barcode_keep_leading_zeros(x) -> str:
    """
    - Si Excel a converti en nombre: enlève .0, convertit en int, puis zfill(12) si <= 12 digits.
    - Si c'est déjà du texte: on conserve tel quel (en enlevant espaces).
    """
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return ""

    s = str(x).strip()
    if s == "":
        return ""

    # cas "12345.0"
    if re.fullmatch(r"\d+\.0", s):
        s = s[:-2]

    # si uniquement digits, padding UPC-A 12 digits si plus court
    if re.fullmatch(r"\d+", s):
        if len(s) <= 12:
            return s.zfill(12)
        return s

    # sinon, on retourne tel quel (au cas où GTIN avec lettres/format)
    return s

# ---------- MAIN ----------
def run_transform(supplier_xlsx_bytes: bytes, help_xlsx_bytes: bytes, vendor_name: str):
    # Lire supplier en mode "texte" pour éviter pertes (UPC, HTS, etc.)
    df = pd.read_excel(io.BytesIO(supplier_xlsx_bytes), sheet_name=0, dtype=str).copy()

    warnings: list[dict] = []

    # Help data (noms d'onglets maintenant OK chez toi)
    color_map = _build_map(help_xlsx_bytes, "Color Standardization")
    size_map = _build_map(help_xlsx_bytes, "Size Standardization")
    country_map = _build_map(help_xlsx_bytes, "Country Abbreviations")

    google_cat = _read_2col_sheet(help_xlsx_bytes, "Google Product Category")   # keyword -> NOM catégorie
    shopify_cat = _read_2col_sheet(help_xlsx_bytes, "Shopify Product Category") # keyword -> NOM catégorie

    # Colonnes requises minimales (Color/Size optionnelles)
    required = ["Product", "Description", "Qty", "Cad MSRP", "UPC", "Origin", "HTS Code"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Colonnes manquantes dans le fichier fournisseur: {missing}")

    # S'assurer que Color/Size existent
    if "Color" not in df.columns:
        df["Color"] = ""
    if "Size" not in df.columns:
        df["Size"] = ""

    # Convertir MSRP en numérique pour arrondi
    df["Cad MSRP_num"] = pd.to_numeric(df["Cad MSRP"].str.replace("$", "", regex=False).str.replace(",", "", regex=False), errors="coerce")

    # Fallback Color/Size depuis Description si vides
    for i, r in df.iterrows():
        color = (r.get("Color") or "").strip()
        size = (r.get("Size") or "").strip()
        if color == "" or size == "":
            fb_color, fb_size = _extract_color_size(r.get("Description", ""))
            if color == "" and fb_color:
                df.at[i, "Color"] = fb_color
            if size == "" and fb_size:
                df.at[i, "Size"] = fb_size

    # Standardisation
    df["Color"] = df["Color"].apply(lambda x: _standardize(x, color_map))
    df["Size"] = df["Size"].apply(lambda x: _standardize(x, size_map))
    df["Origin"] = df["Origin"].apply(lambda x: _standardize(x, country_map))

    # Title = Description + Color
    df["Title"] = (df["Description"].fillna("").astype(str).str.strip() + " " + df["Color"].fillna("").astype(str).str.strip()).str.strip()

    # Handle = Vendor + Description + Color
    df["Handle"] = df.apply(lambda r: slugify(f"{vendor_name} {r['Description']} {r['Color']}".strip()), axis=1)

    # Price rounding x9.99
    df["Variant Price"] = df["Cad MSRP_num"].apply(_round_to_nearest_9_99)

    # Barcode keep leading zeros
    df["Variant Barcode"] = df["UPC"].apply(_clean_barcode_keep_leading_zeros)

    # Category names (NOT numbers) via exact match
    df["Google Category Name"] = df["Description"].apply(lambda d: _best_keyword_match_value(d, google_cat))
    df["Shopify Category Name"] = df["Description"].apply(lambda d: _best_keyword_match_value(d, shopify_cat))

    # Warnings utiles
    for i, r in df.iterrows():
        if (r.get("Variant Barcode") or "").strip() == "":
            warnings.append({"row": i + 2, "issue": "UPC/Barcode manquant"})
        if (r.get("HTS Code") or "").strip() == "":
            warnings.append({"row": i + 2, "issue": "HTS Code manquant"})
        if (r.get("Color") or "").strip() == "":
            warnings.append({"row": i + 2, "issue": "Color vide (après fallback/standardisation)"})
        if (r.get("Size") or "").strip() == "":
            warnings.append({"row": i + 2, "issue": "Size vide (après fallback/standardisation)"})
        if (r.get("Shopify Category Name") or "").strip() == "":
            warnings.append({"row": i + 2, "issue": "Aucun match Shopify Product Category (nom) via Description"})
        if (r.get("Google Category Name") or "").strip() == "":
            warnings.append({"row": i + 2, "issue": "Aucun match Google Product Category (nom) via Description"})

    # Output
    out = pd.DataFrame(columns=SHOPIFY_COLUMNS)
    out["Handle"] = df["Handle"]
    out["Command"] = "NEW"  # ✅ a) NEW
    out["Title"] = df["Title"]
    out["Body (HTML)"] = ""  # MVP
    out["Vendor"] = vendor_name

    # ✅ b) Category: ID = NOM catégorie (pas numéro)
    out["Category: ID"] = df["Shopify Category Name"]

    out["Variant Option1 Name"] = "Size"
    out["Variant Option1 Value"] = df["Size"].astype(str).fillna("")

    out["Variant Option2 Name"] = "Color"
    out["Variant Option2 Value"] = df["Color"].astype(str).fillna("")

    out["Variant Price"] = df["Variant Price"]
    out["Variant SKU"] = df["Product"].astype(str).fillna("")
    out["Variant Barcode"] = df["Variant Barcode"]  # ✅ c) conserve zéros
    out["Variant Inventory Qty"] = 0  # ✅ g) toujours 0

    out["Country of Origin"] = df["Origin"].astype(str).fillna("")
    out["HS Code"] = df["HTS Code"].astype(str).fillna("")

    # ✅ d) Metafield = NOM catégorie Google (pas numéro)
    out["Metafield: mm-google-shopping.google_product_category"] = df["Google Category Name"]

    warnings_df = pd.DataFrame(warnings)

    # Export Excel
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        out.to_excel(writer, index=False, sheet_name="shopify_import")
        warnings_df.to_excel(writer, index=False, sheet_name="warnings")
    buffer.seek(0)

    return buffer.getvalue(), warnings_df
