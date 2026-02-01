import streamlit as st
import io
import openpyxl
import time
import hashlib
import pandas as pd
import re


def _read_csv_bytes(file_bytes: bytes) -> pd.DataFrame:
    """Robust CSV reader for supplier files (encoding + delimiter)."""
    encodings = ["utf-8-sig", "utf-8", "cp1252", "latin1"]
    seps = [",", ";", "	"]
    last_err = None
    for enc in encodings:
        for sep in seps:
            try:
                return pd.read_csv(io.BytesIO(file_bytes), encoding=enc, sep=sep, dtype=str, keep_default_na=False)
            except Exception as e:
                last_err = e
                continue
    # final fallback: replace undecodable chars
    try:
        return pd.read_csv(io.BytesIO(file_bytes), encoding="cp1252", sep=",", dtype=str, keep_default_na=False, encoding_errors="replace")
    except Exception as e:
        last_err = e
    raise ValueError(f"Impossible de lire le CSV (encodage). Dernière erreur: {last_err}")


from suppliers.fournisseur_abc import run_transform as run_abc

st.set_page_config(page_title="Générateur Shopify – Fichiers fournisseurs", layout="wide")

# --- CSS bouton (normal + hover) ---
st.markdown(
    """
    <style>
    div[data-testid="stButton"] > button {
        background: #ffffff !important;
        border: 1px solid #d6d6d9 !important;
        color: #2f5f8f !important;
        border-radius: 10px !important;
        padding: 0.55rem 1.1rem !important;
        font-weight: 500 !important;
        box-shadow: none !important;
    }
    div[data-testid="stButton"] > button:hover {
        background: #f0f2f6 !important;
        border: 1px solid #d6d6d9 !important;
        color: #2f5f8f !important;
    }
    div[data-testid="stButton"] > button:focus {
        box-shadow: none !important;
        outline: none !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

st.title("Générateur de fichier Shopify")

SUPPLIERS = {
    "Balmoral": run_abc,
    "Bandit": run_abc,
    "Café du Cycliste": run_abc,
    "Ciele": run_abc,
    "District Vision": run_abc,
    "Fingerscrossed": run_abc,
    "MAAP": run_abc,
    "norda": run_abc,
    "Pas Normal Studios": run_abc,
    "Rapha": run_abc,
    "Soar": run_abc,
    "Tracksmith": run_abc,
    "Satisfy": run_abc,

}

st.markdown("### 1️⃣ Sélection du fournisseur")
supplier_name = st.selectbox("Choisir le fournisseur", sorted(SUPPLIERS.keys(), key=lambda x: x.lower()))

st.markdown("### 2️⃣ Upload des fichiers")
supplier_file = st.file_uploader("Fichier fournisseur (.xlsx ou .csv)", type=["xlsx","csv"])
help_file = st.file_uploader("Help data (.xlsx)", type=["xlsx"])


existing_shopify_file = st.file_uploader("Fichier de produits existant dans Shopify (.xlsx)", type=["xlsx"])
st.markdown("### 3️⃣ Tags")

event_promo_tag = st.selectbox(
    "Event/Promotion Related",
    options=["", "spring-summer", "fall-winter"],
    index=0,
)

style_season_map: dict[str, str] = {}

def _clean_style_key(v) -> str:
    s = " ".join(str(v or "").strip().split())
    # if Excel treated numeric as float: 123.0 -> 123
    s = re.sub(r"^(\d+)\.0+$", r"\1", s)
    return s

def _clean_style_number_base(v) -> str:
    """
    Seasonality UX: keep only what is BEFORE the first hyphen.
    Example: 11000-FA-SAB -> 11000
    """
    s = _clean_style_key(v)
    if not s:
        return ""
    return s.split("-", 1)[0].strip()

def _first_existing_col(cols: list[str], candidates: list[str]) -> str | None:
    """
    Robust column matcher:
    - strip surrounding whitespace
    - collapse internal whitespace
    - case-insensitive
    - fallback to contains match
    """
    def norm(x: str) -> str:
        s = str(x or "")
        s = re.sub(r"\s+", " ", s).strip().lower()
        return s

    col_map = {norm(c): c for c in cols}
    for cand in candidates:
        k = norm(cand)
        if k in col_map:
            return col_map[k]

    cols_norm = [(norm(c), c) for c in cols]
    for cand in candidates:
        ck = norm(cand)
        if not ck:
            continue
        for cn, orig in cols_norm:
            if ck in cn:
                return orig
    return None


def _norm(s) -> str:
    """Normalize cell text for display (keep content, just collapse whitespace)."""
    return re.sub(r"\s+", " ", str(s or "")).strip()


def _extract_unique_style_rows(file_bytes: bytes, supplier_name: str = "", file_name: str = "") -> pd.DataFrame | None:
    """Extract unique styles from the supplier file (.xlsx ou .csv).

    Returns a dataframe with columns (when available) in this order:
      1) Style Name
      2) Style Number
    """
    is_csv = str(file_name or "").strip().lower().endswith(".csv")
    if is_csv:
        try:
            df0 = _read_csv_bytes(file_bytes)
        except Exception:
            return None

        # Minimal detection (case-insensitive)
        cols_norm = {str(c).strip().lower(): c for c in df0.columns}
        style_name_col = cols_norm.get("style name") or cols_norm.get("style_name") or cols_norm.get("style") or cols_norm.get("style name ")
        style_number_col = cols_norm.get("style number") or cols_norm.get("style no") or cols_norm.get("style code") or cols_norm.get("style #")

        if not style_name_col and not style_number_col:
            return None

        out = pd.DataFrame()
        if style_name_col:
            out["Style Name"] = df0[style_name_col]
        if style_number_col:
            out["Style Number"] = df0[style_number_col]

        # Normalize and drop duplicates
        for c in out.columns:
            out[c] = out[c].astype(str).str.strip()
        out = out.drop_duplicates()

        cols = []
        if "Style Name" in out.columns:
            cols.append("Style Name")
        if "Style Number" in out.columns:
            cols.append("Style Number")
        return out[cols].reset_index(drop=True)

    bio = io.BytesIO(file_bytes)
    xls = pd.ExcelFile(bio)

    vendor_key = re.sub(r"[\s\-_/]+", "", str(supplier_name or "").strip().lower())
    is_pas = vendor_key in ("pasnormalstudios", "pasnormalstudio")
    # PAS Normal Studios: use only "Summary + Data" like the transformer (ensures correct STYLE NAME)
    if vendor_key in ("pasnormalstudios", "pasnormalstudio") and "Summary + Data" in xls.sheet_names:
        sheet_names = ["Summary + Data"]
    else:
        sheet_names = list(xls.sheet_names)
    # Prefer explicit style-number fields (avoid generic "Style" which can match a style-name field in some files)
    style_number_candidates = [
        "Style NO", "Style No", "STYLE NO", "style no",
        "Style Number", "Style Num", "Style #", "style number", "style #",
        "Style Code", "style code",
    ]
    style_name_candidates = [
        "STYLE NAME", "Style Name", "Style name", "style name",
        "Product Name", "Name",
    ]

    rows: list[pd.DataFrame] = []
    for sheet in sheet_names:
        try:
            df = pd.read_excel(xls, sheet_name=sheet)

            # PAS: Force the supplier column "STYLE NAME" as the source for Style Name (when present)
            if is_pas:
                def _find_pas_style_name(cols: list[str]) -> str | None:
                    def norm(x: str) -> str:
                        return re.sub(r"\s+", " ", str(x or "")).strip().lower()
                    for c in cols:
                        if norm(c) == "style name":
                            return c
                    return None

                pas_style_name_col = _find_pas_style_name(list(df.columns))
                if pas_style_name_col:
                    # Override candidate list so we never match other "...NAME" columns (ex: STYLE COLOR NAME)
                    style_name_candidates = [pas_style_name_col]
        except Exception:
            continue
        if df is None or df.empty:
            continue

        # PAS: keep only rows with Order Qty >= 1 when extracting Seasonality styles
        if is_pas:
            oq_col = _first_existing_col(list(df.columns), ["Order Qty", "order qty", "Order Quantity", "order quantity"])
            if oq_col:
                qty_num = pd.to_numeric(
                    df[oq_col].astype(str).str.replace(",", "", regex=False).str.strip(),
                    errors="coerce",
                ).fillna(0)
                df = df.loc[qty_num >= 1].copy()
                if df.empty:
                    continue

        num_col = _first_existing_col(list(df.columns), style_number_candidates)
        # Style Name: STRICT priority on the real supplier column (avoid matching Product Name / Name)
        def _find_style_name_col(cols: list[str]) -> str | None:
            def norm(x: str) -> str:
                return re.sub(r"\s+", " ", str(x or "")).strip().lower()
            for c in cols:
                nc = norm(c)
                if nc in ("style name", "stylename", "style_name"):
                    return c
                if nc.startswith("style name"):
                    return c
            return None
        # GLOBAL RULE: if a supplier column "Style Name" exists, ALWAYS use it for UX Style Name.
        def _find_style_name_col(cols: list[str]) -> str | None:
            def norm(x: str) -> str:
                # collapse whitespace and lowercase for robust matching
                return re.sub(r"\s+", " ", str(x or "")).strip().lower()
            for c in cols:
                nc = norm(c)
                # accept common variants
                if nc in ("style name", "stylename", "style_name", "style-name"):
                    return c
                if nc.startswith("style name"):
                    return c
            return None

        name_col = _find_style_name_col(list(df.columns))
        if not name_col:
            # fallback ONLY when Style Name truly not present
            name_col = _first_existing_col(list(df.columns), ["Product Name", "Name", "Title", "Description"])

        if not num_col and not name_col:
            continue

        data = {}
        if name_col:
            pass
        data["Style Name"] = df[name_col].astype(str).fillna("").map(_norm)
        if num_col:
            data["Style Number"] = df[num_col].map(_clean_style_number_base)

        tmp = pd.DataFrame(data)
        for c in tmp.columns:
            tmp = tmp[tmp[c].astype(str).str.strip().ne("").fillna(False)]

        if not tmp.empty:
            rows.append(tmp)

    if not rows:
        return None

    out = pd.concat(rows, ignore_index=True).drop_duplicates()
    # De-duplicate by Style Number ONLY (one row per style).
    # If multiple Style Name values exist for the same Style Number in the supplier file,
    # keep the most frequent value FROM THE ACTUAL "Style Name" column.
    if "Style Number" in out.columns and "Style Name" in out.columns:
        out["Style Number"] = out["Style Number"].astype(str).str.strip()
        out["Style Name"] = out["Style Name"].astype(str).str.strip()

        def mode_nonempty(s: pd.Series) -> str:
            s = s.dropna().astype(str).str.strip()
            s = s[s.ne("")]
            if s.empty:
                return ""
            return s.value_counts().index[0]

        agg = out.groupby("Style Number")["Style Name"].apply(mode_nonempty).reset_index()
        out = agg



    cols = []
    if "Style Name" in out.columns:
        cols.append("Style Name")
    if "Style Number" in out.columns:
        cols.append("Style Number")

    return out[cols].reset_index(drop=True)

if supplier_file is not None:
    style_rows_df = _extract_unique_style_rows(supplier_file.getvalue(), supplier_name, supplier_file.name)
    if style_rows_df is not None and not style_rows_df.empty:
        st.markdown("#### Seasonality")

        key_col = "Style Number" if "Style Number" in style_rows_df.columns else "Style Name"
        # Stabilize ordering so the fingerprint doesn't change between reruns
        style_rows_df = style_rows_df.sort_values(by=key_col).reset_index(drop=True)

        supplier_fp = hashlib.md5(supplier_file.getvalue()).hexdigest()
        styles_fp = hashlib.md5("|".join(style_rows_df[key_col].astype(str).tolist()).encode("utf-8")).hexdigest()
        fp = f"{supplier_fp}:{styles_fp}:{key_col}"
        widget_key = "seasonality_editor"

        # Initialize/refresh ONLY when file/styles change
        if st.session_state.get("seasonality_fp") != fp:
            st.session_state["seasonality_fp"] = fp

            # preserve prior entries from stored df (not widget key)
            prev = st.session_state.get("seasonality_df")
            prev_map = {}
            if prev is not None and isinstance(prev, pd.DataFrame) and key_col in prev.columns and "Seasonality Tags" in prev.columns:
                prev_map = {
                    _clean_style_key(k): str(v).strip()
                    for k, v in zip(prev[key_col].astype(str), prev['Seasonality Tags'].astype(str))
                    if _clean_style_key(k)
                }

            init_df = style_rows_df.copy()
            init_df["Seasonality Tags"] = init_df[key_col].astype(str).map(lambda k: prev_map.get(_clean_style_key(k), ""))
            st.session_state["seasonality_df"] = init_df

            # Reset the widget state so it reloads the new dataframe cleanly
            if widget_key in st.session_state:
                del st.session_state[widget_key]

        # Safety: ensure df exists
        if "seasonality_df" not in st.session_state or st.session_state["seasonality_df"] is None:
            tmp = style_rows_df.copy()
            tmp["Seasonality Tags"] = ""
            st.session_state["seasonality_df"] = tmp

            st.session_state["seasonality_fp"] = fp

            # reset widget state only on change
            if "seasonality_editor" in st.session_state:
                del st.session_state["seasonality_editor"]

            prev = st.session_state.get("seasonality_df")
            prev_map = {}
            if prev is not None and key_col in prev.columns and "Seasonality Tags" in prev.columns:
                prev_map = {
                    _clean_style_key(k): str(v or "").strip()
                    for k, v in zip(prev[key_col], prev["Seasonality Tags"])
                    if _clean_style_key(k)
                }

            init_df = style_rows_df.copy()
            init_df["Seasonality Tags"] = init_df[key_col].map(lambda k: prev_map.get(_clean_style_key(k), ""))
            st.session_state["seasonality_df"] = init_df

        # guard
        if "seasonality_df" not in st.session_state or st.session_state["seasonality_df"] is None:
            tmp = style_rows_df.copy()
            tmp["Seasonality Tags"] = ""
            st.session_state["seasonality_df"] = tmp

        edited_df = st.data_editor(
            st.session_state["seasonality_df"],
            key="seasonality_editor",
            use_container_width=True,
            hide_index=True,
            num_rows="fixed",
            column_config={
                "Style Name": st.column_config.TextColumn(disabled=True),
                "Style Number": st.column_config.TextColumn(disabled=True),
                "Seasonality Tags": st.column_config.TextColumn(
                    help="Champ libre (ex: spring-summer, fall-winter, core, etc.)",
                    required=False,
                ),
            },
        )

        # Use the widget live state; normalize to DataFrame
        current_df = st.session_state.get("seasonality_editor", edited_df)
        if not isinstance(current_df, pd.DataFrame):
            try:
                current_df = pd.DataFrame(current_df)
            except Exception:
                current_df = edited_df
        style_season_map = {}
        for _, r in current_df.iterrows():
            k = _clean_style_key(r.get(key_col, ""))
            v = str(r.get("Seasonality Tags", "")).strip()
            if k and v:
                style_season_map[k] = v
    else:
        st.info("Aucun champ 'Style Name' ou 'Style Number' détecté dans le fichier. Seasonality ignorée.")

# 🔹 Projet pilote : pas de sélection de marque
brand_choice = ""

generate = st.button(
    "Générer le fichier Shopify",
    type="secondary",
    disabled=not (supplier_file and help_file),
)

if generate:
    st.markdown("### Génération en cours")
    status = st.empty()
    progress = st.progress(0)

    try:
        transform_fn = SUPPLIERS[supplier_name]

        status.info("Préparation des fichiers…")
        progress.progress(10)
        time.sleep(0.15)

        status.info("Lecture du fichier fournisseur…")
        progress.progress(25)
        time.sleep(0.15)

        status.info("Lecture du help data…")
        progress.progress(40)
        time.sleep(0.15)

        with st.spinner("Traitement en cours…"):
            output_bytes, warnings_df = transform_fn(
                supplier_xlsx_bytes=supplier_file.getvalue(),
                supplier_filename=supplier_file.name,
                help_xlsx_bytes=help_file.getvalue(),
                existing_shopify_xlsx_bytes=(existing_shopify_file.getvalue() if existing_shopify_file is not None else None),
                vendor_name=supplier_name,
                brand_choice=brand_choice,  # toujours vide pour le pilote
                            event_promo_tag=event_promo_tag,
                style_season_map=style_season_map,
            )

        status.info("Finalisation du fichier Shopify…")
        progress.progress(85)
        time.sleep(0.15)

        progress.progress(100)
        status.success("Fichier généré avec succès ✅")

        if warnings_df is not None and not warnings_df.empty:
            with st.expander("⚠️ Warnings détectés"):
                st.dataframe(warnings_df, use_container_width=True)

        st.download_button(
            label="⬇️ Télécharger output.xlsx",
            data=output_bytes,
            file_name=f"output_{supplier_name.replace(' ', '_')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    except Exception as e:
        progress.empty()
        status.error(f"Erreur lors de la génération : {e}")
