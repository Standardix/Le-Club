import streamlit as st
import io
import openpyxl
import time

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

st.title("Générateur de fichier Shopify (Pilote)")

SUPPLIERS = {
    "Balmoral": run_abc,
    "Bandit": run_abc,
    "Café du Cycliste": run_abc,
    "Ciele": run_abc,
    "District Vision": run_abc,
    "Fingerscrossed": run_abc,
    "MAAP": run_abc,
    "Pas Normal Studios": run_abc,
    "Tracksmith": run_abc,
}

st.markdown("### 1️⃣ Sélection du fournisseur")
supplier_name = st.selectbox("Choisir le fournisseur", list(SUPPLIERS.keys()))

st.markdown("### 2️⃣ Upload des fichiers")
supplier_file = st.file_uploader("Fichier fournisseur (.xlsx)", type=["xlsx"])
help_file = st.file_uploader("Help data (.xlsx)", type=["xlsx"])

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
                help_xlsx_bytes=help_file.getvalue(),
                vendor_name=supplier_name,
                brand_choice=brand_choice,  # toujours vide pour le pilote
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
