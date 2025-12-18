import streamlit as st
from suppliers.fournisseur_abc import run_transform as run_abc

st.set_page_config(
    page_title="Générateur Shopify – Fichiers fournisseurs",
    layout="wide"
)

# --- CSS bouton (normal + hover) ---
st.markdown(
    """
    <style>
    /* Style pour tous les boutons Streamlit (MVP simple) */
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
    unsafe_allow_html=True
)

st.title("Générateur de fichier Shopify (MVP)")

SUPPLIERS = {
    "Fournisseur ABC": run_abc,
    # Ajouter d'autres fournisseurs ici plus tard
}

st.markdown("### 1️⃣ Sélection du fournisseur")
supplier_name = st.selectbox("Choisir le fournisseur", list(SUPPLIERS.keys()))

st.markdown("### 2️⃣ Upload des fichiers")
supplier_file = st.file_uploader("Fichier fournisseur (.xlsx)", type=["xlsx"])
help_file = st.file_uploader("Help data (.xlsx)", type=["xlsx"])

generate = st.button(
    "Générer le fichier Shopify",
    type="secondary",
    disabled=not (supplier_file and help_file)
)

if generate:
    try:
        transform_fn = SUPPLIERS[supplier_name]

        output_bytes, warnings_df = transform_fn(
            supplier_xlsx_bytes=supplier_file.getvalue(),
            help_xlsx_bytes=help_file.getvalue(),
            vendor_name=supplier_name
        )

        st.success("Fichier généré avec succès ✅")

        if warnings_df is not None and not warnings_df.empty:
            with st.expander("⚠️ Warnings détectés"):
                st.dataframe(warnings_df, use_container_width=True)

        st.download_button(
            label="⬇️ Télécharger output.xlsx",
            data=output_bytes,
            file_name=f"output_{supplier_name.replace(' ', '_')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        st.error(f"Erreur lors de la génération : {e}")
