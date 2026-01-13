import streamlit as st

st.set_page_config(page_title="Accueil")
st.header("Bienvenue")
col1, col2, col3= st.columns([1,1,1])
with col1:
  if st.button("Consulter les Jobs"):
    st.switch_page("pages/listJobs.py")
with col2:
  if st.button("Créer un Job"):
    st.switch_page('pages/JOB_new.py')
with col3:
  if st.button("Historique"):
    st.switch_page('pages/historique.py')