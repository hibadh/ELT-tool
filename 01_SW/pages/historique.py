import streamlit as st
from pymongo import MongoClient

st.set_page_config(page_title="Historique")

class HistoriqueManager:
    def __init__(self):
        self.uri = "mongodb://localhost:27017/"
        self.client = MongoClient(self.uri)
        self.db = self.client.app
        self.historique = self.db.historique
        self.connect_mongo()

    def connect_mongo(self):
        """Vérifie la connexion à MongoDB"""
        try:
            self.client.admin.command('ping')
        except Exception as e:
            st.error(f"Erreur MongoDB : {e}")

    def effacer_historique(self):
        """Efface tout l'historique"""
        if st.button("Effacer historique"):
            self.historique.delete_many({})
            st.success("Historique effacé !")

    def afficher_historique(self):
        """Affiche tous les jobs dans l'historique"""
        hist = self.historique.find()
        for h in hist:
            with st.container():
                st.write(f"**Titre:** {h.get('Job', '')}")
                st.write(f"**Date execution:** {h.get('date execution', '')}")
                erreur = h.get('erreur', '')
                if erreur:
                    st.write(f"**Erreur:** {erreur}")


# ==================== MAIN ====================
if st.button("Accueil"):
    st.switch_page("home.py")

st.header("Historique des Jobs")

historique_manager = HistoriqueManager()
historique_manager.effacer_historique()
historique_manager.afficher_historique()
