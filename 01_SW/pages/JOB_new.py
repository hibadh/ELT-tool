import streamlit as st
import mysql.connector
import pandas as pd
import os
from pymongo import MongoClient
from datetime import datetime

st.set_page_config(page_title="Créer Job", layout="wide")

class JobManager:
    def __init__(self):
        self.df = pd.DataFrame()
        self.selected = pd.DataFrame()
        self.init_mongo()
        self.init_session()
    
    def init_mongo(self):
        """Connexion à MongoDB"""
        self.uri = "mongodb://localhost:27017/"
        self.client = MongoClient(self.uri)
        self.db = self.client.app
        self.collection = self.db.test_job
        self.hist = self.db.historique
    
    def init_session(self):
        """Initialisation des states"""
        if "tab" not in st.session_state:
            st.session_state.tab = None
        if "df" not in st.session_state:
            st.session_state.df = pd.DataFrame()

    def render_inputs(self):
        """Affichage des inputs Streamlit"""
        col1, col2 = st.columns([3,1])
        with col1:
            self.titre = st.text_input("Titre")
            self.path_doss = st.text_input("Chemin complet du dossier")
            self.dbName = st.text_input("Nom de la base")
            self.host = st.text_input("host")
            self.table = st.text_input("Nom de la table")
            self.files = pd.DataFrame()
        with col2:
            st.title("Requetes SQL")
            self.sql = st.text_area(
                'Entrez vos requetes SQL (precedées par " - " ) ',
                height=315
            )
    
    def ajouter_job(self):
        """Ajouter un job dans MongoDB"""
        if st.button("Ajouter Job"):
            try:
                self.client.admin.command('ping')
            except Exception as e:
                st.error(f"Erreur MongoDB : {e}")
                return
            
            if self.titre and self.path_doss and self.dbName and self.host and self.table:
                req = self.sql.split("-")[1:]  # On supprime le premier élément vide
                resultat = self.collection.insert_one({
                    "titre": self.titre,
                    "path": self.path_doss,
                    "db": self.dbName,
                    "host": self.host,
                    "table": self.table,
                    "requete": req
                })
                if resultat.inserted_id:
                    st.success(f"Job '{self.titre}' ajouté")
            else:
                st.warning("Veuillez remplir tous les champs correctement !")
    
    def executer(self):
        """Exécuter le job"""
        if st.button("Executer"):
            if not (self.titre and self.path_doss and self.dbName and self.host and self.table):
                st.warning("Veuillez remplir tous les champs correctement !")
                st.switch_page("pages/transform.py")
                return

            try:
                conn = mysql.connector.connect(
                    host=self.host,
                    user="root",
                    password="",
                    database=self.dbName
                )
                cursor = conn.cursor()
                
                # Lecture des fichiers CSV
                for f in os.listdir(self.path_doss):
                    if f.endswith(".csv"):
                        self.df = pd.concat([self.df, pd.read_csv(os.path.join(self.path_doss, f))], ignore_index=True)

                # Création de la table si nécessaire
                cursor.execute(
                    f"CREATE TABLE IF NOT EXISTS {self.table} "
                    "(id INT AUTO_INCREMENT PRIMARY KEY, nom VARCHAR(50), prenom VARCHAR(50))"
                )

                # Insertion des données CSV
                for _, row in self.df.iterrows():
                    cursor.execute(f"INSERT INTO {self.table} (nom, prenom) VALUES (%s, %s)", (row["nom"], row["prenom"]))

                # Exécution des requêtes SQL
                req = self.sql.split("-")[1:]
                for r in req:
                    if r.strip():
                        cursor.execute(r)
                
                conn.commit()

                # Chargement des données pour affichage
                tab = pd.read_sql(f"SELECT * FROM {self.table}", conn)
                tab["select"] = False
                st.session_state.tab = tab

                st.success(f"Job '{self.titre}' exécuté avec succès !")
                self.hist.insert_one({"Job": self.titre, "date execution": datetime.now(), "erreur": ""})
            except Exception as e:
                st.error(f"Erreur : {e}")
                self.hist.insert_one({"Job": self.titre, "date execution": datetime.now(), "erreur": str(e)})
            finally:
                if 'conn' in locals():
                    cursor.close()
                    conn.close()
            return tab

    def display_table(self):
        """Affichage du tableau éditable et sélection des lignes"""
        if st.session_state.tab is not None:
            edited_tab = st.data_editor(
                st.session_state.tab,
                key="table_editor",
                hide_index=False
            )
            if not edited_tab.equals(st.session_state.tab):
                st.session_state.tab = edited_tab
                st.rerun()
            
            # Lignes sélectionnées
            st.write("Lignes sélectionnées")
            self.selected = st.session_state.tab[st.session_state.tab["select"]]
            st.write(self.selected)

    def supprimer_lignes(self):
        """Supprimer les lignes sélectionnées"""
        if st.button("Supprimer lignes") and not self.selected.empty:
            try:
                conn = mysql.connector.connect(
                    host=self.host,
                    user="root",
                    password="",
                    database=self.dbName
                )
                cursor = conn.cursor()
                for _, s in self.selected.iterrows():
                    cursor.execute(f"DELETE FROM {self.table} WHERE id = %s", (s["id"],))
                conn.commit()
                st.success("Lignes supprimées avec succès")
            except Exception as e:
                st.error(f"Erreur suppression : {e}")
            finally:
                if 'conn' in locals():
                    cursor.close()
                    conn.close()

# ================== MAIN ==================
job_manager = JobManager()

if st.button("Accueil"):
    st.switch_page("home.py")

st.header("Créer un Job")
job_manager.render_inputs()
job_manager.ajouter_job()
job_manager.executer()
job_manager.display_table()
job_manager.supprimer_lignes()
