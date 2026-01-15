import streamlit as st
import mysql.connector
import pandas as pd
import os
from datetime import datetime
from pymongo import MongoClient
from apscheduler.schedulers.background import BackgroundScheduler

st.set_page_config(page_title="Jobs")

class JobManager:
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()
        self.uri = "mongodb://localhost:27017/"
        self.client = MongoClient(self.uri)
        self.db = self.client.app
        self.collection = self.db.test_job
        self.hist = self.db.historique
        self.jobs = list(self.collection.find())

    def connect_mongo(self):
        """Vérifie la connexion MongoDB"""
        try:
            self.client.admin.command('ping')
        except Exception as e:
            st.error(f"Erreur MongoDB : {e}")
    
    def executer(self, j):
        """Exécute un job sur MySQL"""
        df = pd.DataFrame()
        try:
            conn = mysql.connector.connect(host=j['host'], user="root", password="", database=j['db'])
            cursor = conn.cursor()

            # Lecture CSV
            for f in os.listdir(j['path']):
                if f.endswith(".csv"):
                    df = pd.concat([df, pd.read_csv(os.path.join(j['path'], f))], ignore_index=True)

            # Création table si elle n'existe pas
            cursor.execute(
                f"CREATE TABLE IF NOT EXISTS {j['table']} "
                "(id INT AUTO_INCREMENT PRIMARY KEY, nom VARCHAR(50), prenom VARCHAR(50))"
            )

            # Insertion CSV
            for _, row in df.iterrows():
                cursor.execute(f"INSERT INTO {j['table']} (nom, prenom) VALUES (%s, %s)", (row["nom"], row["prenom"]))

            # Exécution requêtes SQL
            for r in j['requete']:
                if r.strip():
                    cursor.execute(r)

            conn.commit()
        except Exception as e:
            st.error(e.args)
            self.hist.insert_one({"Job": j['titre'], "date execution": datetime.now(), "erreur": str(e)})
        else:
            st.success(f"{j['titre']} fait !")
            self.hist.insert_one({"Job": j['titre'], "date execution": datetime.now(), "erreur": ""})
        finally:
            if 'conn' in locals():
                cursor.close()
                conn.close()
        st.markdown('<a href="/transform?run=1" target=_blank> Executer </a>',unsafe_allow_html=True)

    def display_jobs(self):
        """Affiche tous les jobs avec leurs options"""
        for j in self.jobs:
            with st.expander(j['titre']):
                col1, col2, col3 = st.columns([1, 1, 1])
                
                # Exécuter
                with col1:
                    if st.button("Executer", key=j['_id']):
                        self.executer(j)
                
                # Annuler planification
                with col2:
                    annuler = st.button("Annuler planification", key=str(j['_id']) + "a")
                
                # Supprimer Job
                with col3:
                    supp = st.button("Supprimer Job", key=str(j['_id']) + "s")
                
                # Formulaire planification
                form_key = "Planification_form_" + str(j['_id'])
                with st.form(key=form_key):
                    date = st.date_input("Date")
                    heure = st.time_input("Heure")
                    options = ["chaque minute", "chaque jour", "chaque semaine", "chaque mois"]
                    choix = st.selectbox("Choisissez une option :", options)
                    submit = st.form_submit_button("Enregistrer", key=str(j['_id']) + "e")
                    dt = datetime.combine(date, heure)

                    if submit:
                        self.connect_mongo()
                        self.collection.update_one(
                            {"titre": j['titre']}, 
                            {"$set": {"d": dt, "frequence": choix}}
                        )
                        self.planifier_job(j, dt, choix)
                        st.success("Planification enregistrée !")

                # Actions annuler et supprimer
                if annuler:
                    self.collection.update_one(
                        {"_id": j['_id']},
                        {"$set": {"d": "", "frequence": ""}}
                    )
                    st.success("Planification annulée !")
                if supp:
                    self.collection.delete_one({"_id": j['_id']})
                    st.success(f"{j['titre']} supprimé avec succès !")

    def planifier_job(self, j, dt, choix):
        """Planifie un job avec APScheduler"""
        job_id = str(j["_id"])
        # Supprime la planification précédente si elle existe
        if self.scheduler.get_job(job_id):
            self.scheduler.remove_job(job_id)

        if choix == "chaque minute":
            self.scheduler.add_job(self.executer, trigger="interval", minutes=1, start_date=dt, args=[j], id=job_id)
        elif choix == "chaque jour":
            self.scheduler.add_job(self.executer, trigger="interval", days=1, start_date=dt, args=[j], id=job_id)
        elif choix == "chaque semaine":
            self.scheduler.add_job(self.executer, trigger="interval", weeks=1, start_date=dt, args=[j], id=job_id)
        elif choix == "chaque mois":
            self.scheduler.add_job(
                self.executer, trigger="cron", day=dt.day, hour=dt.hour, minute=dt.minute, args=[j], id=job_id
            )

# ==================== MAIN ====================
job_manager = JobManager()

if st.button("Accueil"):
    st.switch_page("home.py")

st.header("Jobs")
job_manager.display_jobs()
