import streamlit as st
import mysql.connector
import pandas as pd
import os
import re
from pymongo import MongoClient
from datetime import datetime


st.set_page_config(page_title="Créer Job")
st.set_page_config(layout="wide")


if st.button("Accueil"):
  st.switch_page("home.py")

st.header("Créer un Job")
uri = "mongodb://localhost:27017/"
client = MongoClient(uri)
db = client.app 
collection=db.test_job
hist=db.historique


col1,col2= st.columns([3,1])
with col1:
    titre = st.text_input("Titre")
    path_doss = st.text_input("Chemin complet du dossier")
    dbName = st.text_input("Nom de la base")
    host = st.text_input("host")
    table = st.text_input("Nom de la table")

    df=pd.DataFrame()
    c=0
    files=pd.DataFrame()


    
with col2:
    st.title("Requetes SQL") 
    sql=st.text_area('Entrez vos requetes SQL (precedées par " - " ) ',height=315)

col3,col4=st.columns([1,1])
with col3:
    if st.button("Ajouter Job"):    
        uri = "mongodb://localhost:27017/"
        client = MongoClient(uri)
        
        try:
            client.admin.command('ping')
        except Exception as e:
            st.error(f"Erreur : {e}")
        if path_doss and dbName and table and titre and host :
            req= [] 
            req = sql.split("-")
            req.pop(0)
            
            resultat = collection.insert_one({
                "titre":titre,
                "path":path_doss,
                "db":dbName,
                "host":host,
                "table":table,
                "requete":req,
            })
            
            if resultat.inserted_id:
                st.success(f"Job ajouté ")           
        else:
            st.warning("Veuillez remplir les champs correctement !") 
            st.write(sql)
with col4:
    if st.button("Executer"):
        if path_doss and dbName and table and titre and host :
            conn= mysql.connector.connect(host=host,user="root",password="",database=dbName)
            cursor=conn.cursor()   
            hist=db.historique

            df=pd.DataFrame()      
            for f in os.listdir(path_doss):
                if f.endswith(".csv"):
                    df = pd.concat([df,pd.read_csv(os.path.join(path_doss, f))],ignore_index=True)            
            req= [] 
            req = sql.split("-")
            req.pop(0)
            try:
                cursor.execute("CREATE TABLE IF NOT EXISTS  "+table+ "(id INT AUTO_INCREMENT PRIMARY KEY  ,nom VARCHAR(50),prenom VARCHAR (50));")                   
                for _, row in df.iterrows():
                    cursor.execute("INSERT INTO "+table+" (nom, prenom) VALUES (%s, %s)",(row["nom"], row["prenom"]))

                for r in req:
                    cursor.execute(r)

                conn.commit()
            except Exception as e:
                st.error(e.args)
                r=hist.insert_one({"Job":titre,"date execution":datetime.now(),"erreur":e.args})
            else:
                st.success(titre+" fait !")
                st.write(pd.read_sql("SELECT * FROM "+table, conn))
                r=hist.insert_one({"Job":titre,"date execution":datetime.now(),"erreur":""})
        else:
            st.warning("Veuillez remplir les champs correctement !") 
        