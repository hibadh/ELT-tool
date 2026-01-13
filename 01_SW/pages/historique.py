import streamlit as st
import mysql.connector
import pandas as pd
import os
from pymongo import MongoClient

st.set_page_config(page_title="Historique")

if st.button("Accueil"):
  st.switch_page("home.py")

st.header("Historique des Jobs")

uri = "mongodb://localhost:27017/"
client = MongoClient(uri)
db = client.app 
historique=db.historique
hist = historique.find()

def connect_mongo():  
  try:
    client.admin.command('ping')
  except Exception as e:
    st.error(f"Erreur : {e}")

connect_mongo()

if st.button("Effacer historique"):
  db.historique.delete_many({})

for h in hist:
  with st.container(border=True):
    st.write("Titre: "+h['Job'])
    st.write("Date execution: "+str(h['date execution']))
    if str(h['erreur']):
      st.write(str(h['erreur']))
