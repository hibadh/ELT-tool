import streamlit as st
import mysql.connector
import pandas as pd
import os
from datetime import datetime
from pymongo import MongoClient
from datetime import datetime, time
from apscheduler.schedulers.background import BackgroundScheduler

scheduler = BackgroundScheduler()
scheduler.start()

st.set_page_config(page_title="Jobs")

if st.button("Accueil"):
  st.switch_page("home.py")

st.header("Jobs")
uri = "mongodb://localhost:27017/"
client = MongoClient(uri)
db = client.app 
collection=db.test_job
hist=db.historique
jobs = collection.find()

#########################################

def connect_mongo():  
  try:
    client.admin.command('ping')
  except Exception as e:
    st.error(f"Erreur : {e}")

#########################################

def executer(j):
  conn= mysql.connector.connect(host=j['host'],user="root",password="",database=j['db'])
  cursor=conn.cursor()   
  df=pd.DataFrame()       
  for f in os.listdir(j['path']):
    if f.endswith(".csv"):
      df = pd.concat([df,pd.read_csv(os.path.join(j['path'], f))],ignore_index=True)    
  try:
    cursor.execute("CREATE TABLE IF NOT EXISTS  "+j['table']+ "(id INT AUTO_INCREMENT PRIMARY KEY  ,nom VARCHAR(50),prenom VARCHAR (50));")                   
    for _, row in df.iterrows():
      cursor.execute("INSERT INTO "+j['table']+" (nom, prenom) VALUES (%s, %s)",(row["nom"], row["prenom"]))

    for r in j['requete']:
      cursor.execute(r)

    conn.commit()
  except Exception as e:
    st.error(e.args)
    r=hist.insert_one({"Job":j['titre'],"date execution":datetime.now(),"erreur":e.args})
  else:
    st.success(j['titre']+" fait !")
    r=hist.insert_one({"Job":j['titre'],"date execution":datetime.now(),"erreur":""})

#########################################

for j in jobs:
  with st.expander(j['titre']):
    with st.container(border=False):
      if st.button("Executer ",key=j['_id']):
        executer(j)
      key = "Planification_form" + str(j['_id'])
      annuler = st.button("Annuler planification ",key=str(j['_id'])+"a")

      with st.form(key=key):
        date = st.date_input("Date")
        heure = st.time_input("Heure")
        options = ["chaque minute","chaque jour", "chaque semaine", "chaque mois"]
        choix = st.selectbox("Choisissez une option :", options)
        submit = st.form_submit_button("enregistrer " ,key=str(j['_id'])+"e")
        dt = datetime.combine(date, heure) 
       
        if submit:
          connect_mongo()
          collection.update_one({"titre":j['titre']},{ "$set":{ "d":dt, "frequence":choix}})
          if choix == "chaque minute":
            scheduler.add_job(executer, trigger="interval", minutes=1, start_date=dt, args=[j], id=str(j["_id"]), replace_existing=True)
          elif choix == "chaque jour":
            scheduler.add_job(executer, trigger="interval", days=1, start_date=dt, args=[j], id=str(j["_id"]), replace_existing=True)
          elif choix == "chaque semaine":
            scheduler.add_job(executer, trigger="interval", weeks=1, start_date=dt, args=[j], id=str(j["_id"]), replace_existing=True)
          elif choix == "chaque mois":
            scheduler.add_job(executer, trigger="cron", day=dt.day, hour=dt.hour, minute=dt.minute, args=[j], id=str(j["_id"]), replace_existing=True)
          st.success("Planification enregistrée !")

        if annuler:
            collection.update_one(
                {"titre": j['titre']},
                {"$set": {"d": "", "frequence": ""}}
            )
            st.success("Planification annulée !")
