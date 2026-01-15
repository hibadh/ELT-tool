import streamlit as st
import mysql.connector
import pandas as pd
import os
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
from pymongo import MongoClient
from datetime import datetime
import streamlit.components.v1 as components


st.set_page_config(page_title="Créer Job",layout="wide")

if st.button("Accueil"):
  st.switch_page("home.py")

st.header("Créer un Job")
uri = "mongodb://localhost:27017/"
client = MongoClient(uri)
db = client.app 
collection=db.test_job
hist=db.historique
df=pd.DataFrame()  


col1,col2= st.columns([3,1])
with col1:
    titre = st.text_input("Titre")
    path_doss = st.text_input("Chemin complet du dossier")
    dbName = st.text_input("Nom de la base")
    host = st.text_input("host")
    table = st.text_input("Nom de la table")    
    files=pd.DataFrame()    
with col2:
    st.title("Requetes SQL") 
    sql=st.text_area('Entrez vos requetes SQL (precedées par " - " ) ',height=315)

############## AJOUT JOB #################

if st.button("Ajouter Job"):   
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

###############################

if "tab" not in st.session_state:
    st.session_state.tab = None

############## EXECCUTER #################
def executer():
    if path_doss and dbName and table and titre and host :
        try:
            conn= mysql.connector.connect(host=host,user="root",password="",database=dbName)
            cursor=conn.cursor()   
            hist=db.historique                
            for f in os.listdir(path_doss):
                if f.endswith(".csv"):
                    df = pd.concat([df,pd.read_csv(os.path.join(path_doss, f))],ignore_index=True)            
            
        
            cursor.execute("CREATE TABLE IF NOT EXISTS  "+table+ "(id INT AUTO_INCREMENT PRIMARY KEY  ,nom VARCHAR(50),prenom VARCHAR (50));")                   
            for _, row in df.iterrows():
                cursor.execute("INSERT INTO "+table+" (nom, prenom) VALUES (%s, %s)",(row["nom"], row["prenom"]))
            req= [] 
            req = sql.split("-")
            req.pop(0)
            for r in req:
                cursor.execute(r)

            conn.commit()

            tab = pd.read_sql(f"SELECT * FROM {table}", conn)
            tab["select"] = False
            st.session_state.tab = tab

            st.success(titre+" fait !")
            
            r=hist.insert_one({"Job":titre,"date execution":datetime.now(),"erreur":""})
        except Exception as e:
            st.error(e.args)
            r=hist.insert_one({"Job":titre,"date execution":datetime.now(),"erreur":e.args})  
    else:
        st.warning("Veuillez remplir les champs correctement !") 
    st.session_state["df"]=df
    
if st.button("Executer"):
    #executer()
    st.markdown('<a href="/transform?run=1" target=_blank> Executer </a>',
            unsafe_allow_html=True)
    


#components.html("""<script>
 #               const_url= new URL(window.location.href);
  #              url.searchParams.set("run",executer);
   #             window.open(url.href,'_blank');</script>""",height=0)
    



#params=st.query_params
#if params.get("run")=="executer":
 #   executer()
###############################


###############################

if st.session_state.tab is not None:
    edited_tab = st.data_editor(
        st.session_state.tab,
        key="table_editor",
        hide_index=False
    )
    
    if not edited_tab.equals(st.session_state.tab):
        st.session_state.tab = edited_tab
        st.rerun()

############## SELECTION LIGNES #################

selected=pd.DataFrame()
if st.session_state.tab is not None:
    st.write("lignes selectionnées")
    selected = st.session_state.tab[st.session_state.tab["select"]]
st.write(selected)

############## SUPP LIGNES #################
if st.button('Supprimer lignes'):
    conn= mysql.connector.connect(host=host,user="root",password="",database=dbName)
    cursor=conn.cursor()  
    for _,s in selected.iterrows():
        cursor.execute( f"DELETE FROM {table} WHERE id = %s",(s["id"],))
    conn.commit()
    cursor.close()
    conn.close()

    st.success("Lignes supprimées avec succès")
