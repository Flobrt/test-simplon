import mysql.connector
import requests
import pandas as pd
from io import StringIO


# Connexion à la base de données MariaDB
mydb = mysql.connector.connect(
    host="192.168.1.13",
    user="root",
    password="root",
    port="3306"
)

# Création d'un curseur
mycursor = mydb.cursor()

# Création base de données 
mycursor.execute("CREATE DATABASE IF NOT EXISTS pme_database;")

# Création des tables avec les liaisons 
mycursor.execute("USE pme_database;")
mycursor.execute("CREATE TABLE IF NOT EXISTS magasin (id_magasin VARCHAR(10) PRIMARY KEY, ville VARCHAR(50), nombre_salarie INT);")
mycursor.execute("CREATE TABLE IF NOT EXISTS produit (id_produit VARCHAR(10) PRIMARY KEY, nom VARCHAR(50), prix FLOAT, stock INT);")
mycursor.execute("CREATE TABLE IF NOT EXISTS vente (id_vente INT AUTO_INCREMENT PRIMARY KEY, id_magasin VARCHAR(10), id_produit VARCHAR(10), date DATE, quantite INT, FOREIGN KEY (id_magasin) REFERENCES magasin(id_magasin), FOREIGN KEY (id_produit) REFERENCES produit(id_produit));")
mycursor.execute("CREATE TABLE IF NOT EXISTS chiffre_affaires_total (date DATE PRIMARY KEY, total_ventes FLOAT);")
mycursor.execute('''
                    CREATE TABLE IF NOT EXISTS ventes_par_produit (
                        id_produit VARCHAR(10),
                        date DATE,
                        total_ventes_par_produit FLOAT,
                        PRIMARY KEY (id_produit, date),
                        FOREIGN KEY (id_produit) REFERENCES produit(id_produit)
                    );
                 ''')
mycursor.execute('''
                    CREATE TABLE IF NOT EXISTS ventes_par_region (
                        id_magasin VARCHAR(50),
                        date DATE,
                        total_ventes_par_ville FLOAT,
                        PRIMARY KEY (id_magasin, date),
                        FOREIGN KEY (id_magasin) REFERENCES magasin(id_magasin)
                    );
                ''')

# Suppression des données dans la table produit pour mettre à jour les stocks
mycursor.execute("TRUNCATE TABLE produit;")

# Insertion des données dans les tables
# Table produit 
url_produit = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSawI56WBC64foMT9pKCiY594fBZk9Lyj8_bxfgmq-8ck_jw1Z49qDeMatCWqBxehEVoM6U1zdYx73V/pub?gid=0&single=true&output=csv"

reponse_produit = requests.get(url_produit)

# Vérifier si la requête a réussi
if reponse_produit.status_code == 200:
    # Utiliser StringIO pour convertir les données binaires en un objet fichier-like que Pandas peut lire
    data = StringIO(reponse_produit.content.decode('utf-8'))
    # Créer un DataFrame à partir du contenu du CSV
    df_produit = pd.read_csv(data)
    # Insérer les données dans la table produit
    try:
        for index, row in df_produit.iterrows():
            mycursor.execute("INSERT INTO produit (id_produit, nom, prix, stock) VALUES (%s, %s, %s, %s);", (row['ID Référence produit'], row['Nom'], row['Prix'], row['Stock']))
    except mysql.connector.Error as e:
        print("Erreur lors de l'insertion des données dans la table produit:", e)
    
    mydb.commit()
else:
    print("Erreur lors du téléchargement du fichier. Code d'état HTTP:", reponse_produit.status_code)
    
    
# Table magasin 
url_magasin = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSawI56WBC64foMT9pKCiY594fBZk9Lyj8_bxfgmq-8ck_jw1Z49qDeMatCWqBxehEVoM6U1zdYx73V/pub?gid=714623615&single=true&output=csv"

reponse_magasin = requests.get(url_magasin)

if reponse_magasin.status_code == 200:
    data = StringIO(reponse_magasin.content.decode('utf-8'))
    df_magasin = pd.read_csv(data)
    try:
        for index, row in df_magasin.iterrows():
            mycursor.execute("INSERT INTO magasin (id_magasin, ville, nombre_salarie) VALUES (%s, %s, %s);", (row['ID Magasin'], row['Ville'], row['Nombre de salariés']))
    except mysql.connector.Error as e:
        print("Erreur lors de l'insertion des données dans la table magasin:", e)        
    
    mydb.commit()
else:
    print("Erreur lors du téléchargement du fichier. Code d'état HTTP:", reponse_produit.status_code)   
    
    
# Table vente

# Fonction pour vérifier si une entrée existe déjà
def vente_exists(id_magasin, id_produit, date):
    query = """
    SELECT EXISTS(
        SELECT 1 FROM vente WHERE id_magasin = %s AND id_produit = %s AND date = %s
    )
    """
    mycursor.execute(query, (id_magasin, id_produit, date))
    return mycursor.fetchone()[0]


url_vente = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSawI56WBC64foMT9pKCiY594fBZk9Lyj8_bxfgmq-8ck_jw1Z49qDeMatCWqBxehEVoM6U1zdYx73V/pub?gid=760830694&single=true&output=csv"

response_vente = requests.get(url_vente)

if response_vente.status_code == 200:
    data = StringIO(response_vente.content.decode('utf-8'))
    df_vente = pd.read_csv(data)
    
    for index, row in df_vente.iterrows():
        if not vente_exists(row['ID Magasin'], row['ID Référence produit'], row['Date']):
            insert_query = """
            INSERT INTO vente (id_magasin, id_produit, date, quantite)
            VALUES (%s, %s, %s, %s);
            """
            mycursor.execute(insert_query, (row['ID Magasin'], row['ID Référence produit'], row['Date'], row['Quantité']))
            print("Insertion réussie:", row)
        else:
            print("Entrée existante, pas d'insertion :", row)

    mydb.commit()
else:
    print("Erreur lors du téléchargement des données. HTTP Status Code:", response.status_code)


# Requetes sur la base de données 
# Question 1: Chiffre d'affaire totale ?
# rqt_1 = '''
#     SELECT 
#         SUM(p.prix * v.quantite) AS chiffre_affaire 
#     FROM produit p 
#     JOIN vente v ON p.id_produit = v.id_produit;
#     '''     
# mycursor.execute(rqt_1)
# result_1 = mycursor.fetchall()
# print("Chiffre d'affaire totale:", result_1[0][0])

mycursor.execute('''
                    INSERT INTO chiffre_affaires_total (date, total_ventes)
                    SELECT CURRENT_DATE, SUM(p.prix * v.quantite)
                    FROM vente v
                    JOIN produit p ON v.id_produit = p.id_produit
                    ON DUPLICATE KEY UPDATE total_ventes = VALUES(total_ventes);
                ''')
mydb.commit()

# Question 2: Nombre de vente par produit ?
# rqt_2 = '''
#     SELECT
#         p.nom AS nom_produit,
#         SUM(v.quantite) AS nombre_vente
#     FROM produit p
#     JOIN vente v ON p.id_produit = v.id_produit
#     GROUP BY p.nom;
#     '''
# mycursor.execute(rqt_2)
# result_2 = mycursor.fetchall()
# print("Nombre de vente par produit:")
# for row in result_2:
#     print(row[0], ":", row[1])

mycursor.execute('''
                    INSERT INTO ventes_par_produit (id_produit, date, total_ventes_par_produit)
                    SELECT v.id_produit, CURRENT_DATE, SUM(v.quantite)
                    FROM vente v
                    GROUP BY v.id_produit
                    ON DUPLICATE KEY UPDATE total_ventes_par_produit = VALUES(total_ventes_par_produit);
                ''')
mydb.commit()

# Question 3: Nombre de vente par région ?
# rqt_3 = '''
#     SELECT
#         m.ville AS region,
#         SUM(v.quantite) AS nombre_vente
#     FROM magasin m
#     JOIN vente v ON m.id_magasin = v.id_magasin
#     GROUP BY m.ville;
#     '''
# mycursor.execute(rqt_3)
# result_3 = mycursor.fetchall()
# print("Nombre de vente par région:")
# for row in result_3:
#     print(row[0], ":", row[1])
mycursor.execute('''
                    INSERT INTO ventes_par_region (id_magasin, date, total_ventes_par_ville)
                    SELECT m.id_magasin, CURRENT_DATE, SUM(v.quantite)
                    FROM vente v
                    JOIN magasin m ON v.id_magasin = m.id_magasin
                    GROUP BY m.id_magasin
                    ON DUPLICATE KEY UPDATE total_ventes_par_ville = VALUES(total_ventes_par_ville);
                ''')
mydb.commit()
    
# Fermeture de la connexion      
if mydb.is_connected():
        mycursor.close()
        mydb.close()
        print("Connexion MySQL est fermée")