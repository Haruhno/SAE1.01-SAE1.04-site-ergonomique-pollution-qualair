#####################################################################
# IMPORTATION DES MODULES
#####################################################################

from flask import Flask, render_template, request
import sqlite3
import os
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
from io import BytesIO
import base64


#####################################################################
# CONFIGURATION
#####################################################################

# Déclaration d'application Flask
app = Flask(__name__)

# Assure la compatibilité de Matplotlib avec Flask
matplotlib.use('Agg')

# Configuration pour servir les fichiers statiques
app.static_folder = 'static'

# Nom de la base de données SQLite
DATABASE = "QUALAIR.db"

# Chemin relatif vers la base de données
db_path = os.path.join(os.path.dirname(__file__), 'data', DATABASE)


#####################################################################
# FONCTIONS UTILES
#####################################################################

# Fonction pour se connecter à la base de données
def connect_db():
    # Connection à la base
    return sqlite3.connect(db_path)

#Fonction pour avoir les données des tables
def get_data_db(table_name):
    #Connection à la base
    conn = connect_db()

    if table_name not in ["Site", "Zas", "Organisme", "Polluant", "Mesure"]:
        raise ValueError("Nom de Table non validée")

    #obtention des données
    query = f"SELECT * FROM {table_name} LIMIT 50000"

    #Utilisation de pandas pour exécuter la requête SQL
    data = pd.read_sql_query(query, conn)

    #Fermerture de la connexion à la base de données
    conn.close()

    return data

def get_all_zas():
    #Connection à la base
    conn = connect_db()

    #Obtention des données
    query = "SELECT code_zas, Zas FROM Zas ORDER BY Zas"

    #Exécution de la réquête
    cur = conn.cursor()
    cur.execute(query)
    zas_list = cur.fetchall()

    #Fermeture de la connexion
    cur.close()
    conn.close()

    return zas_list


def requete_user(name_zas):
    # Connexion à la base
    conn = connect_db()

    print("Paramètre name_zas:", name_zas)

    # Création de la requête SQL
    query = """
SELECT 
    Z.Zas, 
    S.nom_site, 
    strftime('%Y-%m-%d', M.date_debut) AS Date_debut, 
    strftime('%Y-%m-%d',M.date_fin) AS Date_fin, 
    P.nom_polluant,
    M.valeur, 
    M.valeur_brute, 
    M.unite_mesure, 
    M.code_qualite
FROM 
    Site S
INNER JOIN 
    Zas Z ON S.id_zas = Z.id_zas
INNER JOIN 
    Mesure M ON S.id_site = M.id_site
INNER JOIN 
    Polluant P ON M.id_polluant = P.id_polluant
WHERE 
    Z.code_zas = ?
ORDER BY 
    M.valeur ASC
LIMIT 10000;
"""

    params = [name_zas]

    try:
        cur = conn.cursor()
        cur.execute(query, params)
        results = [dict(zip([column[0] for column in cur.description], row)) for row in cur.fetchall()]
        print("Résultats bruts de fetchall:", results)
    except Exception as e:
        print("Une erreur est survenue lors de l'exécution de la requête : ", e)
    finally:
        cur.close()
        conn.close()

    return results

def get_id_zas():
    # Connection à la base
    conn = connect_db()

    # Obtention des données
    query = "SELECT id_zas,code_zas, Zas FROM Zas ORDER BY Zas"

    # Exécution de la requête
    cur = conn.cursor()
    cur.execute(query)
    zas_list = cur.fetchall()

    # Fermeture de la connexion
    cur.close()
    conn.close()
    return zas_list

def get_polluant_moyenne(selected_zas_id=None, start_date='2023/01/01 00:00:00', end_date='2023/01/31 00:00:00'):
    conn = connect_db()

    if selected_zas_id:
        # Obtention des moyennes des polluants pour un ZAS spécifique
        query = """SELECT Polluant.nom_polluant as Polluant, AVG(valeur) as 'Moyenne' FROM Mesure
                   INNER JOIN Site ON Mesure.id_site = Site.id_site
                   INNER JOIN Zas ON Zas.id_zas = Site.id_zas
                   INNER JOIN Polluant ON Polluant.id_polluant = Mesure.id_polluant
                   WHERE Zas.id_zas = ? AND date_debut BETWEEN ? AND ?
                   GROUP BY Polluant.nom_polluant
                   ORDER BY Moyenne
                """
        params = (selected_zas_id, start_date, end_date)
    else:
        # Obtention des moyennes des polluants sans filtre sur le ZAS
        query = """SELECT Polluant.nom_polluant as Polluant, AVG(valeur) as 'Moyenne' FROM Mesure
                   INNER JOIN Polluant ON Polluant.id_polluant = Mesure.id_polluant
                   WHERE date_debut BETWEEN ? AND ?
                   GROUP BY Polluant.nom_polluant
                   ORDER BY Moyenne
                """
        params = (start_date, end_date)

    # Utilisation de pandas pour exécuter la requête SQL
    pollutant_moyenne = pd.read_sql_query(query, conn, params=params)

    # Fermeture de la connexion à la base de données
    conn.close()

    # Ajoutez ces impressions pour voir les résultats dans la console Flask
    print(f"Query: {query}")
    print(f"Params: {params}")
    print(f"Results:\n{pollutant_moyenne}")

    return pollutant_moyenne


# Fonction pour générer l'histogramme des moyennes et le convertir en image
def generate_histogram(selected_zas=None, start_date=1, end_date=31):

     # Obtention de la date de début (1er janvier 2023) avec le jour choisi
    start_date = f'2023/01/{start_date:02d} 00:00:00'

    # Obtention de la date de fin (même mois) avec le jour choisi
    end_date = f'2023/01/{end_date:02d} 23:59:59'
    # Récupération des résultats de la requête
    pollutant_moyenne = get_polluant_moyenne(selected_zas, start_date=start_date, end_date=end_date)

    # Génération de l'histogramme avec Matplotlib
    plt.figure(figsize=(10, 6))
    plt.bar(pollutant_moyenne['Polluant'], pollutant_moyenne['Moyenne'], color='blue')
    plt.xlabel('Polluants')
    plt.ylabel('Moyenne')
    plt.title('Moyenne des polluants du')
    plt.xticks(rotation=45, ha='right')  # Rotation des étiquettes pour une meilleure lisibilité
    plt.grid(axis='y', alpha=0.75)

    # Convertion de l'histogramme en image
    image_stream = BytesIO()
    plt.savefig(image_stream, format='png', bbox_inches='tight')
    plt.close()

    # Convertion de l'image en format base64 pour l'inclure dans le template
    image_base64 = base64.b64encode(image_stream.getvalue()).decode('utf-8')
    return f'data:image/png;base64,{image_base64}'



# Fonction pour obtenir les données sur les mesures de polluants depuis la base de données
def get_pollutant_data():
    conn = connect_db()

    # Obtention des données sur les mesures de polluants
    query = """SELECT Mesure.date_debut, Mesure.valeur, Polluant.nom_polluant
               FROM Mesure
               INNER JOIN Site ON Mesure.id_site = Site.id_site
               INNER JOIN Zas ON Zas.id_zas = Site.id_zas
               INNER JOIN Polluant ON Polluant.id_polluant = Mesure.id_polluant
               WHERE date_debut BETWEEN "2023/01/01 00:00:00" AND "2023/01/02 00:00:00"
               ORDER BY Mesure.date_debut
            """

    # Utilisation de pandas pour exécuter la requête SQL
    pollutant_data = pd.read_sql_query(query, conn)

    # Fermeture de la connexion à la base de données
    conn.close()

    return pollutant_data

# Fonction pour générer les boîtes à moustaches pour les polluants et le convertir en image
def generate_boxplot_polluants(df):
    # Générer un graphique à boîtes à moustaches avec Matplotlib
    plt.figure(figsize=(8, 6))
    boxplot = df.boxplot(column='valeur', by='nom_polluant', vert=False)
    plt.title('Distribution des valeurs par polluant')
    plt.xlabel('Valeur')
    plt.ylabel('Polluant')
    plt.subplots_adjust(left=0.15, right=0.9, top=0.9, bottom=0.2)

    # Convertir le graphique en image
    image_stream = BytesIO()
    plt.savefig(image_stream, format='png')
    plt.close()

    # Convertir l'image en format base64 pour l'inclure dans le template
    image_base64 = base64.b64encode(image_stream.getvalue()).decode('utf-8')

    return f'data:image/png;base64,{image_base64}'


#####################################################################
# ROUTES VERS LES VUES
#####################################################################

# Route pour la page d'accueil
@app.route('/')
def accueil():
    # Affichage du template
    return render_template('index.html')

# Route pour la page d'à propos
@app.route('/apropos')
def apropos():
    # Affichage du template
    return render_template('apropos.html')


#Route pour afficher le contenu de chaque table
@app.route('/afficher_tables', methods=['GET','POST'])
def afficher_tables():
    column_config = {
        'Site':['id_site', 'nom_site', 'code_site', 'type_implant', 'id_zas'],
        'Zas':['id_zas', 'code_zas', 'Zas'],
        'Organisme':['id_organisme', 'nom_organisme', 'id_zas'],
        'Polluant':['id_polluant', 'nom_polluant'],
        'Mesure':['id_mesure', 'id_site', 'id_polluant', 'date_debut', 'date_fin', 'valeur', 'valeur_brute', 'unite_mesure', 'code_qualite']
    }
    if request.method == 'POST' :
        table_name = request.form['table_name']

        #obtention des données d'une table
        data = get_data_db(table_name)

        #affichage du template avec les resultats
        return render_template('afficher_tables.html', 
                           table_data=data, table_name=table_name, column_config=column_config)
    else:
        empty_data = pd.DataFrame()
        return render_template('afficher_tables.html', table_data=empty_data, column_config=column_config)
    
#Route pour afficher la requête de l'utilisateur (date de début, date de fin, ZAS)
@app.route('/filtre', methods=['GET', 'POST'])
def filtre():
    results = []
    zas_option=get_all_zas()

    if request.method == 'POST':
        selected_zas=request.form.get('zas')
        print("Zas sélectionné à partir du formulaire:", selected_zas)
        try:
            results = requete_user(selected_zas)
        except Exception as e:
            print(f"Une erreur est survenue lors de l'exécution de la requête: {e}")
        print(f"Resultats: {results}")

    return render_template('filtre.html', results=results, zas_options=zas_option)


@app.route('/rechercher', methods=['POST'])
def rechercher():
    table_name = request.form.get('table_name')
    search_query = request.form.get('search_query')

    if search_query:
        # Utilisez un dictionnaire pour mapper les tables à leurs colonnes de recherche pertinentes
        table_columns_mapping = {
            'Site': ['nom_site'],
            'Zas': ['nom_zas'],
            'Organisme': ['nom_organisme'],
            'Polluant': ['nom_polluant'],
            'Mesure': ['id_mesure', 'id_site', 'id_polluant']
        }

        # Vérifiez si la table choisie est dans le dictionnaire
        if table_name in table_columns_mapping:
            columns_to_search = table_columns_mapping[table_name]
            
            # Construisez dynamiquement la requête SQL en fonction des colonnes de recherche
            query = f"SELECT * FROM {table_name} WHERE "
            query += " OR ".join([f"{column} LIKE ?" for column in columns_to_search])

            search_query = f"%{search_query}%"  # Ajoutez les jokers % pour la recherche partielle

            # Connexion à la base
            conn = connect_db()

            try:
                # Utilisation de pandas pour exécuter la requête SQL avec le paramètre de recherche
                data = pd.read_sql_query(query, conn, params=(search_query,) * len(columns_to_search))

                # Fermeture de la connexion à la base de données
                conn.close()

                # Convertissez les résultats en un format utilisable dans le template
                columns = [col for col in data.columns]
                results = [dict(zip(columns, row)) for row in data.itertuples(index=False)]
            except Exception as e:
                # Gestion des erreurs
                results = []
                error_message = str(e)
                print(f"Error: {error_message}")
        else:
            # Table non prise en charge
            results = []
            print(f"La table {table_name} n'est pas prise en charge pour la recherche.")

    else:
        results = []

    return render_template('resultats_recherche.html', table_name=table_name, results=results)


@app.route('/histogramme', methods=['GET', 'POST'])
def histogramme():
    # Obtention de la liste des ZAS
    zas_options = get_id_zas()

    selected_zas_id = None
    selected_zas_name = None
    selected_start_date = 1  # Valeur par défaut
    selected_end_date = 31   # Valeur par défaut

    if request.method == 'POST':
        # Récupération du ZAS sélectionné
        selected_zas_id = request.form.get('selected_zas')

        # Récupération des dates de début et de fin
        selected_start_date = int(request.form.get('start_date', 1))
        selected_end_date = int(request.form.get('end_date', 31))

        if selected_zas_id:
            # Obtenir le nom du ZAS directement à partir de zas_options
            selected_zas_name = next(zas_name for zas_id, zas_code, zas_name in zas_options if zas_id == int(selected_zas_id))

            # Obtention de l'image de l'histogramme pour le ZAS sélectionné et les dates spécifiées
            histogram_image = generate_histogram(selected_zas_id, start_date=selected_start_date, end_date=selected_end_date)

            # Affichage du template des résultats
            return render_template('histogramme.html', histogram_image=histogram_image, zas_options=zas_options, selected_zas_name=selected_zas_name, selected_start_date=selected_start_date, selected_end_date=selected_end_date)

    # Si la route est appelée avec une requête GET (chargement initial ou formulaire non valide)
    return render_template('histogramme.html', zas_options=zas_options, selected_start_date=selected_start_date, selected_end_date=selected_end_date)


# Route pour afficher les statistiques sur les polluants
@app.route('/statistiques')
def statistiques_polluants():
    # Obtention des données sur les mesures de polluants
    df = get_pollutant_data()

    # Convertir la colonne 'valeur' en type numérique
    df['valeur'] = pd.to_numeric(df['valeur'], errors='coerce')

    # Calculer les statistiques des valeurs par polluant à l'aide de Pandas
    stats_par_polluant = df.groupby('nom_polluant')['valeur'].agg(['mean', 'std', 'min', 'max']).round(2)


    # Génération de l'image des boîtes à moustaches pour les polluants
    boxplot_image = generate_boxplot_polluants(df)

    # Passer les statistiques et l'image au template
    return render_template('statistiques.html',
                           stats_par_polluant=stats_par_polluant,
                           img_bytes=boxplot_image)

#####################################################################
# POINT D'ENTREE DU PROGRAMME
#####################################################################

if __name__ == '__main__':
    app.run(debug=True)
