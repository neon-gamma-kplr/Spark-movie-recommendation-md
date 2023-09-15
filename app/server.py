
import time # pour la gestion du temps
import sys # pour accéder aux arguments de la ligne de commande
import cherrypy # pour créer le serveur web CherryPy
import os # pour effectuer des opérations sur le système d'exploitation
import cheroot.wsgi # pour le serveur WSGI CherryPy
import SparkContext # pour travailler avec Spark
import SparkConf # pour travailler avec Spark
import SparkSession # pour créer une session Spark
from app import create_app # pour créer l'application Flask


# Créez un objet `SparkConf`
conf = SparkConf().setAppName("movie_recommendation-server")
    
# Initialisez le contexte Spark
sc = SparkContext(conf=conf, pyFiles=['engine.py', 'app.py'])
    
# Obtenez les chemins des jeux de données des films et des évaluations à partir des arguments de la ligne de commande
movies_set_path = sys.argv[1] if len(sys.argv) > 1 else ""
ratings_set_path = sys.argv[2] if len(sys.argv) > 2 else ""
    
# Créez l'application Flask
app = create_app(sc, movies_set_path, ratings_set_path)

# Configurez et démarrez le serveur CherryPy
cherrypy.tree.graft(app.wsgi_app, '/')
cherrypy.config.update({'server.socket_host': '0.0.0.0', 'server.socket_port': 5432, 'engine.autoreload.on': False})
cherrypy.engine.start()