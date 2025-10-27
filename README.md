# real-time-election-tracking-project
Un système complet de traitement de votes en temps réel utilisant Apache Kafka, PostgreSQL, Spark Streaming et Streamlit pour la visualisation.
#  Architecture Globale
[Générateur Données] → [Kafka] → [Traitement Spark] → [Kafka] → [Dashboard Streamlit]
         ↓                      ↓                       ↓
   [PostgreSQL]          [Checkpoints]             [Visualisation]

# Stack Technologique Complète
Génération données : Python, Faker API 

Message Broker : Apache Kafka

Traitement temps réel : Apache Spark Streaming

Stockage persistant : PostgreSQL

Visualisation : Streamlit, Matplotlib

Orchestration : Python Scripts

# Composants du Système
1. Générateur de Données (main.py)
python
# Fonctionnalités principales
- Génération de données électorales réalistes
- Peuplement automatique de la base PostgreSQL
- Production de messages Kafka
- Gestion des transactions base de données
  
  Caractéristiques :
  
  Génération de 3 candidats avec photos et plateformes
  
  Création de 1000 électeurs avec données démographiques
  
  Intégration avec l'API RandomUser pour des données réalistes
  
  Production Kafka avec delivery reports
  
  2.  Processeur de Votes (voting.py)
  
  # Architecture Consumer/Producer
  consumer = Consumer()  # Lecture votes entrants
  producer = Producer()  # Émission votes traités
  Fonctionnalités :
  
  Consommation des votes depuis Kafka
  
  Attribution aléatoire aux candidats
  
  Stockage en base PostgreSQL
  
  Réémission vers topic de traitement
  
  3.  Traitement Spark Streaming (spark-streaming.py)
  # Pipeline de traitement
  votes_df = spark.readStream()  # Lecture Kafka
  enriched_votes_df = votes_df.withWatermark()  # Fenêtrage temporel
  aggregated_data = votes_df.groupBy().agg()  # Agrégations
  Transformations :
  
  Agrégation votes par candidat
  
  Calcul participation par localisation
  
  Fenêtrage temporel (1 minute)
  
  Émission vers topics Kafka de sortie

  4. Dashboard Temps Réel (streamlit.py)
  # Visualisations implémentées
  - Métriques principales (électeurs, candidats)
  - Graphique barres colorées
  - Camembert distribution votes
  - Tableau paginé participation géographique
  - Actualisation automatique
