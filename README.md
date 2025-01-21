# Construction et Analyse d'un Graphe de Protéines

## Aperçu du Projet
Ce projet vise à construire une représentation graphique des données sur les protéines, où :
- **Nœuds** : chaque nœud représente une protéine avec des attributs tels que la taxonomie, la fonction et la séquence.
- **Arêtes** : chaque arête représente la similarité entre deux protéines, calculée à l'aide de la similarité de Jaccard sur leurs domaines.

Le pipeline permet de traiter les données brutes, de construire le graphe, de l'intégrer dans Neo4j pour des requêtes et de visualiser la structure pour analyse.

## Étapes Principales
1. **Préparation des Données** : Chargement et prétraitement des données brutes en format TSV.
2. **Création du Graphe** : Construction des nœuds et des arêtes à partir des données traitées.
3. **Intégration dans Neo4j** : Importation des nœuds et arêtes dans une base Neo4j pour interrogation.
4. **Visualisation** : Génération d'une visualisation interactive pour explorer le graphe.

## Prérequis
- Python 3.8 ou supérieur
- Base de données Neo4j (version 5.0 ou supérieure)
- Java (requis pour PySpark)
- Docker (pour exécuter les conteneurs Spark et Kafka)

## Installation
1. **Cloner le Référentiel** :
    ```bash
    git clone https://github.com/Moahmednour/ProtienProja.git
    cd ProtienProja

    ```
2. **Configurer l'Environnement Virtuel** :
    ```bash
    python -m venv venv
    source venv/bin/activate   # Linux/macOS
    venv\Scripts\activate      # Windows
    ```
3. **Installer les Dépendances** :
    ```bash
    pip install -r requirements.txt
    ```
4. **Configurer les Conteneurs Docker** :
    - Démarrer le conteneur Docker pour Spark :
      ```bash
      docker run -d -v $PWD/ateliers:/home/jovyan/ateliers -e GRANT_SUDO=yes --user root  --name spark -p 4040:4040 -p 8888:8888 jupyter/all-spark-notebook:x86_64-ubuntu-22.04 start-notebook.sh --NotebookApp.token=''
      ```
    - Démarrer les autres conteneurs (par ex., Kafka, Neo4j) si nécessaire avec `docker-compose` :
      ```bash
      docker-compose up -d
      ```
    - Vérifiez que tous les conteneurs sont connectés au même réseau.

5. **Configurer Neo4j** :
    - Démarrez votre instance Neo4j.
    - Assurez-vous que la base supporte `LOAD CSV` en activant le paramètre `dbms.directories.import` dans `neo4j.conf`.

## Exécution
Pour exécuter le pipeline, suivez ces étapes :

1. **Préparer les Données d'Entrée** :
    - Placez vos données protéiques au format TSV dans `data/test.tsv`.
    - Assurez-vous que le fichier contient des en-têtes tels que `Entry`, `Sequence`, et `InterPro`.

2. **Lancer le Pipeline Principal** :
    ```bash
    python main.py
    ```
    
3. **Workflow du Pipeline** :
    - Charge et prétraite les données.
    - Construit les nœuds et arêtes du graphe.
    - Importe le graphe dans Neo4j.
    - Visualise la structure du graphe.

## Structure du Projet
```
.
├── data/                    # Fichiers de données bruts
├── output/                  # Nœuds, arêtes et visualisations générés
│   ├── nodes.csv            # Détails des nœuds
│   ├── edges.csv            # Détails des arêtes
│   └── graph.html           # Visualisation du graphe
├── src/                     # Modules du code source
│   ├── data_preprocessing.py
│   ├── graph_nodes.py
│   ├── graph_edges.py
│   ├── graph_visualization.py
│   ├── utils.py
│   └── validation.py
├── main.py                  # Script principal du pipeline
├── neo4j_integration.py     # Module d'intégration Neo4j
├── requirements.txt         # Dépendances
└── README.md                # Documentation
```

## Résultats
- **Nœuds et Arêtes** :
    - Les nœuds sont sauvegardés dans `output/nodes.csv`.
    - Les arêtes sont sauvegardées dans `output/edges.csv`.
- **Visualisation du Graphe** :
    - Un graphe interactif est disponible dans `output/graph.html`.
- **Importation Neo4j** :
    - Les nœuds et arêtes sont importés dans votre base Neo4j pour interrogation.
