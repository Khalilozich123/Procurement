# 📦 Pipeline Big Data de Réapprovisionnement (Procurement)

Ce projet implémente un pipeline de données complet (**"End-to-End"**) pour automatiser le réapprovisionnement d'une chaîne de supermarchés. Il simule la génération de données de ventes, leur stockage distribué sur un cluster Hadoop, et le calcul des besoins de commande via Trino, le tout orchestré par **Apache Airflow**.

## 🚀 Installation & Démarrage Rapide

Ce projet est **entièrement conteneurisé** avec Docker. Vous n'avez pas besoin d'installer Python, Java ou Hadoop sur votre machine.

### Prérequis
* **Docker Desktop** doit être installé et en cours d'exécution.

### Instructions de Lancement

Nous avons créé des scripts d'installation automatique pour simplifier le déploiement et la configuration de l'environnement.

**Pour Windows :**
1. Ouvrez le dossier du projet.
2. Double-cliquez sur le fichier **`installation.bat`**.
3. Une fenêtre s'ouvrira et installera tout automatiquement (construction de l'image Airflow personnalisée, démarrage des conteneurs Hadoop/Trino/Postgres, initialisation de la base de données).

> **Note :** Le premier lancement peut prendre quelques minutes le temps de construire l'image Docker d'Airflow avec les dépendances nécessaires.

### 🌐 Accès à l'Interface de Supervision (Airflow)

Une fois le démarrage terminé, le pipeline est pilotable via l'interface web d'Airflow :

* **URL :** [http://localhost:8081](http://localhost:8081)
* **Identifiant :** `admin`
* **Mot de passe :** `admin`

Vous y trouverez le DAG nommé **`supply_chain_pipeline`**. Activez-le (bouton "Unpause" à gauche) pour lancer l'orchestration des tâches.

## 🏗️ Architecture du Projet

Le pipeline suit une architecture Big Data moderne divisée en 5 couches :

1.  **Source de Données (PostgreSQL) :** Contient les données de référence ("Master Data") : Produits, Fournisseurs, Règles de stock (MOQ, Stock de sécurité).
2.  **Génération & Ingestion (Python) :** Des tâches Airflow simulent 5000 commandes quotidiennes (format JSON) et les téléversent dans le **Data Lake**.
3.  **Stockage Distribué (HDFS) :** Un cluster Hadoop avec **3 DataNodes** et un facteur de réplication de 3 assure la tolérance aux pannes et le stockage des données brutes.
4.  **Traitement Distribué (Trino) :** Moteur de requête SQL distribué qui joint les données brutes (JSON/CSV sur HDFS) avec les données de référence (PostgreSQL) "In-Memory" pour calculer les besoins nets.
5.  **Orchestration (Apache Airflow) :** Remplace les scripts séquentiels par un **DAG** (Directed Acyclic Graph). Airflow gère :
    * L'ordre d'exécution des tâches (Génération -> Ingestion -> Calcul).
    * Les reprises automatiques en cas d'échec (Retries).
    * L'historique et la centralisation des logs.

## 🛠️ Dépannage (Troubleshooting)

**Problème : Erreur "NameNode is in Safe Mode"**
* **Cause :** Le cluster Hadoop vient de démarrer et vérifie l'intégrité des blocs de données.
* **Solution :** Attendez 30 secondes ou forcez la sortie du mode sans échec avec la commande :
  ```bash
  docker exec namenode hdfs dfsadmin -safemode leave
