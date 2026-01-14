#  Pipeline Big Data de Réapprovisionnement (Procurement)

Ce projet implémente un pipeline de données complet ("End-to-End") pour automatiser le réapprovisionnement d'une chaîne de supermarchés. Il simule la génération de données de ventes, leur stockage distribué sur un cluster Hadoop, et le calcul des besoins de commande via Trino.

##  Installation & Démarrage Rapide

Ce projet est **entièrement conteneurisé** avec Docker. Vous n'avez pas besoin d'installer Python, Java ou Hadoop sur votre machine.

### Prérequis
* **Docker Desktop** doit être installé et en cours d'exécution.

### Instructions de Lancement

Nous avons créé des scripts d'installation automatique pour simplifier le déploiement.

**Pour Windows :**
1. Ouvrez le dossier du projet.
2. Double-cliquez sur le fichier **`installation.bat`**.
3. Une fenêtre s'ouvrira et installera tout automatiquement (démarrage des conteneurs, peuplement de la base de données, configuration de Trino).

##  Architecture du Projet

Le pipeline suit une architecture Big Data moderne :

1. **Source de Données (PostgreSQL) :** Contient les données de référence ("Master Data") : Produits, Fournisseurs, Règles de stock (MOQ, Stock de sécurité).
2. **Génération & Ingestion (Python) :** Le script `generate_orders.py` simule 5000 commandes quotidiennes et les téléverse dans le **Data Lake**.
3. **Stockage Distribué (HDFS) :** Un cluster Hadoop avec **3 DataNodes** et un facteur de réplication de 3 assure la tolérance aux pannes.
4. **Traitement Distribué (Trino) :** Moteur de requête SQL distribué qui joint les données brutes (JSON/CSV sur HDFS) avec les données de référence (PostgreSQL).
5. **Orchestration (Docker) :** Un conteneur dédié (`scheduler`) automatise l'exécution du pipeline chaque jour à 22h00.

##  Dépannage (Troubleshooting)

**Problème :** Erreur "NameNode is in Safe Mode".
* **Solution :** Le cluster vient de démarrer et vérifie l'intégrité des blocs. Attendez 30 secondes ou forcez la sortie :
  `docker exec namenode hdfs dfsadmin -safemode leave`

**Problème :** Les scripts Python échouent avec "Connection refused".
* **Solution :** Assurez-vous d'avoir lancé le projet via `installion.bat` pour garantir que les conteneurs sont bien connectés au même réseau Docker.
