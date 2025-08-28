# 📊 Financial Data Analysis  

Projet complet d’analyse de données financières personnelles.  
L’objectif est de montrer mes compétences en **ETL (Python)**, **modélisation SQL** et **visualisation Power BI**.  

---

## 🚀 Objectifs du projet
- Automatiser l’**ETL** : extraction depuis plusieurs sources (CSV, OFX, Yahoo Finance, MySQL).  
- Nettoyer et transformer les données.  
- Charger les données dans une **base MySQL**.  
- Créer des **dashboards Power BI** interactifs pour analyser les finances (revenus, dépenses, tendances).  

---

## ⚙️ Technologies utilisées
- **Python** : pandas, yfinance, SQLAlchemy, etc.  
- **MySQL** : stockage des données structurées.  
- **Power BI** : visualisation interactive.  

---

## 🗂️ Structure du dépôt
```bash
├── dashboards/ # Rapports Power BI
│ ├── dashboard.pbix # Fichier Power BI
│ └── dashboards.pdf # Export des dashboards
│
├── etl/ # ETL complet
│ ├── data/ # Données brutes et en attente
│ │ ├── archives/
│ │ ├── error/
│ │ └── to_process/
│ ├── extract/ # Scripts d’extraction (CSV, OFX, Yahoo Finance, MySQL)
│ ├── transform/ # Scripts de transformation
│ ├── load/ # Scripts de chargement MySQL
│ ├── pipelines/ # Pipelines ETL orchestrés
│ ├── logs.log # Logs d’exécution
│ └── main.py # Point d’entrée du pipeline global
│
├── sql/
│ ├── schema.sql # Schéma de la base MySQL
│ └── views.py # Vues SQL utiles pour Power BI
│
└── README.md # Présentation du projet
```
---

## 📥 Installation & utilisation
1. Cloner le dépôt :  
   ```bash
   git clone https://github.com/username/financial-data-analysis.git

2. Créer la base MySQL avec le script dans /sql/.

3. Lancer le script ETL :
  python etl/main.py

4. Explorer les dashboards Power BI disponibles dans /dashboards/.
