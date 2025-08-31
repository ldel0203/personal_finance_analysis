# 📊 Financial Data Analysis  

Projet complet d’analyse de données financières personnelles. Ce projet a pour but de charger, stocker et analyser les données issues de différentes sources.

---

## 🚀 Objectifs du projet
- Automatiser l’**ETL** : extraction depuis plusieurs sources (CSV, OFX, Yahoo Finance, MySQL).  
- Nettoyer et transformer les données.  
- Charger les données dans une **base MySQL**.  
- Créer des **dashboards Power BI** interactifs pour analyser les finances (revenus, dépenses, tendances).  

---

## ⚙️ Technologies utilisées
- **Python** : pandas, yfinance, SQLAlchemy, ofxparse  
- **MySQL** : stockage des données structurées  
- **Power BI** : visualisation interactive  

---

## 🗂️ Structure du dépôt
```bash
├── dashboards/          # Rapports Power BI
│   └── dashboards.pdf   # Export des dashboards
│
├── etl/                 # ETL complet
│   ├── data/            # Données brutes et en attente
│   │   ├── archives/
│   │   ├── error/
│   │   └── to_process/
│   ├── extract/         # Scripts d’extraction (CSV, OFX, Yahoo Finance, MySQL)
│   ├── transform/       # Scripts de transformation
│   ├── load/            # Scripts de chargement MySQL
│   ├── pipelines/       # Pipelines ETL orchestrés
│   ├── logs.log         # Logs d’exécution
│   └── main.py          # Point d’entrée du pipeline global
│
├── sql/
│   ├── schema.sql       # Schéma de la base MySQL
│   └── views.py         # Vues SQL utiles pour Power BI
│
├── requirements.txt     # Dépendances Python
└── README.md            # Présentation du projet
```

---

## 📥 Installation & utilisation

### 🔧 Prérequis
- Python 3.10+
- MySQL (version 8+ recommandée)
- Power BI Desktop (pour ouvrir le fichier .pbix)

### 🔽 Installation

1. Cloner le dépôt :

```bash
git clone https://github.com/username/financial-data-analysis.git
cd financial-data-analysis
```

2. Installer les dépendances Python :
```bash
pip install -r requirements.txt
```

3. Créer la base MySQL avec le script :
```bash
mysql -u user -p < sql/schema.sql
```

4. Lancer le script ETL :
```bash
python etl/main.py
```

5. Explorer les dashboards Power BI disponibles dans /dashboards/.

---

## 📄 Licence

Projet publié sous licence MIT.
