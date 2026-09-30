"""
Audit initial des données du projet Sport Data Solution.

Ce script permet de découvrir les deux fichiers Excel fournis :
- les données RH des salariés ;
- les informations relatives à leur pratique sportive.

À cette étape, aucune donnée n'est modifiée.
L'objectif est uniquement de comprendre la structure
et le contenu des fichiers sources.
"""

from pathlib import Path

import pandas as pd


# ---------------------------------------------------------
# 1. DÉFINITION DES CHEMINS
# ---------------------------------------------------------

# Le script se trouve dans le dossier "src".
# parent.parent permet donc de retrouver la racine du projet.
PROJECT_DIR = Path(__file__).resolve().parent.parent

# Construction des chemins vers les deux fichiers bruts.
RH_FILE = PROJECT_DIR / "data" / "Donnees_RH.xlsx"
SPORT_FILE = PROJECT_DIR / "data" / "Donnees_Sportive.xlsx"


# ---------------------------------------------------------
# 2. CHARGEMENT DES DONNÉES
# ---------------------------------------------------------

# Lecture du fichier contenant les informations RH.
df_rh = pd.read_excel(RH_FILE)

# Lecture du fichier contenant les informations
# sur la pratique sportive des salariés.
df_sport = pd.read_excel(SPORT_FILE)


# ---------------------------------------------------------
# 3. AUDIT DU FICHIER RH
# ---------------------------------------------------------

print("\n" + "=" * 60)
print("FICHIER RH")
print("=" * 60)

# shape retourne :
# - le nombre de lignes ;
# - le nombre de colonnes.
print(f"Nombre de lignes   : {df_rh.shape[0]}")
print(f"Nombre de colonnes : {df_rh.shape[1]}")

# Affichage du nom de chaque colonne.
print("\nColonnes disponibles :")
for colonne in df_rh.columns:
    print(f"- {colonne}")

# Affichage des cinq premières lignes pour observer
# concrètement la structure des données.
print("\n5 premières lignes :")
print(df_rh.head())

# Affichage des types détectés automatiquement par Pandas.
print("\nTypes de données :")
print(df_rh.dtypes)


# ---------------------------------------------------------
# 4. AUDIT DU FICHIER SPORT
# ---------------------------------------------------------

print("\n" + "=" * 60)
print("FICHIER SPORT")
print("=" * 60)

print(f"Nombre de lignes   : {df_sport.shape[0]}")
print(f"Nombre de colonnes : {df_sport.shape[1]}")

print("\nColonnes disponibles :")
for colonne in df_sport.columns:
    print(f"- {colonne}")

print("\n5 premières lignes :")
print(df_sport.head())

print("\nTypes de données :")
print(df_sport.dtypes)

# ---------------------------------------------------------
# 5. CONTRÔLE DE LA QUALITÉ DES DONNÉES
# ---------------------------------------------------------

print("\n" + "=" * 60)
print("CONTRÔLE QUALITÉ DES DONNÉES")
print("=" * 60)

# ---------------------------------------------------------
# 5.1 VALEURS MANQUANTES
# ---------------------------------------------------------

# isna() identifie les cellules vides.
# sum() permet ensuite de compter le nombre de valeurs
# manquantes pour chaque colonne.
print("\nValeurs manquantes - fichier RH :")
print(df_rh.isna().sum())

print("\nValeurs manquantes - fichier Sport :")
print(df_sport.isna().sum())


# ---------------------------------------------------------
# 5.2 DOUBLONS
# ---------------------------------------------------------

# duplicated() permet d'identifier les lignes strictement
# identiques présentes plusieurs fois dans un fichier.
print("\nNombre de lignes dupliquées :")

doublons_rh = df_rh.duplicated().sum()
doublons_sport = df_sport.duplicated().sum()

print(f"- Fichier RH    : {doublons_rh}")
print(f"- Fichier Sport : {doublons_sport}")


# ---------------------------------------------------------
# 5.3 UNICITÉ DES IDENTIFIANTS SALARIÉS
# ---------------------------------------------------------

# Chaque salarié doit normalement être identifié
# de manière unique par la colonne "ID salarié".
# On vérifie donc qu'un même identifiant n'apparaît
# pas plusieurs fois dans chaque fichier.
print("\nID salariés dupliqués :")

id_dupliques_rh = df_rh["ID salarié"].duplicated().sum()
id_dupliques_sport = df_sport["ID salarié"].duplicated().sum()

print(f"- Fichier RH    : {id_dupliques_rh}")
print(f"- Fichier Sport : {id_dupliques_sport}")


# ---------------------------------------------------------
# 5.4 COHÉRENCE ENTRE LES DEUX FICHIERS
# ---------------------------------------------------------

# Création d'ensembles contenant les identifiants salariés.
# Un set permet de comparer facilement les ID présents
# dans les deux sources.
ids_rh = set(df_rh["ID salarié"])
ids_sport = set(df_sport["ID salarié"])

# Recherche des salariés présents dans le fichier RH
# mais absents du fichier Sport.
ids_absents_sport = ids_rh - ids_sport

# Recherche des salariés présents dans le fichier Sport
# mais absents du fichier RH.
ids_absents_rh = ids_sport - ids_rh

print("\nCohérence des ID entre les deux fichiers :")
print(f"- ID présents dans RH mais absents de Sport : {len(ids_absents_sport)}")
print(f"- ID présents dans Sport mais absents de RH : {len(ids_absents_rh)}")

# Si des incohérences existent, on affiche les identifiants
# concernés afin de pouvoir les analyser.
if ids_absents_sport:
    print(f"  ID concernés : {sorted(ids_absents_sport)}")

if ids_absents_rh:
    print(f"  ID concernés : {sorted(ids_absents_rh)}")


# ---------------------------------------------------------
# 5.5 VALEURS DES VARIABLES MÉTIER IMPORTANTES
# ---------------------------------------------------------

# value_counts() permet d'observer les différentes modalités
# présentes et leur fréquence.
#
# Ces informations seront importantes plus tard pour appliquer
# les règles métier du projet.

print("\nRépartition des moyens de déplacement :")
print(df_rh["Moyen de déplacement"].value_counts(dropna=False))

print("\nRépartition des pratiques sportives :")
print(df_sport["Pratique d'un sport"].value_counts(dropna=False))

print("\nRépartition des types de contrat :")
print(df_rh["Type de contrat"].value_counts(dropna=False))

print("\nRépartition des BU :")
print(df_rh["BU"].value_counts(dropna=False))


# ---------------------------------------------------------
# 6. FIN DE L'AUDIT
# ---------------------------------------------------------

print("\n" + "=" * 60)
print("AUDIT INITIAL ET CONTRÔLE QUALITÉ TERMINÉS")
print("=" * 60)
