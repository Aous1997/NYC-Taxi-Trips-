"""
Détection systématique des anomalies dans les données NYC Taxi.
"""
import dask.dataframe as dd
import pandas as pd
from datetime import datetime
from pathlib import Path


def detect_quality_issues(ddf: dd.DataFrame) -> dict:
    """ 
    Analyse complète de la qualité des données.
    Retourne un dict de stats sur tous les problèmes détectés.
    """
    total = len(ddf)
    issues = {}
    print(" [1/8] Doublons...")
    # 1. Doublons exacts
    nb_doublons = (ddf.groupby(list(ddf.columns)).size() > 1).sum().compute()
    issues['doublons'] = int(nb_doublons)
    
    # 2. Valeurs manquantes par colonne
    print(" [2/8] NaN par colonne...")
    nans = ddf.isna().sum().compute()
    issues['nans'] = nans[nans > 0].to_dict()
    
    # 3. Dates aberrantes
    print(" [3/8] Dates...")
    issues['dates'] = {
        'pickup_avant_2023': (ddf['tpep_pickup_datetime'] < '2023-01-01').sum().compute(),
        'pickup_apres_2024': (ddf['tpep_pickup_datetime'] > '2024-01-01').sum().compute(),
        'dropoff_avant_pickup': (
            ddf['tpep_dropoff_datetime'] < ddf['tpep_pickup_datetime']
        ).sum().compute()
    }
    
    # 4. Durées aberrantes  
    print(" [4/8] Durées...")
    # 🆕 Fix typo : tpep au lieu de tep
    duree = (ddf['tpep_dropoff_datetime'] - ddf['tpep_pickup_datetime']).dt.total_seconds()
    issues['durees'] = {
        'negatives': (duree < 0).sum().compute(),
        'zero': (duree == 0).sum().compute(),
        'sup_24h': (duree > 86400).sum().compute(),
    }
    
    # 5. Passagers aberrants
    print(" [5/8] Passagers...")
    issues['passagers'] = {
        'zero': (ddf['passenger_count'] == 0).sum().compute(),
        'sup_6': (ddf['passenger_count'] > 6).sum().compute(),
        'nan': ddf['passenger_count'].isna().sum().compute(),
    }
    
    # 6. Distances aberrantes
    print(" [6/8] Distances...")
    issues['distances'] = {
        'zero': (ddf['trip_distance'] == 0).sum().compute(),
        'negatives': (ddf['trip_distance'] < 0).sum().compute(),
        'sup_200km': (ddf['trip_distance'] > 124).sum().compute(),  # 124 miles ≈ 200km
    }
    
    # 7. Tarifs aberrants
    print(" [7/8] Tarifs...")
    issues['tarifs'] = {
        'negatifs': (ddf['fare_amount'] < 0).sum().compute(),
        'zero': (ddf['fare_amount'] == 0).sum().compute(),
        'inf_2_50': (ddf['fare_amount'] < 2.50).sum().compute(),
        'sup_1000': (ddf['fare_amount'] > 1000).sum().compute(),
    }
    
    # 8. Cohérence montants
    print(" [8/8] Cohérence...")
    issues['coherence'] = {
        'tip_neg': (ddf['tip_amount'] < 0).sum().compute(),
        'total_neg': (ddf['total_amount'] < 0).sum().compute(),
    }
    
    # Résumé
    issues['total_lignes'] = total
    issues['pct_problemes'] = sum_all_issues(issues) / total * 100
    return issues


def sum_all_issues(issues: dict) -> int:
    """Somme totale des lignes problématiques (avec recouvrement possible)."""
    s = issues.get('doublons', 0)
    for cat in ['dates', 'durees', 'passagers', 'distances', 'tarifs', 'coherence']:
        if cat in issues:
            s += sum(issues[cat].values())
    return s


def print_quality_report(issues: dict):
    """Affichage formaté du rapport qualité."""
    total = issues['total_lignes']
    print("\n" + "=" * 60)
    print(f" Rapport Qualité - {total:,} lignes")
    print("=" * 60)
    for category, data in issues.items():
        if category in ['total_lignes', 'pct_problemes']:
            continue
        if isinstance(data, dict):
            print(f"\n[{category.upper()}]")
            for key, val in data.items():
                pct = val / total * 100
                bar = "=" * int(pct * 2)
                print(f" {key:25s} {val:>10,}  {pct:5.2f}% {bar}")
        else:
            pct = data / total * 100
            print(f"\n{category:25s} {data:>10,} ({pct:.2f}%)")


if __name__ == "__main__":

    # Évite le KeyError 'airport_fee' qui existe dans certains fichiers mais pas d'autres
    COLONNES_COMMUNES = [
        'VendorID',
        'tpep_pickup_datetime',
        'tpep_dropoff_datetime',
        'passenger_count',
        'trip_distance',
        'RatecodeID',
        'store_and_fwd_flag',
        'PULocationID',
        'DOLocationID',
        'payment_type',
        'fare_amount',
        'extra',
        'mta_tax',
        'tip_amount',
        'tolls_amount',
        'improvement_surcharge',
        'total_amount',
        'congestion_surcharge',
    ]
    

    ddf = dd.read_parquet(
        "data/raw/yellow_tripdata_2023-*.parquet",
        columns=COLONNES_COMMUNES
    )
    
    issues = detect_quality_issues(ddf)
    print_quality_report(issues)
    
    # Sauvegarde le rapport
    import json
    Path("data/reports").mkdir(exist_ok=True, parents=True)
    with open("data/reports/quality_initial.json", "w") as f:
        json.dump(issues, f, indent=2, default=str)