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
    #1. Doublons exacts
    nb_doublons = (ddf.groupby(list(ddf.columns)).size() > 1).sum().compute()
    issues['doublons'] = int(nb_doublons)
    
    #2. Valeurs manquantes par colonne
    
    print(" [2/8] NaN par colonne...")
    nans = ddf.isna().sum().compute()
    issues['nans'] = nans[nans > 0].to_dict()
    
    #3. Dates aberrantes
    
    print(" [3/8] Dates...")
    issues['dates'] = {
        'pickup_avant_2023':(ddf['tpep_pickup_datetime'] < '2023-01-01').sum().compute(),
        'pickup_apres_2024': (ddf['tpep_pickup_datetime'] > '2024-01-01').sum().compute(),
        'dropoff_avant_pickup' : (
            ddf['tpep_dropoff_datetime'] < ddf['tpep_pickup_datetime']
        ).sum().compute()
    }