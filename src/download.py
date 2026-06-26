"""
Télecharger les fichiers Parquet NYC Taxi deouis l'API officielle
"""
import requests
from pathlib import Path
from datetime import datetime
DATA_DIR = Path("./data/raw")
DATA_DIR.mkdir(parents=True, exist_ok=True)
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TAXI_TYPES = ["yellow", "green", "fhv"]
def download_mounth(taxi_type: str, year: int, month: int) -> Path:
    """Télécharge 1 fichier mensuel."""
    filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    url = f"{BASE_URL}/{filename}"
    output = DATA_DIR / filename
    if output.exists():
        print(f" [Skip] {filename} (déjà présent)")
        return output
    print(f" [DL] {filename}...")
    try:
        r = requests.get(url, stream=True, timeout=60)
        r.raise_for_status()
        with open(output, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
        size_mb = output.stat().st_size / 1024 / 1024
        print(f" [OK] {filename} ({size_mb:.1f} Mo)")
        return output
    except Exception as e:
        print(f" [ERR] {filename} : {e}")
        if output.exists():
            output.unlink()
        return None
def download_year(taxi_type: str = "yellow", year: int = 2023):
    """Télécharge une année compléte."""
    print(f"\n=== Téléchargement {taxi_type} {year} ===")
    files = []
    for month in range(1,13):
        f = download_mounth(taxi_type,year, month)
        if f:
            files.append(f)
    return files
if __name__ == "__main__" : 
    for mounth in [1,6,12]:
        download_mounth("yellow", 2023, mounth)
        # Pour une année de données :
        # download_year("yellow",2023)