import json
from datetime import date, timedelta
from pathlib import Path
import requests
import sys

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

CITIES = {
    "casablanca": (33.57, -7.59),
    "rabat": (34.02, -6.84),
    "marrakech": (31.63, -8.01),
    "fes": (34.03, -5.00),
    "tangier": (35.76, -5.83),
}

BACKFILL_START = "2021-01-01"

def fetch_weather(url, latitude, longitude, start_date, end_date):
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m,precipitation",
        "timezone": "Africa/Casablanca",
    }
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    return response.json()

def save_raw(data, city, run_date, mode):
    folder = Path("data/raw") / f"date={run_date}"
    folder.mkdir(parents=True, exist_ok=True)
    file_path = folder / f"{city}_{mode}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f)
    return file_path

def get_date_range(mode):
    today = date.today()
    if mode == "backfill":
        start = BACKFILL_START
        end = (today - timedelta(days=7)).isoformat()
        return ARCHIVE_URL, start, end
    elif mode == "daily":
        start = (today - timedelta(days=7)).isoformat()
        end = (today - timedelta(days=1)).isoformat()
        return FORECAST_URL, start, end
    else:
        raise ValueError(f"Unknown mode: {mode}. Use 'backfill' or 'daily'.")


def run(mode):
    url, start_date, end_date = get_date_range(mode)
    run_date = date.today().isoformat()

    for city, (lat, lon) in CITIES.items():
        data = fetch_weather(url, lat, lon, start_date, end_date)
        path = save_raw(data, city, run_date, mode)
        print(f"{city}: {start_date} -> {end_date} saved to {path}")

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "daily"
    run(mode)