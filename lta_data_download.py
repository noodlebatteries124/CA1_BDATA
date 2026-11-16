import os
import csv
import zipfile
import requests
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()  # reads .env into environment variables
LTA_API_KEY = os.getenv("LTA_API_KEY")
if not LTA_API_KEY:
    raise SystemExit("LTA_API_KEY not found. Check your .env file")


BASE_URL = "https://datamall2.mytransport.sg/ltaodataservice"

OUTPUT = Path(__file__).resolve().parent / "userfiles" / "CA1_Datasets"
OUTPUT.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "AccountKey": LTA_API_KEY,
    "accept": "application/json"
}


# Send GET request to LTA DataMall endpoint and return parsed JSON
def get_json(endpoint, params=None):
    response = requests.get(
        f"{BASE_URL}/{endpoint}",
        headers=HEADERS,
        params=params,
        timeout=60
    )
    response.raise_for_status()
    return response.json()


# 1. Download and unzip monthly Bus Origin-Destination ZIPs (Aug, Sep, Oct 2026)
months = ["202608", "202609", "202610"]

for month in months:
    zip_path = OUTPUT / f"bus_od_{month}.zip"
    csv_target = OUTPUT / f"origin_destination_bus_{month}.csv"

    if not zip_path.exists():
        data = get_json("PV/ODBus", {"Date": month})
        records = data.get("value", [])

        if not records or not records[0].get("Link"):
            raise RuntimeError(
                f"No download link returned for {month}: {data}"
            )

        download_url = records[0]["Link"]

        # Download immediately: link expires after 5 minutes
        response = requests.get(download_url, timeout=120)
        response.raise_for_status()

        zip_path.write_bytes(response.content)
        print(f"Downloaded: {zip_path}")
    else:
        print(f"Already exists, skipping download: {zip_path}")

    if not csv_target.exists():
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(OUTPUT)
        print(f"Extracted: {zip_path} -> {OUTPUT}/")

# 2. Download all Bus Stops (500 records per request) if not already saved
csv_path = OUTPUT / "bus_stops.csv"

if not csv_path.exists():
    bus_stops = []
    skip = 0

    while True:
        data = get_json("BusStops", {"$skip": skip})
        batch = data.get("value", [])

        if not batch:
            break

        bus_stops.extend(batch)
        print(f"Retrieved {len(bus_stops)} bus stops")

        if len(batch) < 500:
            break

        skip += 500

    # 3. Save Bus Stops as CSV
    columns = [
        "BusStopCode",
        "RoadName",
        "Description",
        "Latitude",
        "Longitude"
    ]

    with csv_path.open(
        "w", newline="", encoding="utf-8-sig"
    ) as file:
        writer = csv.DictWriter(
            file, fieldnames=columns, extrasaction="ignore"
        )
        writer.writeheader()
        writer.writerows(bus_stops)

    print(f"Saved: {csv_path}")
else:
    print(f"Bus stops file already exists, skipping: {csv_path}")

print("Download completed.")