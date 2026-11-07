
import csv
from importlib.resources import path
import time
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

START = date(2026, 9, 1)
END = date(2026, 9, 30)
CARPARK_EVERY_HOURS = 1

OUTPUT = Path("CA1_Datasets")
OUTPUT.mkdir(exist_ok=True)

RAINFALL_URL = "https://api-open.data.gov.sg/v2/real-time/api/rainfall"
CARPARK_URL = "https://api.data.gov.sg/v1/transport/carpark-availability"

MAX_RETRIES = 8
SKIP_RAINFALL_IF_EXISTS = True

def get_json(url, params):
    retryable_statuses = {429, 500, 502, 503, 504}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                url,
                params=params,
                timeout=60
            )
            if response.status_code in retryable_statuses:

                if attempt == MAX_RETRIES:
                    response.raise_for_status()
                retry_after = response.headers.get("Retry-After")

                if retry_after:
                    try:
                        wait_time = float(retry_after)
                    except ValueError:
                        wait_time = min(5 * (2 ** (attempt - 1)), 60)
                else:
                    wait_time = min(5 * (2 ** (attempt - 1)), 60)
                print(
                    f"HTTP {response.status_code}. "
                    f"Retry {attempt}/{MAX_RETRIES} "
                    f"in {wait_time:.0f} seconds..."
                )
                time.sleep(wait_time)
                continue
            response.raise_for_status()
            return response.json()
        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError
        ) as error:
            if attempt == MAX_RETRIES:
                raise
            wait_time = min(5 * (2 ** (attempt - 1)), 60)
            print(
                f"Connection error: {error}. "
                f"Retry {attempt}/{MAX_RETRIES} "
                f"in {wait_time} seconds..."
            )
            time.sleep(wait_time)
    raise RuntimeError(f"Request failed: {url} {params}")

def days():

    day = START

    while day <= END:
        yield day
        day += timedelta(days=1)

def download_rainfall():
    path = OUTPUT / f"rainfall_{START:%Y%m%d}_{END:%Y%m%d}.csv"
    if SKIP_RAINFALL_IF_EXISTS and path.exists():
        print(f"Rainfall file already exists. Skipping: {path}")
        return
    with path.open("w", newline="", encoding="utf-8-sig") as file:
        writer = csv.writer(file)
        writer.writerow([
            "timestamp",
            "station_id",
            "station_name",
            "latitude",
            "longitude",
            "rainfall_mm"
        ])
        for day in days():
            params = {"date": day.isoformat()}
            while True:
                data = get_json(RAINFALL_URL, params)["data"]
                stations = {
                    s["id"]: s for s in data["stations"]
                }
                for reading in data["readings"]:
                    for item in reading["data"]:
                        station = stations[item["stationId"]]
                        writer.writerow([
                            reading["timestamp"],
                            item["stationId"],
                            station["name"],
                            station["location"]["latitude"],
                            station["location"]["longitude"],
                            item["value"],
                        ])
                token = data.get("paginationToken")

                if not token:
                    break
                params = {
                    "date": day.isoformat(),
                    "paginationToken": token
                }
            print(f"Rainfall done: {day}")
    print(f"Saved: {path}")

def download_carpark():
    path = OUTPUT / f"carpark_{START:%Y%m%d}_{END:%Y%m%d}.csv"
    progress_path = path.with_suffix(".progress")

    if path.exists() and not progress_path.exists():
        backup_path = path.with_name(
            path.stem + "_pre_resume_backup" + path.suffix
        )
        if backup_path.exists():
            raise FileExistsError(
                f"Backup already exists: {backup_path}. "
                "Rename it before running again."
            )
        path.rename(backup_path)
        print(f"Previous partial CSV backed up to: {backup_path}")

    completed = set()
    if progress_path.exists():
        with progress_path.open("r", encoding="utf-8") as file:
            completed = {
                line.strip()
                for line in file
                if line.strip()
            }
    print(f"Previously completed requests: {len(completed)}")
    existing_rows = set()
    if path.exists():
        with path.open("r", newline="", encoding="utf-8-sig") as file:
            reader = csv.DictReader(file)
            for row in reader:
                key = (
                    row["snapshot_time"],
                    row["carpark_number"],
                    row["lot_type"]
                )
                existing_rows.add(key)

    file_exists = path.exists()
    with path.open("a", newline="", encoding="utf-8-sig") as file:
        writer = csv.writer(file)
        if not file_exists:
            writer.writerow([
                "snapshot_time",
                "carpark_number",
                "lot_type",
                "lots_available",
                "total_lots",
                "update_datetime"
            ])
            file.flush()

        for day in days():
            for hour in range(0, 24, CARPARK_EVERY_HOURS):
                moment = datetime(
                    day.year,
                    day.month,
                    day.day,
                    hour,
                    1
                )
                request_time = moment.isoformat()

                if request_time in completed:
                    continue

                data = get_json(
                    CARPARK_URL,
                    {"date_time": request_time}
                )

                if not data["items"]:
                    print(f"No carpark data for {moment}")

                for item in data["items"]:
                    for carpark in item.get("carpark_data", []):
                        for lot in carpark["carpark_info"]:
                            key = (
                                item["timestamp"],
                                carpark["carpark_number"],
                                lot["lot_type"]
                            )
                            if key in existing_rows:
                                continue

                            writer.writerow([
                                item["timestamp"],
                                carpark["carpark_number"],
                                lot["lot_type"],
                                lot["lots_available"],
                                lot["total_lots"],
                                carpark["update_datetime"],
                            ])
                            existing_rows.add(key)

                file.flush()

                with progress_path.open("a", encoding="utf-8") as progress_file:
                    progress_file.write(request_time + "\n")

                completed.add(request_time)

                print(
                    f"Carpark completed: {request_time} "
                    f"({len(completed)} requests total)"
                )
            print(f"Carpark done: {day}")
    print(f"Saved: {path}")
    print(f"Progress saved: {progress_path}")

download_rainfall()
download_carpark()

print("Download completed.")
