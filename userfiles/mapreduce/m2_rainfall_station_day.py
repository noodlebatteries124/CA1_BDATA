#!/usr/bin/env python3
import csv
from datetime import datetime
from mrjob.job import MRJob
from mrjob.step import MRStep

EXPECTED_DAILY_SLOTS = 288
ELIGIBLE_MIN_SLOTS = 260  # 260 / 288 = 90.28% (>= 90% coverage threshold)
VALID_MINUTES = {0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55}


class M2RainfallStationDay(MRJob):

    # Parse each rainfall record, validate 5-minute slot alignment, and key by (station_id, date)
    def mapper_slots(self, _, line):
        try:
            row = next(csv.reader([line]))
        except Exception:
            return

        if not row or len(row) < 6:
            return

        ts_raw = row[0].strip().lstrip("\ufeff")
        if ts_raw in ("timestamp", "observed_at", ""):
            return

        station_id = row[1].strip()
        station_name = row[2].strip()
        if not station_id:
            return

        try:
            rainfall_mm = float(row[5].strip())
        except ValueError:
            return

        if rainfall_mm < 0:
            return

        try:
            dt = datetime.fromisoformat(ts_raw)
        except ValueError:
            return

        # Require aligned 5-minute slot timestamps
        if dt.minute not in VALID_MINUTES or dt.second != 0:
            return

        date_str = dt.strftime("%Y-%m-%d")
        slot_str = dt.strftime("%H:%M")

        yield (station_id, date_str), [slot_str, rainfall_mm, station_name]

    # Deduplicate 5-minute slots per (station_id, date), compute coverage, and split eligible vs partial days
    def reducer_station_day(self, key, values):
        station_id, date_str = key
        slot_readings = {}
        station_name = station_id

        for slot_str, rainfall_mm, s_name in values:
            if s_name:
                station_name = s_name
            # Collapse repeated observations within the same 5-minute slot without summing duplicates
            slot_readings[slot_str] = rainfall_mm

        valid_slots = len(slot_readings)
        total_rainfall_mm = round(sum(slot_readings.values()), 2)
        coverage_pct = round((valid_slots / EXPECTED_DAILY_SLOTS) * 100.0, 2)

        record = {
            "station_id": station_id,
            "station_name": station_name,
            "date": date_str,
            "total_rainfall_mm": total_rainfall_mm,
            "valid_unique_slots": valid_slots,
            "expected_slots": EXPECTED_DAILY_SLOTS,
            "missing_slots": EXPECTED_DAILY_SLOTS - valid_slots,
            "coverage_pct": coverage_pct
        }

        if valid_slots >= ELIGIBLE_MIN_SLOTS:
            yield "ELIGIBLE", record
        else:
            yield "PARTIAL", record

    # Rank Top 10 eligible station-days by rainfall and retain partial days (<90% coverage) separately
    def reducer_rank_and_separate(self, status_key, records):
        items = list(records)

        if status_key == "ELIGIBLE":
            # Sort descending by total_rainfall_mm, then valid_unique_slots, then ascending by station_id & date
            items.sort(
                key=lambda r: (
                    -r["total_rainfall_mm"],
                    -r["valid_unique_slots"],
                    r["station_id"],
                    r["date"]
                )
            )

            for rank_idx, rec in enumerate(items[:10], start=1):
                rec_out = dict(rec)
                rec_out["rank"] = rank_idx
                yield f"TOP_10_ELIGIBLE|RANK_{rank_idx:02d}", rec_out

            yield "ELIGIBLE_SUMMARY", {
                "eligible_station_days_count": len(items),
                "coverage_threshold_pct": 90.0,
                "minimum_slots_required": ELIGIBLE_MIN_SLOTS
            }
        else:
            # Retain partial days (<90% coverage) in a separate result sorted by coverage ascending, then date & station
            items.sort(key=lambda r: (r["valid_unique_slots"], r["date"], r["station_id"]))
            yield "PARTIAL_SUMMARY", {
                "partial_station_days_count": len(items),
                "note": "Station-days with >0 observed slots but <90% (<260/288) coverage; excluded from Top 10 ranking"
            }
            for rec in items:
                yield f"PARTIAL_DAY|{rec['date']}|{rec['station_id']}", rec

    # Define the two-step MapReduce pipeline
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_slots,
                reducer=self.reducer_station_day
            ),
            MRStep(
                reducer=self.reducer_rank_and_separate
            )
        ]


if __name__ == "__main__":
    M2RainfallStationDay.run()
