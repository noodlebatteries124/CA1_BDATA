#!/usr/bin/env python3
import csv
from datetime import datetime, timezone, timedelta
from mrjob.job import MRJob
from mrjob.step import MRStep

SG_TZ = timezone(timedelta(hours=8))
VALID_MINUTES = {0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55}


# Self-Generated Question M4:
# Time-Aligned Weather vs. Carpark Pressure Analysis (Reduce-Side Join on Singapore Date-Hour)
# Business Justification:
# Transport and parking operators managing HDB commercial/town-centre carparks need to know which
# carparks experience acute occupancy surges or high-pressure (>=90% full) spikes during rainy hours
# compared to dry hours (e.g., drivers sheltering or shifting from walking/micromobility to driving).
# This multi-step MapReduce job performs a time-aligned reduce-side join of 5-minute NEA station
# rainfall and hourly HDB Lot Type C carpark availability by Singapore clock hour (YYYY-MM-DD HH),
# classifies each hour as WET (mean station rainfall >= 0.5 mm or >= 10% of stations recording rain)
# vs. DRY, and ranks carparks by their wet-hour occupancy delta and high-pressure surge.
class M4WeatherParkingJoin(MRJob):

    # Read both rainfall_clean.csv and carpark_clean.csv and emit keyed by Singapore (date, hour)
    def mapper_hourly_align(self, _, line):
        try:
            row = next(csv.reader([line]))
        except Exception:
            return

        if not row or len(row) < 6:
            return

        first_col = row[0].strip().lstrip("\ufeff")
        if first_col in ("timestamp", "snapshot_time", "observed_at", ""):
            return

        # Distinguish rainfall_clean.csv vs carpark_clean.csv by column 2 (lot_type is 1 char 'C','H','S','Y')
        col2 = row[2].strip()

        # Case 1: carpark_clean.csv (snapshot_time, carpark_number, lot_type, lots_available, total_lots, update_datetime)
        if col2 in ("C", "H", "S", "Y", "M", "L"):
            if col2 != "C":
                return
            carpark_id = row[1].strip()
            if not carpark_id:
                return
            try:
                lots_available = int(float(row[3].strip()))
                total_lots = int(float(row[4].strip()))
                dt = datetime.fromisoformat(first_col)
                if dt.tzinfo is not None:
                    dt = dt.astimezone(SG_TZ)
            except ValueError:
                return

            if total_lots <= 0 or lots_available < 0 or lots_available > total_lots:
                return

            hour_key = dt.strftime("%Y-%m-%d %H:00")
            obs_ts = dt.strftime("%Y-%m-%dT%H:%M:%S")
            yield hour_key, ["PARK", carpark_id, obs_ts, lots_available, total_lots]
            return

        # Case 2: rainfall_clean.csv (timestamp, station_id, station_name, latitude, longitude, rainfall_mm)
        station_id = row[1].strip()
        if not station_id:
            return
        try:
            rainfall_mm = float(row[5].strip())
            dt = datetime.fromisoformat(first_col)
            if dt.tzinfo is not None:
                dt = dt.astimezone(SG_TZ)
        except ValueError:
            return

        if rainfall_mm < 0 or dt.minute not in VALID_MINUTES or dt.second != 0:
            return

        hour_key = dt.strftime("%Y-%m-%d %H:00")
        slot_str = dt.strftime("%H:%M")
        yield hour_key, ["RAIN", station_id, slot_str, rainfall_mm]

    # Join hourly station rainfall profile with carpark Lot Type C observations in the same clock hour
    def reducer_hourly_join(self, hour_key, records):
        station_slots = {}
        carpark_obs = {}

        for rec in records:
            tag = rec[0]
            if tag == "RAIN":
                _, station_id, slot_str, rainfall_mm = rec
                station_slots[(station_id, slot_str)] = rainfall_mm
            elif tag == "PARK":
                _, carpark_id, obs_ts, lots_available, total_lots = rec
                carpark_obs[(carpark_id, obs_ts)] = (lots_available, total_lots)

        if not station_slots or not carpark_obs:
            return

        # Aggregate hourly rainfall per station first (do not sum across stations into islandwide depth)
        station_hourly_rain = {}
        for (station_id, _), rain_mm in station_slots.items():
            station_hourly_rain[station_id] = station_hourly_rain.get(station_id, 0.0) + rain_mm

        num_stations = len(station_hourly_rain)
        rainy_stations = sum(1 for r in station_hourly_rain.values() if r >= 0.2)
        mean_station_rain = sum(station_hourly_rain.values()) / float(num_stations)
        rainy_station_share = rainy_stations / float(num_stations)

        # Classify hour as WET if >= 10% of stations record >= 0.2mm rain or mean station rain >= 0.5mm
        weather_state = "WET" if (rainy_station_share >= 0.10 or mean_station_rain >= 0.5) else "DRY"

        for (carpark_id, _), (lots_available, total_lots) in carpark_obs.items():
            occ = (total_lots - lots_available) / float(total_lots)
            high_press = 1 if occ >= 0.90 else 0
            yield carpark_id, [weather_state, occ, high_press, mean_station_rain]

    # Compute wet vs dry occupancy metrics per carpark and emit to global ranking step
    def reducer_carpark_comparison(self, carpark_id, values):
        wet_occ = []
        dry_occ = []
        wet_hp = 0
        dry_hp = 0

        for weather_state, occ, high_press, _ in values:
            if weather_state == "WET":
                wet_occ.append(occ)
                wet_hp += high_press
            else:
                dry_occ.append(occ)
                dry_hp += high_press

        # Require at least 5 wet-hour and 5 dry-hour observations for reliable comparison
        if len(wet_occ) < 5 or len(dry_occ) < 5:
            return

        wet_mean = sum(wet_occ) / float(len(wet_occ))
        dry_mean = sum(dry_occ) / float(len(dry_occ))
        occ_delta = wet_mean - dry_mean
        wet_hp_rate = wet_hp / float(len(wet_occ))
        dry_hp_rate = dry_hp / float(len(dry_occ))

        yield "RANK_ALL", {
            "carpark_id": carpark_id,
            "wet_hour_obs": len(wet_occ),
            "dry_hour_obs": len(dry_occ),
            "wet_mean_occupancy": round(wet_mean, 4),
            "dry_mean_occupancy": round(dry_mean, 4),
            "wet_minus_dry_occupancy_delta": round(occ_delta, 4),
            "wet_high_pressure_fraction": round(wet_hp_rate, 4),
            "dry_high_pressure_fraction": round(dry_hp_rate, 4)
        }

    # Rank Top 15 carparks with largest positive occupancy surge during wet hours
    def reducer_rank_top_sensitive(self, _, records):
        items = list(records)
        items.sort(
            key=lambda r: (
                -r["wet_minus_dry_occupancy_delta"],
                -r["wet_mean_occupancy"],
                r["carpark_id"]
            )
        )

        yield "M4_SUMMARY", {
            "total_evaluated_carparks": len(items),
            "mean_wet_occupancy_across_carparks": round(
                sum(r["wet_mean_occupancy"] for r in items) / float(len(items)), 4
            ) if items else 0.0,
            "mean_dry_occupancy_across_carparks": round(
                sum(r["dry_mean_occupancy"] for r in items) / float(len(items)), 4
            ) if items else 0.0
        }

        for rank_idx, rec in enumerate(items[:15], start=1):
            rec_out = dict(rec)
            rec_out["rank"] = rank_idx
            yield f"TOP_15_WET_SURGE|RANK_{rank_idx:02d}", rec_out

    # Define the three-step MapReduce pipeline (Reduce-Side Join -> Per-Carpark Delta -> Global Top 15)
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_hourly_align,
                reducer=self.reducer_hourly_join
            ),
            MRStep(
                reducer=self.reducer_carpark_comparison
            ),
            MRStep(
                reducer=self.reducer_rank_top_sensitive
            )
        ]


if __name__ == "__main__":
    M4WeatherParkingJoin.run()
