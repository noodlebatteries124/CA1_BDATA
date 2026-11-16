#!/usr/bin/env python3
import csv
from pathlib import Path
from mrjob.job import MRJob
from mrjob.step import MRStep


class M1BusCorridors(MRJob):

    # Register bus_stops.csv side-data file so YARN distributes it to reducers
    def configure_args(self):
        super().configure_args()
        self.add_file_arg(
            "--bus-stops",
            default="bus_stops_clean.csv",
            help="Path to cleaned bus_stops_clean.csv metadata file"
        )
        self.add_passthru_arg(
            "--top-n",
            type=int,
            default=10,
            help="Number of top directed corridors to rank per month and day_type"
        )

    # Parse each Bus OD record and emit ((year_month, day_type, origin, dest), total_trips)
    def mapper_trips(self, _, line):
        try:
            row = next(csv.reader([line]))
        except Exception:
            return

        if not row or len(row) != 7:
            return

        first_col = row[0].strip().lstrip("\ufeff")
        if first_col in ("YEAR_MONTH", "year_month"):
            return

        year_month = first_col
        day_type = row[1].strip()
        pt_type = row[3].strip()
        origin = row[4].strip()
        destination = row[5].strip()

        if not all([year_month, day_type, origin, destination]):
            return
        if pt_type != "BUS":
            return

        try:
            hour = int(row[2].strip())
            total_trips = int(row[6].strip())
        except ValueError:
            return

        if not (0 <= hour <= 23) or total_trips < 0:
            return

        # Exclude hour from key to sum total_trips across all hours of the day
        yield (year_month, day_type, origin, destination), total_trips

    # Partially sum total_trips per mapper to reduce shuffle volume across 24 hours
    def combiner_trips(self, key, values):
        yield key, sum(values)

    # Sum total_trips across all hours and re-key by (year_month, day_type) for ranking
    def reducer_trips(self, key, values):
        year_month, day_type, origin, destination = key
        yield (year_month, day_type), [sum(values), origin, destination]

    # Load bus_stops_clean.csv into memory once per reducer task for double metadata join
    def reducer_init_join_rank(self):
        self.stop_lookup = {}
        stops_path = self.options.bus_stops
        if stops_path and Path(stops_path).exists():
            with open(stops_path, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.reader(f)
                for row in reader:
                    if not row or len(row) < 3:
                        continue
                    code = row[0].strip().lstrip("\ufeff")
                    if code in ("BusStopCode", "bus_stop_id", ""):
                        continue
                    road_name = row[1].strip()
                    description = row[2].strip()
                    self.stop_lookup[code] = (description, road_name)

    # Join origin and destination stop metadata, disclose unmatched stops, and rank Top 10
    def reducer_rank_and_join(self, group_key, corridor_records):
        year_month, day_type = group_key
        corridors = []

        unmatched_origin_codes = set()
        unmatched_dest_codes = set()
        unmatched_corridor_count = 0
        unmatched_trip_total = 0

        for total_trips, origin, destination in corridor_records:
            orig_matched = origin in self.stop_lookup
            dest_matched = destination in self.stop_lookup

            if not orig_matched:
                unmatched_origin_codes.add(origin)
            if not dest_matched:
                unmatched_dest_codes.add(destination)
            if not (orig_matched and dest_matched):
                unmatched_corridor_count += 1
                unmatched_trip_total += total_trips

            corridors.append((total_trips, origin, destination, orig_matched, dest_matched))

        # Sort descending by total_trips, breaking ties ascending by origin ID then destination ID
        corridors.sort(key=lambda x: (-x[0], x[1], x[2]))

        top_n = self.options.top_n
        for rank_idx, (trips, origin, destination, orig_ok, dest_ok) in enumerate(corridors[:top_n], start=1):
            orig_desc, orig_road = self.stop_lookup.get(origin, ("UNMATCHED_STOP", "UNMATCHED_ROAD"))
            dest_desc, dest_road = self.stop_lookup.get(destination, ("UNMATCHED_STOP", "UNMATCHED_ROAD"))

            if orig_ok and dest_ok:
                match_status = "MATCHED"
            elif not orig_ok and not dest_ok:
                match_status = "UNMATCHED_BOTH"
            elif not orig_ok:
                match_status = "UNMATCHED_ORIGIN"
            else:
                match_status = "UNMATCHED_DESTINATION"

            yield (
                f"{year_month}|{day_type}|RANK_{rank_idx:02d}",
                {
                    "year_month": year_month,
                    "day_type": day_type,
                    "rank": rank_idx,
                    "origin_stop_id": origin,
                    "origin_description": orig_desc,
                    "origin_road": orig_road,
                    "destination_stop_id": destination,
                    "destination_description": dest_desc,
                    "destination_road": dest_road,
                    "total_trips": trips,
                    "metadata_status": match_status
                }
            )

        # Disclose unmatched stop statistics for this (year_month, day_type)
        yield (
            f"{year_month}|{day_type}|UNMATCHED_DISCLOSURE",
            {
                "year_month": year_month,
                "day_type": day_type,
                "total_directed_corridors": len(corridors),
                "unmatched_corridor_pairs": unmatched_corridor_count,
                "unmatched_corridor_trips": unmatched_trip_total,
                "unmatched_origin_stop_ids": sorted(unmatched_origin_codes),
                "unmatched_destination_stop_ids": sorted(unmatched_dest_codes)
            }
        )

    # Define the two-step MapReduce pipeline
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_trips,
                combiner=self.combiner_trips,
                reducer=self.reducer_trips
            ),
            MRStep(
                reducer_init=self.reducer_init_join_rank,
                reducer=self.reducer_rank_and_join
            )
        ]


if __name__ == "__main__":
    M1BusCorridors.run()
