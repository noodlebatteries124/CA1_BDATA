#!/usr/bin/env python3
import csv
from pathlib import Path
from mrjob.job import MRJob
from mrjob.step import MRStep

AM_PEAK_HOURS = {6, 7, 8, 9}
PM_PEAK_HOURS = {17, 18, 19, 20}


# Self-Generated Question M5:
# Bidirectional Bus Corridor AM vs. PM Peak Tidal Asymmetry & Reversal Analysis
# Business Justification:
# Bus operations planners scheduling fleet turnarounds and deadhead (empty) repositioning trips need
# to identify stop pairs {Stop A, Stop B} that exhibit strong tidal commuter reversal — heavily loaded
# from A -> B during the AM peak (06:00-09:59) and reversing from B -> A during the PM peak (17:00-20:59).
# A simple sum of a single column cannot detect bidirectional reversal because (A -> B) and (B -> A)
# are separate directed records across different hours. This 2-step MapReduce job pairs opposite
# directions under a canonical undirected key (min_stop, max_stop), computes each pair's AM/PM
# directional flow shares and Tidal Reversal Index, joins bus_stops.csv twice, and ranks the top 10
# tidal corridors per month and day type.
class M5BusDirectionalAsymmetry(MRJob):

    # Register bus_stops.csv for stop description lookup
    def configure_args(self):
        super().configure_args()
        self.add_file_arg(
            "--bus-stops",
            default="bus_stops_clean.csv",
            help="Path to cleaned bus_stops_clean.csv metadata file"
        )
        self.add_passthru_arg(
            "--min-peak-trips",
            type=int,
            default=2000,
            help="Minimum combined AM+PM peak trips required for a bidirectional pair to be ranked"
        )

    # Map directed peak-hour OD records onto canonical undirected pair (stop_a, stop_b)
    def mapper_peak_pairs(self, _, line):
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

        if not all([year_month, day_type, origin, destination]) or origin == destination:
            return
        if pt_type != "BUS":
            return

        try:
            hour = int(row[2].strip())
            total_trips = int(row[6].strip())
        except ValueError:
            return

        if total_trips < 0:
            return

        is_am = hour in AM_PEAK_HOURS
        is_pm = hour in PM_PEAK_HOURS
        if not (is_am or is_pm):
            return

        # Canonical undirected stop ordering so A->B and B->A meet in the same reducer key
        if origin < destination:
            stop_a, stop_b = origin, destination
            is_forward = True
        else:
            stop_a, stop_b = destination, origin
            is_forward = False

        am_fwd = total_trips if (is_am and is_forward) else 0
        am_rev = total_trips if (is_am and not is_forward) else 0
        pm_fwd = total_trips if (is_pm and is_forward) else 0
        pm_rev = total_trips if (is_pm and not is_forward) else 0

        yield (year_month, day_type, stop_a, stop_b), [am_fwd, am_rev, pm_fwd, pm_rev]

    # Combine peak directional counts locally per mapper
    def combiner_peak_pairs(self, key, values):
        am_fwd = am_rev = pm_fwd = pm_rev = 0
        for v0, v1, v2, v3 in values:
            am_fwd += v0
            am_rev += v1
            pm_fwd += v2
            pm_rev += v3
        yield key, [am_fwd, am_rev, pm_fwd, pm_rev]

    # Compute bidirectional AM/PM shares and Tidal Reversal Index for each undirected pair
    def reducer_tidal_metrics(self, key, values):
        year_month, day_type, stop_a, stop_b = key
        am_fwd = am_rev = pm_fwd = pm_rev = 0
        for v0, v1, v2, v3 in values:
            am_fwd += v0
            am_rev += v1
            pm_fwd += v2
            pm_rev += v3

        am_total = am_fwd + am_rev
        pm_total = pm_fwd + pm_rev
        combined_peak = am_total + pm_total

        if am_total == 0 or pm_total == 0 or combined_peak < self.options.min_peak_trips:
            return

        # Net tidal swing between AM flow balance (am_fwd - am_rev) and PM flow balance (pm_fwd - pm_rev)
        tidal_swing = abs((am_fwd - am_rev) - (pm_fwd - pm_rev))
        tidal_reversal_index = round(tidal_swing / float(combined_peak), 4)
        am_a_to_b_share = round(am_fwd / float(am_total), 4)
        pm_b_to_a_share = round(pm_rev / float(pm_total), 4)

        yield (year_month, day_type), [
            tidal_reversal_index,
            combined_peak,
            stop_a,
            stop_b,
            am_fwd,
            am_rev,
            pm_fwd,
            pm_rev,
            am_a_to_b_share,
            pm_b_to_a_share
        ]

    # Load bus_stops.csv metadata for double join in Step 2 reducer
    def reducer_init_stops(self):
        self.stop_lookup = {}
        stops_path = self.options.bus_stops
        if stops_path and Path(stops_path).exists():
            with open(stops_path, "r", encoding="utf-8-sig", newline="") as f:
                for row in csv.reader(f):
                    if not row or len(row) < 3:
                        continue
                    code = row[0].strip().lstrip("\ufeff")
                    if code in ("BusStopCode", "bus_stop_id", ""):
                        continue
                    self.stop_lookup[code] = (row[2].strip(), row[1].strip())

    # Rank Top 10 bidirectional corridors by combined peak trips among strong tidal pairs (or by tidal swing)
    def reducer_rank_tidal(self, group_key, records):
        year_month, day_type = group_key
        items = list(records)

        # Rank descending by tidal_reversal_index, then combined_peak, breaking ties by stop_a, stop_b
        items.sort(key=lambda x: (-x[0], -x[1], x[2], x[3]))

        for rank_idx, rec in enumerate(items[:10], start=1):
            (
                tidal_idx, combined_peak, stop_a, stop_b,
                am_fwd, am_rev, pm_fwd, pm_rev, am_share, pm_share
            ) = rec
            desc_a, road_a = self.stop_lookup.get(stop_a, ("UNMATCHED_STOP", "UNMATCHED_ROAD"))
            desc_b, road_b = self.stop_lookup.get(stop_b, ("UNMATCHED_STOP", "UNMATCHED_ROAD"))

            yield (
                f"{year_month}|{day_type}|RANK_{rank_idx:02d}",
                {
                    "year_month": year_month,
                    "day_type": day_type,
                    "rank": rank_idx,
                    "stop_a_id": stop_a,
                    "stop_a_description": desc_a,
                    "stop_a_road": road_a,
                    "stop_b_id": stop_b,
                    "stop_b_description": desc_b,
                    "stop_b_road": road_b,
                    "am_peak_a_to_b_trips": am_fwd,
                    "am_peak_b_to_a_trips": am_rev,
                    "pm_peak_a_to_b_trips": pm_fwd,
                    "pm_peak_b_to_a_trips": pm_rev,
                    "combined_peak_trips": combined_peak,
                    "am_a_to_b_share": am_share,
                    "pm_b_to_a_share": pm_share,
                    "tidal_reversal_index": tidal_idx
                }
            )

    # Define the two-step MapReduce pipeline
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_peak_pairs,
                combiner=self.combiner_peak_pairs,
                reducer=self.reducer_tidal_metrics
            ),
            MRStep(
                reducer_init=self.reducer_init_stops,
                reducer=self.reducer_rank_tidal
            )
        ]


if __name__ == "__main__":
    M5BusDirectionalAsymmetry.run()
