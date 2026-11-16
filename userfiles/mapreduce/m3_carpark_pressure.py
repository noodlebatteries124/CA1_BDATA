#!/usr/bin/env python3
import csv
from datetime import datetime, timezone, timedelta
from mrjob.job import MRJob
from mrjob.step import MRStep

SG_TZ = timezone(timedelta(hours=8))


class M3CarparkPressure(MRJob):

    # Add optional top-n argument for summary ranking output alongside per-carpark-day results
    def configure_args(self):
        super().configure_args()
        self.add_passthru_arg(
            "--top-n",
            type=int,
            default=20,
            help="Number of highest-pressure carpark-days to highlight at the top of output"
        )

    # Filter to valid Lot Type C observations and emit ((carpark_id, sg_date), (obs_ts, available, capacity))
    def mapper_lot_c(self, _, line):
        try:
            row = next(csv.reader([line]))
        except Exception:
            return

        if not row or len(row) < 6:
            return

        ts_raw = row[0].strip().lstrip("\ufeff")
        if ts_raw in ("snapshot_time", "observed_at", ""):
            return

        carpark_id = row[1].strip()
        lot_type = row[2].strip()

        # Question M3 requires car lot type C only
        if not carpark_id or lot_type != "C":
            return

        try:
            lots_available = int(float(row[3].strip()))
            total_lots = int(float(row[4].strip()))
        except ValueError:
            return

        # Require total_lots > 0 and 0 <= lots_available <= total_lots
        if total_lots <= 0 or lots_available < 0 or lots_available > total_lots:
            return

        try:
            dt = datetime.fromisoformat(ts_raw)
            if dt.tzinfo is not None:
                dt = dt.astimezone(SG_TZ)
        except ValueError:
            return

        date_str = dt.strftime("%Y-%m-%d")
        obs_key = dt.strftime("%Y-%m-%dT%H:%M:%S")

        yield (carpark_id, date_str), [obs_key, lots_available, total_lots]

    # Deduplicate by timestamp per (carpark_id, date) and compute sample-weighted occupancy metrics
    def reducer_carpark_day(self, key, values):
        carpark_id, date_str = key
        unique_obs = {}

        for obs_key, lots_available, total_lots in values:
            # Retain latest observation if duplicate timestamps exist
            unique_obs[obs_key] = (lots_available, total_lots)

        obs_count = len(unique_obs)
        if obs_count == 0:
            return

        occ_Sum = 0.0
        max_occ = 0.0
        high_pressure_count = 0

        for lots_available, total_lots in unique_obs.values():
            occ = (total_lots - lots_available) / float(total_lots)
            occ_Sum += occ
            if occ > max_occ:
                max_occ = occ
            if occ >= 0.90:
                high_pressure_count += 1

        mean_occ = round(occ_Sum / obs_count, 4)
        max_occ_rounded = round(max_occ, 4)
        frac_ge_90 = round(high_pressure_count / float(obs_count), 4)

        record = {
            "carpark_id": carpark_id,
            "date": date_str,
            "observation_count": obs_count,
            "high_pressure_obs_count": high_pressure_count,
            "mean_occupancy_ratio": mean_occ,
            "max_occupancy_ratio": max_occ_rounded,
            "fraction_occupancy_ge_90pct": frac_ge_90
        }

        # Emit individual carpark-day result and also forward to TOP_RANKING reducer key
        yield f"CARPARK_DAY|{carpark_id}|{date_str}", record
        yield "SUMMARY_RANKING", record

    # Pass through individual carpark-day rows and emit Top N highest-pressure carpark-days
    def reducer_finalize(self, key, values):
        if key != "SUMMARY_RANKING":
            for val in values:
                yield key, val
            return

        all_days = list(values)
        all_days.sort(
            key=lambda r: (
                -r["fraction_occupancy_ge_90pct"],
                -r["mean_occupancy_ratio"],
                -r["max_occupancy_ratio"],
                r["carpark_id"],
                r["date"]
            )
        )

        top_n = self.options.top_n
        for rank_idx, rec in enumerate(all_days[:top_n], start=1):
            rec_out = dict(rec)
            rec_out["rank"] = rank_idx
            yield f"TOP_{top_n}_PRESSURE|RANK_{rank_idx:02d}", rec_out

    # Define the two-step MapReduce pipeline
    def steps(self):
        return [
            MRStep(
                mapper=self.mapper_lot_c,
                reducer=self.reducer_carpark_day
            ),
            MRStep(
                reducer=self.reducer_finalize
            )
        ]


if __name__ == "__main__":
    M3CarparkPressure.run()
