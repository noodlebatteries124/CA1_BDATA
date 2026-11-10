import csv
from mrjob.job import MRJob


class BusODAggregation(MRJob):

    def mapper(self, _, line):
        try:
            row = next(csv.reader([line]))

            # Skip CSV header
            if row and row[0].strip() == "YEAR_MONTH":
                return

            if len(row) != 7:
                return

            year_month = row[0].strip()
            day_type = row[1].strip()
            hour = int(row[2])
            pt_type = row[3].strip()
            origin = row[4].strip()
            destination = row[5].strip()
            total_trips = int(row[6])

            if not all([year_month, day_type, origin, destination]):
                return

            if not 0 <= hour <= 23:
                return

            if pt_type != "BUS" or total_trips < 0:
                return

            # Exclude hour so trips are aggregated across all hours
            key = (year_month, day_type, origin, destination)

            yield key, total_trips

        except (ValueError, IndexError, StopIteration):
            return

    def combiner(self, key, values):
        yield key, sum(values)

    def reducer(self, key, values):
        yield key, sum(values)


if __name__ == "__main__":
    BusODAggregation.run()