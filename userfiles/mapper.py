#!/usr/bin/env python3

import sys
import csv

reader = csv.reader(sys.stdin)

for row in reader:
    # Skip CSV header
    if row and row[0].strip() == "YEAR_MONTH":
        continue

    # Reject malformed rows
    if len(row) != 7:
        sys.stderr.write("Invalid row: expected 7 columns\n")
        continue

    try:
        year_month = row[0].strip()
        day_type = row[1].strip()
        hour = int(row[2])
        pt_type = row[3].strip()
        origin = row[4].strip()
        destination = row[5].strip()
        total_trips = int(row[6])

        # Validate required fields
        if not all([year_month, day_type, origin, destination]):
            continue

        if not 0 <= hour <= 23:
            continue

        if pt_type != "BUS":
            continue

        if total_trips < 0:
            continue

        # Hour is intentionally excluded:
        # M1 requires totals across all hours.
        key = (
            year_month,
            day_type,
            origin,
            destination
        )

        print(f"{'|'.join(key)}\t{total_trips}")

    except (ValueError, IndexError):
        sys.stderr.write("Invalid row: conversion failed\n")
        continue