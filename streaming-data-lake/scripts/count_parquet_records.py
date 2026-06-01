#!/usr/bin/env python3

# Copyright (c) 2026 Dynatrace LLC. All rights reserved.
# Licensed under the terms in the repository LICENSE file.

import argparse
import glob
import os
import duckdb


def summarize(path_pattern: str, min_bytes: int, con: duckdb.DuckDBPyConnection) -> None:
    files = [f for f in glob.glob(path_pattern) if os.path.getsize(f) >= min_bytes]

    print(f"Path: {path_pattern}")
    if not files:
        print("No files found")
        print("Count: 0")
        print()
        return

    count = con.sql("SELECT COUNT(*) FROM read_parquet(?)", params=[files]).fetchone()[0]
    fields = con.sql("DESCRIBE SELECT * FROM read_parquet(?)", params=[files]).fetchall()

    print(f"Files: {len(files)}")
    print(f"Count: {count}")
    print(f"Fields ({len(fields)}):")
    for field in fields:
        print(f"  - {field[0]}: {field[1]}")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Count records and list schema from Parquet files.")
    parser.add_argument(
        "--path",
        action="append",
        required=True,
        help="Glob pattern for parquet files. Repeat --path for multiple directories.",
    )
    parser.add_argument(
        "--min-bytes",
        type=int,
        default=1024,
        help="Ignore files smaller than this size in bytes. Default: 1024",
    )
    args = parser.parse_args()

    con = duckdb.connect()
    for p in args.path:
        summarize(p, args.min_bytes, con)


if __name__ == "__main__":
    main()
