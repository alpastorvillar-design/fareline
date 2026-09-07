"""Prove that a standalone Spark cluster can publish and read shared output."""

from __future__ import annotations

import json
import os
import socket
import time

from pyspark.sql import SparkSession

EXPECTED_ROWS = 100_000
OUTPUT_PATH = os.environ.get(
    "FARELINE_SMOKE_OUTPUT",
    "file:///opt/fareline/output/spark-storage-smoke",
)


def executor_host(partition: object) -> list[str]:
    """Keep tasks alive briefly so both standalone workers receive work."""
    for _ in partition:
        break
    time.sleep(0.2)
    return [socket.gethostname()]


def main() -> None:
    spark = SparkSession.builder.appName("fareline-storage-smoke").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        hosts = sorted(
            spark.sparkContext.parallelize(range(32), 32)
            .mapPartitions(executor_host)
            .distinct()
            .collect()
        )
        if len(hosts) < 2:
            raise AssertionError(f"Expected at least two executor hosts, observed {hosts}")

        spark.range(EXPECTED_ROWS, numPartitions=8).write.mode("overwrite").parquet(OUTPUT_PATH)
        actual_rows = spark.read.parquet(OUTPUT_PATH).count()
        if actual_rows != EXPECTED_ROWS:
            raise AssertionError(f"Expected {EXPECTED_ROWS} rows, read {actual_rows}")

        hadoop = spark.sparkContext._jvm.org.apache.hadoop  # type: ignore[attr-defined]
        path = hadoop.fs.Path(OUTPUT_PATH)
        filesystem = path.getFileSystem(spark.sparkContext._jsc.hadoopConfiguration())
        data_files = [
            item.getPath().getName()
            for item in filesystem.listStatus(path)
            if item.isFile() and item.getPath().getName().startswith("part-")
        ]
        if not data_files:
            raise AssertionError("Spark reported success without publishing data files")

        print(
            "FARELINE_SPARK_SMOKE="
            + json.dumps(
                {
                    "data_files": len(data_files),
                    "executor_hosts": hosts,
                    "output_path": OUTPUT_PATH,
                    "rows": actual_rows,
                    "spark_version": spark.version,
                },
                sort_keys=True,
            )
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
