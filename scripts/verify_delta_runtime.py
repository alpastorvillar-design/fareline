"""Prove that Delta is wired correctly and warm the Ivy cache.

This runs during the image build so that the pinned Delta artifacts are
resolved once, offline afterwards, and so that a broken Spark/Delta combination
fails the build instead of the first pipeline run.
"""

from __future__ import annotations

import json
import shutil
import sys
from pathlib import Path

from fareline.m1 import occurrences


def main() -> int:
    target = Path(sys.argv[1] if len(sys.argv) > 1 else "/tmp/fareline-delta-verify")
    spark = occurrences.build_session("fareline-delta-verify", target / "warehouse")
    spark.sparkContext.setLogLevel("WARN")
    try:
        import delta

        table = occurrences.as_uri(target / "table")
        spark.range(0, 8).write.format("delta").mode("overwrite").save(table)
        rows = spark.read.format("delta").load(table).count()
        if rows != 8:
            raise AssertionError(f"Delta round trip returned {rows} rows")
        packages = spark.sparkContext.getConf().get("spark.jars.packages", "")
        if not packages.endswith(delta.__version__):
            raise AssertionError(
                f"Delta jar coordinate {packages} does not match Python package {delta.__version__}"
            )
        print(
            "FARELINE_DELTA_RUNTIME="
            + json.dumps(
                {
                    "spark_version": spark.version,
                    "delta_python_version": delta.__version__,
                    "delta_jar_packages": packages,
                    "java_version": spark._jvm.System.getProperty("java.version"),
                    "rows": rows,
                },
                sort_keys=True,
            )
        )
        return 0
    finally:
        spark.stop()
        shutil.rmtree(target, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
