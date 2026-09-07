"""spark-submit entry point for the M1 command line.

The Spark distribution provides PySpark only inside a submitted application, so
the same CLI is launched here instead of through the installed console script.
"""

from fareline.m1.cli import main

if __name__ == "__main__":
    raise SystemExit(main())
