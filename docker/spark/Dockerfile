ARG SPARK_IMAGE=apache/spark:4.1.0-scala2.13-java17-python3-ubuntu@sha256:578022b509170d08ce22eced52be35901550c96b854662e848e00bb7eb0bf876
FROM ${SPARK_IMAGE}

# Delta 4.2.0 is the release compatible with Spark 4.1.x. The Python package and
# the JVM artifacts must stay on the same version; both are derived from here.
ARG DELTA_VERSION=4.2.0

USER root
WORKDIR /opt/fareline

COPY pyproject.toml README.md LICENSE ./
COPY src ./src
COPY scripts ./scripts

# --no-deps keeps pip from installing a second PySpark distribution next to the
# one this image already ships.
RUN python3 -m pip install --no-cache-dir . \
    && python3 -m pip install --no-cache-dir --no-deps "delta-spark==${DELTA_VERSION}"

# HOME is /nonexistent in this image, so Ivy needs an explicit writable cache.
# spark.jars.packages has to be set before the JVM starts, which is why it lives
# in spark-defaults instead of the SparkSession builder.
RUN mkdir -p /opt/spark/conf /opt/fareline/ivy /opt/fareline/data /opt/fareline/output \
        /opt/fareline/warehouse /opt/fareline/spark-events /opt/fareline/samples \
        /opt/fareline/evidence \
    && printf '%s\n' \
        "spark.jars.packages io.delta:delta-spark_2.13:${DELTA_VERSION}" \
        "spark.jars.ivy /opt/fareline/ivy" \
        > /opt/spark/conf/spark-defaults.conf \
    && SPARK_LOCAL_IP=127.0.0.1 /opt/spark/bin/spark-submit --master "local[1]" \
        /opt/fareline/scripts/verify_delta_runtime.py \
    && chown -R 185:0 /opt/fareline /opt/spark/conf

USER 185
