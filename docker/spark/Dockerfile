ARG SPARK_IMAGE=apache/spark:4.1.0-scala2.13-java17-python3-ubuntu@sha256:578022b509170d08ce22eced52be35901550c96b854662e848e00bb7eb0bf876
FROM ${SPARK_IMAGE}

USER root
WORKDIR /opt/fareline

COPY pyproject.toml README.md LICENSE ./
COPY src ./src
RUN python3 -m pip install --no-cache-dir .

RUN mkdir -p /opt/fareline/data /opt/fareline/output /opt/fareline/spark-events \
    && chown -R 185:0 /opt/fareline

USER 185
