# clickhouse-server with python3, the UDF installed and pointed at the mock API inside the same container.
ARG CH_VERSION=26.1
FROM clickhouse/clickhouse-server:${CH_VERSION}
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 \
 && rm -rf /var/lib/apt/lists/*
COPY clickhouse/jev_udf.py /var/lib/clickhouse/user_scripts/jev_udf.py
COPY clickhouse/jev_function.xml /etc/clickhouse-server/jev_function.xml
COPY clickhouse/jev_wrappers.sql test/clickhouse/mock_api.py /opt/jev/
RUN chmod 755 /var/lib/clickhouse/user_scripts/jev_udf.py \
 && printf '%s\n' '{"api_key": "test-key", "api_url": "http://127.0.0.1:8765/v1/systemone", "cache_path": "/var/lib/clickhouse/jev_cache.sqlite"}' \
    > /etc/clickhouse-server/jev.json \
 && chown -R clickhouse:clickhouse /var/lib/clickhouse/user_scripts /etc/clickhouse-server/jev.json
