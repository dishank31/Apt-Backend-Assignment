FROM postgres:15

# Required by cdc_worker.py for pg_create_logical_replication_slot(..., 'wal2json')
RUN apt-get update \
    && apt-get install -y postgresql-15-wal2json \
    && rm -rf /var/lib/apt/lists/*
