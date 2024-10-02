CREATE OR REPLACE TABLE {schema}._queries_{random_hash} AS (
    SELECT
        query,
        group_id
    FROM parquet_scan('{parquet_file}')
);
