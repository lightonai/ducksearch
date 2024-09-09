CREATE OR REPLACE TABLE {schema}._queries AS (
    SELECT
        query
    FROM parquet_scan('{parquet_file}')
);
