INSERT INTO {schema}.queries (query)

WITH _distinct_queries AS (
    SELECT DISTINCT
        df.query,
        q.id AS existing_id
    FROM parquet_scan('{parquet_file}') AS df
    LEFT JOIN {schema}.queries AS q
        ON df.query = q.query
)

SELECT DISTINCT query
FROM _distinct_queries
WHERE existing_id IS NULL;
