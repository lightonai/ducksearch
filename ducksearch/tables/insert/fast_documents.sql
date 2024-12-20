INSERT INTO {schema}.documents ({fields})

WITH _distinct_documents AS (
    SELECT DISTINCT
        {df_fields}
    FROM read_parquet('{parquet_files}') df
)

SELECT
    *
FROM _distinct_documents;
