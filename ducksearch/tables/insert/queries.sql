INSERT INTO queries (query)
WITH distinct_queries AS (
    SELECT DISTINCT
        df.query,
        q.id AS existing_id
    FROM df_queries AS df
    LEFT JOIN queries AS q
        ON df.query = q.query
)

SELECT DISTINCT query
FROM distinct_queries
WHERE existing_id IS NULL;
