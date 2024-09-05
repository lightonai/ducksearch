WITH input_queries AS (
    SELECT
        query, 
        token,
        tf
    FROM read_parquet('{parquet_files}')
),

matchs AS (
    SELECT
        iq.query,
        UNNEST(
            s.list_ids[:{top_k_token}]
        ) as id,
        UNNEST(
            s.list_scores[:{top_k_token}]
        ) as score
    FROM input_queries iq
    INNER JOIN {schema}.scores  s
        ON iq.token = s.token
),

matchs_scores AS (
    SELECT 
        query,
        id,
        sum(score) as score
    FROM matchs
    GROUP BY 1, 2
),

partition_scores AS (
    SELECT
        query,
        id,
        score,
        ROW_NUMBER() OVER (PARTITION BY query ORDER BY score DESC) as rank
    FROM matchs_scores
)

SELECT
    ps.query as _query,
    ps.id,
    ps.score,
    s.* EXCLUDE (id)
FROM partition_scores ps
LEFT JOIN {source} s
    ON ps.id = s.id
WHERE ps.rank <= {top_k}
ORDER BY ps.rank ASC;
