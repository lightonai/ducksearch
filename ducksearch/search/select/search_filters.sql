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
        query as _query,
        id as _id,
        sum(score) as _score
    FROM matchs
    GROUP BY 1, 2
),

filtered_scores AS (
    SELECT
        _query,
        _id,
        _score,
        s.* EXCLUDE (id)
    FROM matchs_scores ms
    LEFT JOIN {source} s
        ON ms._id = s.id
    WHERE {filters}
),

partition_scores AS (

    SELECT
        _query,
        _id as id,
        _score as score,
        * EXCLUDE (_id, _score, _query),
        ROW_NUMBER() OVER (PARTITION BY _query ORDER BY _score DESC) as rank
    FROM filtered_scores

)

SELECT 
    * EXCLUDE (rank)
FROM partition_scores
WHERE rank <= {top_k};
