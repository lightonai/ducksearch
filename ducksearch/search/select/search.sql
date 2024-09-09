WITH _input_queries AS (
    SELECT
        pf.query,
        ftsdict.term
    FROM parquet_scan('{parquet_file}') pf
    JOIN fts_{schema}__queries.docs docs
        ON pf.query = docs.name
    JOIN fts_{schema}__queries.terms terms
        ON docs.docid = terms.docid
    JOIN fts_{schema}__queries.dict ftsdict
        ON terms.termid = ftsdict.termid
),

_matchs AS (
    SELECT
        query,
        UNNEST(
            s.list_docids[:{top_k_token}]
        ) as bm25id,
        UNNEST(
            s.list_scores[:{top_k_token}]
        ) as score
    FROM _input_queries iq
    INNER JOIN {schema}.scores s
        ON iq.term = s.term
),

_matchs_scores AS (
    SELECT 
        query,
        bm25id,
        sum(score) as score
    FROM _matchs
    GROUP BY 1, 2
),

_partition_scores AS (
    SELECT
        query,
        bm25id,
        score,
        ROW_NUMBER() OVER (PARTITION BY query ORDER BY score DESC, RANDOM()) as rank
    FROM _matchs_scores
)

SELECT
    s.* EXCLUDE (bm25id),
    ps.score,
    ps.query as _query
FROM _partition_scores ps
LEFT JOIN {source_schema}.{source} s
    ON ps.bm25id = s.bm25id
WHERE ps.rank <= {top_k}
ORDER BY ps.rank ASC;
