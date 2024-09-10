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

_nested_matchs AS (
    SELECT
        iq.query,
        s.list_docids[0:{top_k_token}] as list_docids,
        s.list_scores[0:{top_k_token}] as list_scores
    FROM {schema}.scores s
    INNER JOIN _input_queries iq
        ON s.term = iq.term
),

_matchs AS (
    SELECT
        query,
        UNNEST(
            s.list_docids
        ) AS bm25id,
        UNNEST(
            s.list_scores
        ) AS score
    FROM _nested_matchs s
),

_matchs_scores AS (
    SELECT 
        query,
        bm25id,
        SUM(score) AS score
    FROM _matchs
    WHERE score IS NOT NULL AND score > 0
    GROUP BY 1, 2

),

_partition_scores AS (
    SELECT
        query,
        bm25id,
        score,
        ROW_NUMBER() OVER (PARTITION BY query ORDER BY score DESC, RANDOM() ASC) AS rank
    FROM _matchs_scores
)

SELECT
    s.* EXCLUDE (bm25id),
    ps.score,
    ps.query AS _query
FROM _partition_scores ps
JOIN {source_schema}.{source} s
    ON ps.bm25id = s.bm25id
WHERE ps.rank <= {top_k}
ORDER BY ps.rank ASC;
