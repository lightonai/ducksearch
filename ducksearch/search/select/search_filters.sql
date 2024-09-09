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
    INNER JOIN {schema}.scores  s
        ON iq.term = s.term
),

_matchs_scores AS (
    SELECT 
        query as _query,
        bm25id,
        sum(score) as _score
    FROM _matchs
    GROUP BY 1, 2
),

_filtered_scores AS (
    SELECT
        _query,
        _score,
        s.* EXCLUDE (bm25id)
    FROM _matchs_scores ms
    JOIN {source_schema}.{source} s
        ON ms.bm25id = s.bm25id
    WHERE {filters}
),

_partition_scores AS (
    SELECT
        _query,
        _score as score,
        * EXCLUDE (_score, _query),
        ROW_NUMBER() OVER (PARTITION BY _query ORDER BY _score DESC) as rank
    FROM _filtered_scores
)

SELECT 
    * EXCLUDE (rank)
FROM _partition_scores
WHERE rank <= {top_k};
