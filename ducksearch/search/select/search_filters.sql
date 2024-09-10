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
        ) AS bm25id,
        UNNEST(
            s.list_scores[:{top_k_token}]
        ) AS score
    FROM _input_queries iq
    INNER JOIN {schema}.scores s
        ON iq.term = s.term
),

_matchs_scores AS (
    SELECT 
        query AS _query,
        bm25id,
        SUM(score) AS _score
    FROM _matchs
    GROUP BY 1, 2
),

_documents_filter AS (
    SELECT
        *
    FROM {source_schema}.{source}
    WHERE {filters}
),

_filtered_scores AS (
    SELECT
        _query,
        _score,
        s.* EXCLUDE (bm25id)
    FROM _matchs_scores ms
    INNER JOIN _documents_filter s
        ON ms.bm25id = s.bm25id
),

_partition_scores AS (
    SELECT
        _query,
        _score AS score,
        * EXCLUDE (_score, _query),
        ROW_NUMBER() OVER (PARTITION BY _query ORDER BY _score DESC) AS _row_number
    FROM _filtered_scores
)

SELECT 
    * EXCLUDE (_row_number)
FROM _partition_scores
WHERE _row_number <= {top_k};
