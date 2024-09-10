-- This query finds the set of tokens scores for which there won't be any docid / score to keep.
WITH _docs_to_delete AS (
    SELECT DISTINCT
        bm25.docid
    FROM parquet_scan('{parquet_file}') p
    INNER JOIN bm25_documents.docs bm25
        ON p.id = bm25.name
),

_terms_to_recompute AS (
    SELECT DISTINCT
        term
    FROM bm25_documents.terms
    INNER JOIN _docs_to_delete
        ON bm25_documents.terms.docid = _docs_to_delete.docid
    INNER JOIN bm25_documents.dict
        ON bm25_documents.terms.termid = bm25_documents.dict.termid
),

_scores_to_update AS (
    SELECT
        _bm25.term,
        _bm25.list_scores,
        _bm25.list_docids
    FROM bm25_documents.scores _bm25
    INNER JOIN _terms_to_recompute _terms
        ON _bm25.term = _terms.term
),

_unested_scores AS (
    SELECT
        term,
        UNNEST(list_scores) AS score,
        UNNEST(list_docids) AS docid
    FROM _scores_to_update
),

_unested_unfiltered_scores AS (
    SELECT
        _scores.term,
        _scores.docid,
        _scores.score,
        _docs.docid AS to_delete
    FROM _unested_scores _scores
    LEFT JOIN _docs_to_delete _docs
        ON _scores.docid = _docs.docid
),

_unested_filtered_scores AS (
    SELECT
        term,
        docid,
        score
    FROM _unested_unfiltered_scores
    WHERE to_delete IS NULL
),

_list_scores AS (
    SELECT
        term,
        LIST(docid ORDER BY score DESC, docid ASC) AS list_docids,
        LIST(score ORDER BY score DESC, docid ASC) AS list_scores
    FROM _unested_filtered_scores
    GROUP BY 1
)

UPDATE bm25_documents.scores s
SET
    list_docids = u.list_docids,
    list_scores = u.list_scores
FROM _list_scores u
WHERE s.term = u.term;