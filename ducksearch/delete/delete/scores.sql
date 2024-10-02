-- This query finds the set of tokens scores for which there won't be any docid / score to keep.
WITH _docs_to_delete AS (
    SELECT DISTINCT bm25.docid
    FROM parquet_scan('{parquet_file}') AS p
    INNER JOIN bm25_documents.docs AS bm25
        ON p.id = bm25.name
),

_terms_to_recompute AS (
    SELECT DISTINCT term
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
    FROM bm25_documents.scores AS _bm25
    INNER JOIN _terms_to_recompute AS _terms
        ON _bm25.term = _terms.term
),

_unested_scores AS (
    SELECT
        term,
        unnest(list_scores) AS score,
        unnest(list_docids) AS docid
    FROM _scores_to_update
),

_unested_unfiltered_scores AS (
    SELECT
        _scores.term,
        _scores.docid,
        _scores.score,
        _docs.docid AS to_delete
    FROM _unested_scores AS _scores
    LEFT JOIN _docs_to_delete AS _docs
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

_terms_to_delete AS (
    SELECT DISTINCT
        ttr.term,
        ufs.term AS missing
    FROM _terms_to_recompute AS ttr
    LEFT JOIN _unested_filtered_scores AS ufs
        ON ttr.term = ufs.term
),

_scores_to_delete_completely AS (
    SELECT DISTINCT term
    FROM _terms_to_delete
    WHERE missing IS NULL
)

DELETE FROM bm25_documents.scores AS _scores
USING _scores_to_delete_completely AS _scores_to_delete
WHERE _scores.term = _scores_to_delete.term;
