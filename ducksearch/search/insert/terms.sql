INSERT INTO {schema}.terms (docid, termid, tf)

WITH _raw_terms AS (
    SELECT DISTINCT termid FROM parquet_scan('{parquet_file}')
),

_unfiltered_raw_terms AS (
    SELECT  DISTINCT
        _dict.term,
        sw.sw IS NOT NULL AS is_stopword
    FROM _raw_terms _rt 
    INNER JOIN {schema}.dict _dict
        ON _rt.termid = _dict.termid
    LEFT JOIN {schema}.stopwords sw
        ON _dict.term = sw.sw
),

_filtered_raw_terms AS (
    SELECT 
        term
    FROM _unfiltered_raw_terms
    WHERE is_stopword = FALSE
),

_filtered_raw_terms_bm25id AS (
    SELECT DISTINCT
        ftsdi.termid
    FROM _filtered_raw_terms _raw
    JOIN fts_{schema}__documents.dict ftsdi
        ON _raw.term = ftsdi.term
)

, _documents_terms_filter AS (
    SELECT 
        docid,
        _terms.termid,
        COUNT(*) AS tf
    FROM fts_{schema}__documents.terms _terms
    INNER JOIN _filtered_raw_terms_bm25id _raw 
        ON _terms.termid = _raw.termid
    GROUP BY 1, 2
)

SELECT 
    docs.docid,
    dict.termid,
    dt.tf
FROM _documents_terms_filter dt
JOIN fts_{schema}__documents.dict ftsdi
    ON dt.termid = ftsdi.termid
JOIN fts_{schema}__documents.docs ftsdo
    ON dt.docid = ftsdo.docid
JOIN {schema}.dict dict
    ON ftsdi.term = dict.term
JOIN {schema}.docs docs
    ON ftsdo.name = docs.name;
