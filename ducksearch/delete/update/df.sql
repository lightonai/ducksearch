WITH _docs_to_delete AS (
    SELECT DISTINCT bm25.docid
    FROM parquet_scan('{parquet_file}') AS p
    INNER JOIN bm25_documents.docs AS bm25
        ON p.id = bm25.name
),

_tf AS (
    SELECT
        termid,
        sum(tf) AS df
    FROM bm25_documents.terms
    INNER JOIN _docs_to_delete
        ON bm25_documents.terms.docid = _docs_to_delete.docid
    GROUP BY 1
)

UPDATE bm25_documents.dict _dict
SET df = greatest(_dict.df - _tf.df, 0)
FROM _tf
WHERE _dict.termid = _tf.termid;
