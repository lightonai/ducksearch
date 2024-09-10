WITH _docs_to_delete AS (
    SELECT
        bm25.docid
    FROM parquet_scan('{parquet_file}') p
    INNER JOIN bm25_documents.docs bm25
        ON p.id = bm25.name
)

DELETE FROM bm25_documents.terms as _terms
USING _docs_to_delete as _docs
WHERE _terms.docid = _docs.docid;