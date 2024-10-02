WITH _docs_to_delete AS (
    SELECT bm25.docid
    FROM parquet_scan('{parquet_file}') AS p
    INNER JOIN bm25_documents.docs AS bm25
        ON p.id = bm25.name
)

DELETE FROM bm25_documents.terms AS _terms
USING _docs_to_delete AS _docs
WHERE _terms.docid = _docs.docid;
