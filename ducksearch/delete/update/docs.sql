DELETE FROM bm25_documents.docs AS _docs
USING parquet_scan('{parquet_file}') AS _df_documents
WHERE _docs.name = _df_documents.id;
