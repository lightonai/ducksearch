DELETE FROM {schema}.documents_queries 
USING parquet_scan('{parquet_file}') AS _df_documents
WHERE {schema}.documents_queries.document_id = _df_documents.id;