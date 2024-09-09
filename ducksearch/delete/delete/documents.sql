DELETE FROM {schema}.documents 
USING parquet_scan('{parquet_file}') AS _df_documents
WHERE {schema}.documents.id = _df_documents.id;
