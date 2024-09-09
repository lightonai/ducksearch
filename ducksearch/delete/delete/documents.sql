DELETE FROM {schema}.documents 
USING parquet_scan('{parquet_file}') AS df_documents
WHERE {schema}.documents.id = df_documents.id;
