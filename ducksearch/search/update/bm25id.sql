UPDATE {source_schema}.{source} source
SET bm25id = {schema}.docs.docid
FROM {schema}.docs 
WHERE source.id = {schema}.docs.name;
