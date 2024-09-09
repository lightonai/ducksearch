INSERT INTO {schema}.docs (len, name)

SELECT 
    len,
    name
FROM fts_{schema}__documents.docs;
