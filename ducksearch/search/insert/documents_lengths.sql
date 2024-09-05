INSERT INTO {schema}.lengths (id, document_length)

WITH existing_documents AS (
    SELECT DISTINCT
        p.id,
        p.document_length,
        dl.id AS existing_document
    FROM read_parquet('{parquet_files}') AS p
    LEFT JOIN
        {schema}.lengths
            AS dl
        ON p.id = dl.id
)

SELECT
    id,
    document_length
FROM existing_documents
WHERE existing_document IS NULL;
