WITH documents_to_index AS (
    SELECT
        s.id,
        dl.id AS existing_document
    FROM {source} AS s
    LEFT JOIN
        {schema}.lengths
            AS dl
        ON s.id = dl.id
)

SELECT count(*) AS count
FROM documents_to_index 
WHERE existing_document IS NULL;
