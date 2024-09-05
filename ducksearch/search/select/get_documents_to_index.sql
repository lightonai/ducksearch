WITH documents_to_index AS (
    SELECT
        s.id,
        CONCAT_WS(' ', 
            {fields}
        ) AS _search,
        l.id AS existing_document
    FROM {source} AS s
    LEFT JOIN
        {schema}.lengths
            AS l
        ON s.id = l.id
)

SELECT
    id,
    _search
FROM documents_to_index
WHERE existing_document IS NULL
LIMIT {batch_size};
