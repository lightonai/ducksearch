INSERT INTO documents (id, {fields})
WITH distinct_documents AS (
    SELECT DISTINCT
        {key_field} AS id,
        {df_fields},
        d.id AS existing_id,
        ROW_NUMBER() OVER (PARTITION BY df.id ORDER BY df.id) AS row_number
    FROM df_documents AS df
    LEFT JOIN documents AS d
        ON df.id = d.id
)

SELECT
    id,
    {fields}
FROM distinct_documents 
WHERE existing_id IS NULL
AND row_number = 1;
