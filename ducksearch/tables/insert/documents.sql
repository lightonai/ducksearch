INSERT INTO {schema}.documents (id, {fields})

WITH _distinct_documents AS (
    SELECT DISTINCT
        {key_field} AS id,
        {df_fields},
        d.id AS existing_id,
        ROW_NUMBER() OVER (PARTITION BY df.id ORDER BY df.id, RANDOM() ASC) AS row_number
    FROM read_parquet('{parquet_files}') AS df
    LEFT JOIN {schema}.documents AS d
        ON df.id = d.id
)

SELECT
    id,
    {fields}
FROM _distinct_documents 
WHERE existing_id IS NULL
AND row_number = 1;
