INSERT INTO {schema}.documents (id, {fields})

WITH _distinct_documents AS (
    SELECT DISTINCT
        {key_field} AS id,
        {df_fields},
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY id, RANDOM() ASC) AS _row_number
    FROM read_parquet('{parquet_files}') df
),

_new_distinct_documents AS (
    SELECT DISTINCT
        dd.*,
        d.id AS existing_id
    FROM _distinct_documents dd
    LEFT JOIN {schema}.documents AS d
        ON dd.id = d.id
    WHERE _row_number = 1
)

SELECT
    id,
    {fields}
FROM _new_distinct_documents 
WHERE existing_id IS NULL;
