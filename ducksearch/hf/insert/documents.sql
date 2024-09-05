CREATE OR REPLACE TABLE df_documents AS (
    WITH hf_dataset AS (
        SELECT
        {key_field} AS id,
        {fields},
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) AS row_number
        FROM '{url}'
    )

    SELECT * EXCLUDE (row_number)
    FROM hf_dataset
    WHERE row_number = 1
);
