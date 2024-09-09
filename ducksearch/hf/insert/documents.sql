INSERT INTO {schema}.documents (id, {fields}) (
    WITH _hf_dataset AS (
        SELECT
            {key_field} AS id,
            {fields},
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) AS row_number
        FROM '{url}'
    )

    SELECT * EXCLUDE (row_number)
    FROM _hf_dataset
    WHERE row_number = 1
);
