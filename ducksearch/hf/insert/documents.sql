INSERT INTO {schema}.documents (id, {fields}) (
    WITH _hf_dataset AS (
        SELECT
            {key_field} AS id,
            {fields}
        FROM '{url}'
        {limit_hf}
    ),

    _hf_row_number AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) AS row_number
        FROM _hf_dataset
    )

    SELECT * EXCLUDE (row_number)
    FROM _hf_row_number
    WHERE row_number = 1
);
