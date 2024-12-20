CREATE OR REPLACE TABLE {schema}._hf_tmp AS (
    WITH _hf_dataset AS (
        SELECT
            {key_field} AS id,
            *
        FROM '{url}'
        {limit_hf}
        {offset_hf}
    ),

    _hf_row_number AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY id, RANDOM()) AS _row_number
        FROM _hf_dataset
    )

    SELECT * EXCLUDE (_row_number)
    FROM _hf_row_number
    WHERE _row_number = 1
);
