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
            ROW_NUMBER() OVER (PARTITION BY id ORDER BY id, RANDOM()) AS _row_number
        FROM _hf_dataset
    ),

    _new_hf_dataset AS (
        SELECT
            _hf_row_number.*,
            d.id AS existing_id
        FROM _hf_row_number
        LEFT JOIN {schema}.documents AS d
            ON _hf_row_number.id = d.id
        WHERE _row_number = 1

    )

    SELECT id, {fields}
    FROM _new_hf_dataset
    WHERE _row_number = 1
    AND existing_id IS NULL
);
