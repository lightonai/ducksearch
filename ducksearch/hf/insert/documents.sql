INSERT INTO {schema}.documents (id, {_hf_tmp_columns}) (
    WITH _hf_dataset AS (
        SELECT
            id,
            * EXCLUDE (id)
        FROM {schema}._hf_tmp
    ),

    _new_hf_dataset AS (
        SELECT
            _hf_dataset.*,
            d.id AS existing_id
        FROM _hf_dataset
        LEFT JOIN {schema}.documents AS d
            ON _hf_dataset.id = d.id

    )

    SELECT id, {_hf_tmp_columns} 
    FROM _new_hf_dataset
    WHERE existing_id IS NULL
);
