INSERT INTO {schema}.tokens (token)

WITH new_tokens AS (
    SELECT DISTINCT
        p.token,
        t.id AS existing_token
    FROM read_parquet('{parquet_files}') AS p
    LEFT JOIN
        {schema}.tokens AS t
        ON p.token = t.token
)

SELECT token
FROM new_tokens
WHERE existing_token IS NULL;


INSERT INTO {schema}.tf (id, token_id, tf)

SELECT DISTINCT
    p.id,
    t.id AS token_id,
    p.tf
FROM read_parquet('{parquet_files}') AS p
INNER JOIN {schema}.tokens AS t
    ON p.token = t.token;
