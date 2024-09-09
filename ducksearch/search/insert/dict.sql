INSERT INTO {schema}.dict (term, df)

WITH _new_terms AS (
    SELECT 
        fts.df,
        fts.term,
        d.termid AS existing_id
    FROM fts_{schema}__documents.dict fts
    LEFT JOIN {schema}.dict d
    ON fts.term = d.term
)

SELECT
    term,
    df
FROM _new_terms
WHERE existing_id IS NULL;
