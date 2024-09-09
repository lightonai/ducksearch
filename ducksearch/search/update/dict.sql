WITH new_terms AS (
    SELECT 
        fts.df,
        fts.term,
        d.termid AS existing_id
    FROM fts_{schema}__documents.dict fts
    LEFT JOIN {schema}.dict d
    ON fts.term = d.term
)

UPDATE {schema}.dict d
SET df = d.df + nt.df
FROM new_terms nt
WHERE d.termid = nt.existing_id;

