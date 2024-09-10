WITH _terms_to_score AS (
    SELECT 
        term
    FROM fts_{schema}__documents.dict

)

SELECT DISTINCT
    d.termid
FROM _terms_to_score t
JOIN {schema}.dict d
ON t.term = d.term;