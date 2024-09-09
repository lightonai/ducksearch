WITH _terms_scores_to_drop AS (
    SELECT DISTINCT
        d.term
    FROM fts_{schema}__documents.dict fts
    INNER JOIN {schema}.dict d
    ON fts.term = d.term
)

DELETE FROM {schema}.scores s
USING _terms_scores_to_drop t
WHERE s.term = t.term;