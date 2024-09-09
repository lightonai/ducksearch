INSERT INTO {schema}.terms (docid, termid, tf)
WITH _documents_terms AS (
    SELECT 
        docid,
        termid,
        COUNT(*) AS tf
    FROM fts_{schema}__documents.terms
    GROUP BY docid, termid
)
SELECT 
    docs.docid,
    dict.termid,
    dt.tf
FROM _documents_terms dt
JOIN fts_{schema}__documents.dict ftsdi
    ON dt.termid = ftsdi.termid
JOIN fts_{schema}__documents.docs ftsdo
    ON dt.docid = ftsdo.docid
JOIN {schema}.dict dict
    ON ftsdi.term = dict.term
JOIN {schema}.docs docs
    ON ftsdo.name = docs.name;
