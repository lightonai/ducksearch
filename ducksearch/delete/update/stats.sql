WITH _stats AS (
    SELECT
        COUNT(*) AS num_docs,
        AVG(len) AS avgdl
    FROM bm25_documents.docs 
)

UPDATE bm25_documents.stats 
SET num_docs = _stats.num_docs,
    avgdl = _stats.avgdl
FROM _stats;