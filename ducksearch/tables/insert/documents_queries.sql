INSERT INTO {schema}.documents_queries (document_id, query_id, score)

WITH _documents_queries_scores AS (
    SELECT
        document_id,
        query,
        MAX(score) AS score
    FROM parquet_scan('{parquet_file}')
    GROUP BY 1, 2
),

_distinct_documents_queries AS (
    SELECT
        dqw.document_id,
        q.id AS query_id,
        dqw.score,
        dq.document_id AS existing_id
    FROM _documents_queries_scores AS dqw
    INNER JOIN {schema}.queries AS q
        ON dqw.query = q.query
    LEFT JOIN {schema}.documents_queries AS dq
        ON q.id = dq.query_id
        AND dqw.document_id = dq.document_id
)

SELECT DISTINCT
    document_id,
    query_id,
    score
FROM _distinct_documents_queries
WHERE existing_id IS NULL;
