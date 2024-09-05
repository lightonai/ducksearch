INSERT INTO documents_queries_graph (document_id, query_id, score)

WITH documents_queries_scores AS (
    SELECT
        document_id,
        query,
        MAX(score) AS score
    FROM df_documents_queries
    GROUP BY 1, 2
),

distinct_documents_queries AS (
    SELECT
        dqw.document_id,
        q.id AS query_id,
        dqw.score,
        dq.document_id AS existing_id
    FROM documents_queries_scores AS dqw
    INNER JOIN queries AS q
        ON
            dqw.query = q.query
    LEFT JOIN documents_queries_graph AS dq
        ON
            q.id = dq.query_id
            AND dqw.document_id = dq.document_id
)

SELECT DISTINCT
    document_id,
    query_id,
    score
FROM distinct_documents_queries
WHERE existing_id IS NULL;
