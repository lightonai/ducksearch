WITH input_queries AS (
    SELECT
        query, 
        token,
        tf
    FROM read_parquet('{parquet_files}')
),

documents_matchs AS (
    SELECT
        iq.query,
        UNNEST(
            s.list_ids[:{top_k_token}]
        ) as id,
        UNNEST(
            s.list_scores[:{top_k_token}]
        ) as score
    FROM input_queries iq
    INNER JOIN bm25_documents.scores  s
        ON iq.token = s.token
),

queries_matchs AS (
    SELECT
        iq.query,
        UNNEST(
            s.list_ids[:{top_k_token}]
        ) as id,
        UNNEST(
            s.list_scores[:{top_k_token}]
        ) as score
    FROM input_queries iq
    INNER JOIN bm25_queries.scores  s
        ON iq.token = s.token
),

documents_scores_without_filter AS (
    SELECT 
        query as _query,
        id as _id,
        sum(score) as _score
    FROM documents_matchs
    GROUP BY 1, 2
),

documents_scores AS (
    SELECT 
        dswf._query as query,
        dswf._id as id,
        dswf._score as score
    FROM documents_scores_without_filter dswf
    INNER JOIN documents d
    ON documents_scores_without_filter._id = d.id
    WHERE {filters}
),

queries_scores AS (
    SELECT 
        query,
        id,
        sum(score) as score
    FROM queries_matchs
    GROUP BY 1, 2
),

documents_ranks AS (
    SELECT
        query,
        id,
        score,
        ROW_NUMBER() OVER (PARTITION BY query ORDER BY score DESC) as rank
    FROM documents_scores
),

queries_ranks AS (
    SELECT
        query,
        id,
        score,
        ROW_NUMBER() OVER (PARTITION BY query ORDER BY score DESC) as rank
    FROM queries_scores
    
),

bm25_documents AS (
    SELECT
        ps.query as _query,
        ps.id,
        ps.score
    FROM documents_ranks ps
    WHERE ps.rank <= {top_k} 
),

-- The following queries computes the BM25 scores for the queries.


bm25_queries AS (
    SELECT
        ps.query as _query,
        ps.id,
        ps.score
    FROM queries_ranks ps
    WHERE ps.rank <= {top_k} 
),


_graph AS (
    SELECT
        bm25.id AS src_id,
        dqg.query_id AS dst_id,
        dqg.score AS edge,
        'document' AS src_type,
        'query' AS dst_type,
        bm25._query
    FROM bm25_documents AS bm25
    INNER JOIN documents_queries_graph AS dqg
        ON bm25.id = dqg.document_id
    INNER JOIN bm25_queries AS bm25q
        ON
            dqg.query_id = bm25q.id
            AND bm25._query = bm25q._query
),

_graph_scores AS (
    SELECT
        g.*,
        coalesce(bm25.score, 0) AS src_score,
        0 AS dst_score
    FROM _graph AS g
    LEFT JOIN bm25_documents AS bm25
        ON
            g.src_id = bm25.id
            AND g._query = bm25._query
    WHERE src_type = 'document'
    UNION
    SELECT
        g.*,
        0 AS src_score,
        coalesce(bm25.score, 0) AS dst_score
    FROM _graph AS g
    LEFT JOIN bm25_documents AS bm25
        ON
            g.dst_id = bm25.id
            AND g._query = bm25._query
    WHERE dst_type = 'document'
    UNION
    SELECT
        g.*,
        coalesce(bm25.score, 0) AS src_score,
        0 AS dst_score
    FROM _graph AS g
    LEFT JOIN bm25_queries AS bm25
        ON
            g.src_id = bm25.id
            AND g._query = bm25._query
    WHERE src_type = 'query'
    UNION
    SELECT
        g.*,
        0 AS src_score,
        coalesce(bm25.score, 0) AS dst_score
    FROM _graph AS g
    LEFT JOIN bm25_queries AS bm25
        ON
            g.dst_id = bm25.id
            AND g._query = bm25._query
    WHERE dst_type = 'query'
),

graph_scores AS (
    SELECT
        src_id,
        dst_id,
        _query,
        src_type,
        dst_type,
        max(src_score) AS src_score,
        max(dst_score) AS dst_score,
        max(edge) AS edge
    FROM _graph_scores
    GROUP BY 1, 2, 3, 4, 5
),

-- The following could be learned with ML.
rank AS (
    SELECT
        src_id AS id,
        _query,
        sum(src_score + dst_score + edge) AS score
    FROM graph_scores
    WHERE src_type = 'document'
    GROUP BY 1, 2
    UNION ALL
    SELECT
        dst_id AS id,
        _query,
        sum(dst_score + src_score + edge) AS score
    FROM graph_scores
    WHERE dst_type = 'document'
    GROUP BY 1, 2
    UNION ALL
    SELECT
        id,
        _query,
        score
    FROM bm25_documents
),

scores AS (
    SELECT
        id,
        _query,
        max(score) AS score
    FROM rank
    GROUP BY 1, 2
)

SELECT
    *
FROM scores
ORDER BY score DESC;
