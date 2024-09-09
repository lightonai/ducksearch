WITH _input_queries AS (
    SELECT
        pf.query,
        ftsdict.term
    FROM parquet_scan('{parquet_file}') pf
    JOIN fts_{documents_schema}__queries.docs docs
        ON pf.query = docs.name
    JOIN fts_{documents_schema}__queries.terms terms
        ON docs.docid = terms.docid
    JOIN fts_{documents_schema}__queries.dict ftsdict
        ON terms.termid = ftsdict.termid
),

_documents_matchs AS (
    SELECT
        iq.query,
        UNNEST(
            s.list_docids[:{top_k_token}]
        ) as id,
        UNNEST(
            s.list_scores[:{top_k_token}]
        ) as score
    FROM _input_queries iq
    INNER JOIN {documents_schema}.scores  s
        ON iq.term = s.term
),

_queries_matchs AS (
    SELECT
        iq.query,
        UNNEST(
            s.list_docids[:{top_k_token}]
        ) as id,
        UNNEST(
            s.list_scores[:{top_k_token}]
        ) as score
    FROM _input_queries iq
    INNER JOIN {queries_schema}.scores  s
        ON iq.term = s.term
),

_documents_scores AS (
    SELECT 
        query,
        id,
        sum(score) as score
    FROM _documents_matchs
    GROUP BY 1, 2
),

_queries_scores AS (
    SELECT 
        query,
        id,
        sum(score) as score
    FROM _queries_matchs
    GROUP BY 1, 2
),

_documents_ranks AS (
    SELECT
        query,
        id,
        score,
        ROW_NUMBER() OVER (PARTITION BY query ORDER BY score DESC) as rank
    FROM _documents_scores
),

_queries_ranks AS (
    SELECT
        query,
        id,
        score,
        ROW_NUMBER() OVER (PARTITION BY query ORDER BY score DESC) as rank
    FROM _queries_scores
    
),

_bm25_documents AS (
    SELECT
        ps.query as _query,
        ddocs.name as id,
        ps.score
    FROM _documents_ranks ps
    INNER JOIN {documents_schema}.docs AS ddocs
        ON ps.id = ddocs.docid
    WHERE ps.rank <= {top_k} 
),

-- The following queries computes the BM25 scores for the queries.

_bm25_queries AS (
    SELECT
        ps.query as _query,
        ddocs.name as id,
        ps.score
    FROM _queries_ranks ps
    INNER JOIN {queries_schema}.docs AS ddocs
        ON ps.id = ddocs.docid
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
    FROM _bm25_documents AS bm25
    INNER JOIN {source_schema}.documents_queries AS dqg
        ON bm25.id = dqg.document_id
    INNER JOIN _bm25_queries AS bm25q
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
    LEFT JOIN _bm25_documents AS bm25
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
    LEFT JOIN _bm25_documents AS bm25
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
    LEFT JOIN _bm25_queries AS bm25
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
    LEFT JOIN _bm25_queries AS bm25
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
    FROM _bm25_documents
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
    docs.* EXCLUDE (bm25id),
    s.score,
    s._query
FROM scores s
JOIN {source_schema}.documents docs
    ON s.id = docs.id
ORDER BY s.score DESC;
