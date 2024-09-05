CREATE TABLE IF NOT EXISTS documents_queries_graph (
    document_id VARCHAR,
    query_id VARCHAR,
    score FLOAT DEFAULT NULL,
    FOREIGN KEY (document_id) REFERENCES documents (id),
    FOREIGN KEY (query_id) REFERENCES queries (id)
);
