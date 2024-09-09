CREATE TABLE IF NOT EXISTS {schema}.documents_queries (
    document_id VARCHAR,
    query_id VARCHAR,
    score FLOAT DEFAULT NULL,
    FOREIGN KEY (document_id) REFERENCES {schema}.documents (id),
    FOREIGN KEY (query_id) REFERENCES {schema}.queries (id)
);
