CREATE SEQUENCE IF NOT EXISTS _seq_documents_id START 1;

CREATE TABLE IF NOT EXISTS {schema}.documents (
    id VARCHAR PRIMARY KEY DEFAULT (nextval('_seq_documents_id')),
    {fields},
    bm25id INT DEFAULT NULL
);
