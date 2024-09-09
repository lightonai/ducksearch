CREATE TABLE IF NOT EXISTS {schema}.documents (
    id VARCHAR PRIMARY KEY NOT NULL,
    {fields},
    bm25id INT DEFAULT NULL
);
