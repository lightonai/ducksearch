CREATE SCHEMA IF NOT EXISTS {schema};

CREATE SEQUENCE IF NOT EXISTS SEQ_{schema}_dict START 1;

CREATE TABLE IF NOT EXISTS {schema}.dict (
    termid INT PRIMARY KEY DEFAULT NEXTVAL('SEQ_{schema}_dict'),
    term VARCHAR,
    df INT
);

CREATE TABLE IF NOT EXISTS {schema}.scores (
    term VARCHAR,
    list_docids INT[],
    list_scores FLOAT4[]
);

CREATE SEQUENCE IF NOT EXISTS SEQ_{schema}_docs START 1;

CREATE TABLE IF NOT EXISTS {schema}.docs (
    docid INT PRIMARY KEY DEFAULT NEXTVAL('SEQ_{schema}_docs'),
    len INT,
    name VARCHAR
);

CREATE TABLE IF NOT EXISTS {schema}.stats (
    num_docs INT,
    avgdl FLOAT
);

CREATE TABLE IF NOT EXISTS {schema}.terms (
    docid INT,
    termid INT,
    tf INT
);

CREATE TABLE IF NOT EXISTS {schema}.stopwords (
    sw VARCHAR
);

CREATE OR REPLACE TABLE {schema}._documents AS (
    WITH _indexed_documents AS (
        SELECT
            s.*,
            d.name AS existing_id
        FROM {source_schema}.{source} s
        LEFT JOIN {schema}.docs d
        ON s.id = d.name
    )

    SELECT 
        {key_field} AS id,
        CONCAT_WS(' ', 
            {fields}
        ) AS _search
    FROM _indexed_documents
    WHERE existing_id IS NULL
);
