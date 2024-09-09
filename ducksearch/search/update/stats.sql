CREATE OR REPLACE TABLE {schema}.stats AS (
    SELECT
        COUNT(*) AS num_docs,
        AVG(len) AS avgdl
    FROM {schema}.docs
);