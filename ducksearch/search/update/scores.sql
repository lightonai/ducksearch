WITH total_docs AS (
    SELECT COUNT(*) AS N 
    FROM {schema}.lengths
),

tokens_df AS (
    SELECT 
        token_id,
        COUNT(DISTINCT id) AS DF
    FROM {schema}.tf
    GROUP BY token_id
),

avg_document_length AS (
    SELECT 
        AVG(document_length) AS avg_document_length
    FROM {schema}.lengths
),

scores AS (
    SELECT 
        tf.id,
        tf.token_id,
        tf.tf *
        LOG(
            (
                (tdocs.n - tdf.df + 0.5) /
                (tdf.df + 0.5)
            ) + 1
        ) *
        (1.0 / (tf.tf + {k1} * (1 - {b} + {b} * (dl.document_length / adl.avg_document_length)))) AS score
    FROM 
        {schema}.tf tf
    JOIN 
        {schema}.lengths dl ON dl.id = tf.id
    JOIN 
        tokens_df tdf ON tdf.token_id = tf.token_id
    CROSS JOIN 
        total_docs tdocs
    CROSS JOIN 
        avg_document_length adl
)

UPDATE {schema}.tf AS tf
SET score = s.score
FROM scores s
WHERE tf.id = s.id
AND tf.token_id = s.token_id;

DROP INDEX IF EXISTS {schema}_scores_index;

CREATE OR REPLACE TABLE {schema}.scores AS (
    SELECT 
        t.token AS token,
        LIST(tf.id ORDER BY tf.score DESC, tf.id ASC) AS list_ids,
        LIST(tf.score ORDER BY tf.score DESC, tf.id ASC) AS list_scores
    FROM 
        {schema}.tf tf
    INNER JOIN {schema}.tokens t 
    ON tf.token_id = t.id
    GROUP BY 
        t.token
    ORDER BY t.token ASC
);


CREATE UNIQUE INDEX {schema}_scores_index ON {schema}.scores (token);