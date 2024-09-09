INSERT INTO {schema}.scores (term, list_docids, list_scores)

WITH _terms AS (
    SELECT termid FROM parquet_scan('{parquet_file}')
),

_unfiltered_terms_df AS (
    SELECT 
        d.termid,
        d.term,
        d.df,
        sw.sw IS NOT NULL AS is_stopword
    FROM {schema}.dict d
    INNER JOIN _terms t
    ON d.termid = t.termid
    LEFT JOIN {schema}.stopwords sw
    ON d.term = sw.sw
),

_terms_df AS (
    SELECT
        termid,
        term,
        df
    FROM _unfiltered_terms_df
    WHERE is_stopword = FALSE
),

_documents_lengths AS (
    SELECT
        docid,
        len
    FROM {schema}.docs
),

_documents_terms_df AS (
    SELECT
        s.docid,
        s.termid,
        s.tf
    FROM {schema}.terms s
    INNER JOIN _terms t
    ON s.termid = t.termid
),

_scores AS (
    SELECT 
        tf.docid,
        tf.termid,
        tf.tf *
        LOG(
            (
                ({num_docs} - tdf.df + 0.5) /
                (tdf.df + 0.5)
            ) + 1
        ) *
        (1.0 / (tf.tf + {k1} * (1 - {b} + {b} * (dl.len / {avgdl})))) AS score
    FROM 
        _documents_terms_df tf
    JOIN
        _documents_lengths dl ON dl.docid = tf.docid
    JOIN 
        _terms_df tdf ON tdf.termid = tf.termid
),

_list_scores AS (
    SELECT
        s.termid,
        LIST(d.docid ORDER BY s.score DESC, s.docid ASC) AS list_docids,
        LIST(s.score ORDER BY s.score DESC, s.docid ASC) AS list_scores
    FROM _scores s
    INNER JOIN
        {schema}.docs d
        ON s.docid = d.docid
    GROUP BY
        s.termid
)

SELECT
    d.term,
    ls.list_docids,
    ls.list_scores
FROM _list_scores ls
JOIN _terms_df d
ON ls.termid = d.termid;




