CREATE OR REPLACE TABLE {schema}.stopwords AS (
    SELECT sw FROM parquet_scan('{parquet_file}')
);