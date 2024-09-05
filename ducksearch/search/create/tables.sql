-- Create the dedicated schema.
CREATE schema IF NOT EXISTS {schema};

-- Lengths table is used to store the length of each document.
CREATE TABLE IF NOT EXISTS {schema}.LENGTHS (
    ID VARCHAR PRIMARY KEY,
    DOCUMENT_LENGTH INT
);

-- Tokens scores table is used to store the scores of each token in each document.
CREATE TABLE IF NOT EXISTS {schema}.TF (
    ID VARCHAR,
    TOKEN_ID INT,
    TF INT,
    SCORE FLOAT DEFAULT 0.0
);

CREATE SEQUENCE IF NOT EXISTS {schema}.SEQ_TOKENS_ID START 1;

-- Tokens table is used to store the tokens.
CREATE TABLE IF NOT EXISTS {schema}.TOKENS (
    ID INT PRIMARY KEY DEFAULT NEXTVAL('{schema}.SEQ_TOKENS_ID'),
    TOKEN VARCHAR
);

