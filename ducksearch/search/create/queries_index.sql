PRAGMA CREATE_FTS_INDEX(
    '{schema}._queries_{random_hash}', 
    'query', 
    'query', 
    STEMMER='{stemmer}', 
    STOPWORDS='{stopwords}', 
    IGNORE='{ignore}',
    STRIP_ACCENTS={strip_accents}, 
    LOWER={lower},
    OVERWRITE=1
);