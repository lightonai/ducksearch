PRAGMA create_fts_index(
    '{schema}._queries', 
    'query', 
    'query', 
    stemmer='{stemmer}', 
    stopwords='{stopwords}', 
    ignore='{ignore}',
    strip_accents={strip_accents}, 
    lower={lower},
    overwrite=1
);