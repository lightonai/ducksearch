PRAGMA create_fts_index(
    '{schema}._documents', 
    'id', 
    '_search', 
    stemmer='{stemmer}', 
    stopwords='{stopwords}', 
    ignore='{ignore}',
    strip_accents={strip_accents}, 
    lower={lower},
    overwrite=1
);

