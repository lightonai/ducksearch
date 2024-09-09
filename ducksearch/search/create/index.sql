PRAGMA CREATE_FTS_INDEX(
    '{schema}._documents', 
    'id', 
    '_search', 
    STEMMER='{stemmer}', 
    STOPWORDS='{stopwords}', 
    IGNORE='{ignore}',
    STRIP_ACCENTS={strip_accents}, 
    LOWER={lower},
    OVERWRITE=1
);
