# queries

Upload queries to DuckDB, map documents to queries, and index using BM25.



## Parameters

- **database** (*str*)

    Name of the DuckDB database.

- **queries** (*list[str] | None*) – defaults to `None`

    List of queries to upload. Each query is a string.

- **documents_queries** (*dict[list]*) – defaults to `None`

    Dictionary mapping document IDs to a list of queries.

- **k1** (*float*) – defaults to `1.5`

    BM25 k1 parameter, controls term saturation.

- **b** (*float*) – defaults to `0.75`

    BM25 b parameter, controls document length normalization.

- **stemmer** (*str*) – defaults to `porter`

    Stemming algorithm to use (e.g., 'porter'). The type of stemmer to be used. One of 'arabic', 'basque', 'catalan', 'danish', 'dutch', 'english', 'finnish', 'french', 'german', 'greek', 'hindi', 'hungarian', 'indonesian', 'irish', 'italian', 'lithuanian', 'nepali', 'norwegian', 'porter', 'portuguese', 'romanian', 'russian', 'serbian', 'spanish', 'swedish', 'tamil', 'turkish', or 'none' if no stemming is to be used.

- **stopwords** (*str | list[str]*) – defaults to `english`

    List of stopwords to exclude from indexing. Default can be a custom list or a language string.

- **ignore** (*str*) – defaults to `(\.|[^a-z])+`

    Regular expression pattern to ignore characters when indexing. Default ignore punctuation and non-alphabetic characters.

- **strip_accents** (*bool*) – defaults to `True`

    Whether to remove accents from characters during indexing.

- **lower** (*bool*) – defaults to `True`

- **batch_size** (*int*) – defaults to `30000`

    Number of queries to process per batch.

- **config** (*dict | None*) – defaults to `None`

    Optional configuration dictionary for the DuckDB connection and other settings.




