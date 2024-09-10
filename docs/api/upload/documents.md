# documents

Upload documents to DuckDB, create necessary schema, and index using BM25.



## Parameters

- **database** (*str*)

    Name of the DuckDB database.

- **key** (*str*)

    Key identifier for the documents. The key will be renamed to `id` in the database.

- **fields** (*str | list[str]*)

    List of fields to upload from each document. If a single field is provided as a string, it will be converted to a list.

- **documents** (*list[dict] | str*)

    Documents to upload. Can be a list of dictionaries or a Hugging Face (HF) URL string pointing to a dataset.

- **k1** (*float*) – defaults to `1.5`

    BM25 k1 parameter, controls term saturation.

- **b** (*float*) – defaults to `0.75`

    BM25 b parameter, controls document length normalization.

- **stemmer** (*str*) – defaults to `porter`

    Stemming algorithm to use (e.g., 'porter'). The type of stemmer to be used. One of 'arabic', 'basque', 'catalan', 'danish', 'dutch', 'english', 'finnish', 'french', 'german', 'greek', 'hindi', 'hungarian', 'indonesian', 'irish', 'italian', 'lithuanian', 'nepali', 'norwegian', 'porter', 'portuguese', 'romanian', 'russian', 'serbian', 'spanish', 'swedish', 'tamil', 'turkish', or 'none' if no stemming is to be used.

- **stopwords** (*str | list[str]*) – defaults to `None`

    List of stopwords to exclude from indexing.  Can be a custom list or a language string.

- **ignore** (*str*) – defaults to `(\.|[^a-z])+`

    Regular expression pattern to ignore characters when indexing. Default ignore punctuation and non-alphabetic characters.

- **strip_accents** (*bool*) – defaults to `True`

    Whether to remove accents from characters during indexing.

- **lower** (*bool*) – defaults to `True`

- **batch_size** (*int*) – defaults to `30000`

    Number of documents to process per batch.

- **n_jobs** (*int*) – defaults to `-1`

    Number of parallel jobs to use for uploading documents. Default use all available processors.

- **dtypes** (*dict[str, str] | None*) – defaults to `None`

- **config** (*dict | None*) – defaults to `None`

    Optional configuration dictionary for the DuckDB connection and other settings.

- **limit** (*int | None*) – defaults to `None`




