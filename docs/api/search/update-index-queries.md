# update_index_queries

Update the BM25 search index for queries.



## Parameters

- **database** (*str*)

    The name of the DuckDB database.

- **k1** (*float*) – defaults to `1.5`

    The BM25 k1 parameter, controls term saturation.

- **b** (*float*) – defaults to `0.75`

    The BM25 b parameter, controls document length normalization.

- **stemmer** (*str*) – defaults to `porter`

    The stemming algorithm to use (e.g., 'porter').

- **stopwords** (*str | list[str]*) – defaults to `None`

    The list of stopwords to exclude from indexing. Can be a list or a string specifying the language (e.g., "english").

- **ignore** (*str*) – defaults to `(\.|[^a-z])+`

    A regex pattern to ignore characters during tokenization. Default ignores punctuation and non-alphabetic characters.

- **strip_accents** (*bool*) – defaults to `True`

    Whether to remove accents from characters during indexing.

- **lower** (*bool*) – defaults to `True`

- **batch_size** (*int*) – defaults to `10000`

    The number of queries to process per batch.

- **config** (*dict | None*) – defaults to `None`

    Optional configuration settings for the DuckDB connection.



## Examples

```python
>>> from ducksearch import evaluation, upload, search

>>> documents, queries, qrels = evaluation.load_beir("scifact", split="test")

>>> upload.queries(
...     database="test.duckdb",
...     queries=queries,
...     documents_queries=qrels,
... )
| Table             | Size |
|-------------------|------|
| documents         | 5183 |
| queries           | 300  |
| bm25_documents    | 5183 |
| bm25_queries      | 300  |
| documents_queries | 339  |
```

