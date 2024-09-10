# queries

Search for queries in the queries table using specified queries.



## Parameters

- **database** (*str*)

    The name of the DuckDB database.

- **queries** (*str | list[str]*)

    A string or list of query strings to search for.

- **batch_size** (*int*) – defaults to `32`

    The batch size for query processing.

- **top_k** (*int*) – defaults to `10`

    The number of top matching queries to retrieve.

- **top_k_token** (*int*) – defaults to `30000`

    The number of documents to score per token.

- **n_jobs** (*int*) – defaults to `-1`

    The number of parallel jobs to use. Default use all available processors.

- **config** (*dict | None*) – defaults to `None`

    Optional configuration for DuckDB connection settings.

- **filters** (*str | None*) – defaults to `None`

    Optional SQL filters to apply during the search.



## Examples

```python
>>> from ducksearch import evaluation, upload, search

>>> documents, queries, qrels = evaluation.load_beir("scifact", split="test")

>>> scores = search.queries(database="test.duckdb", queries=queries)

>>> n = sum(1 for sample, query in zip(scores, queries) if sample[0]["query"] == query)
>>> assert n >= 290
```

