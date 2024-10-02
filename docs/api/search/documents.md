# documents

Search for documents in the documents table using specified queries.



## Parameters

- **database** (*str*)

    The name of the DuckDB database.

- **queries** (*str | list[str]*)

    A string or list of query strings to search for.

- **batch_size** (*int*) – defaults to `32`

    The batch size for query processing.

- **top_k** (*int*) – defaults to `10`

    The number of top documents to retrieve for each query.

- **top_k_token** (*int*) – defaults to `30000`

    The number of documents to score per token.

- **n_jobs** (*int*) – defaults to `-1`

    The number of parallel jobs to use. Default use all available processors.

- **config** (*dict | None*) – defaults to `None`

    Optional configuration for DuckDB connection settings.

- **filters** (*str | None*) – defaults to `None`

    Optional SQL filters to apply during the search.

- **order_by** (*str | None*) – defaults to `None`

- **tqdm_bar** (*bool*) – defaults to `True`

    Whether to display a progress bar when searching.



## Examples

```python
>>> from ducksearch import evaluation, upload, search

>>> documents, queries, qrels = evaluation.load_beir(
...     "scifact",
...     split="test",
... )

>>> scores = search.documents(
...     database="test.duckdb",
...     queries=queries,
...     top_k_token=1000,
... )
```

