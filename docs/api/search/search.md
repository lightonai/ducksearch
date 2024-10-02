# search

Run the search for documents or queries in parallel.



## Parameters

- **database** (*str*)

    The name of the DuckDB database.

- **schema** (*str*)

    The name of the schema containing the indexed documents or queries.

- **source_schema** (*str*)

    The name of the schema containing the original documents or queries.

- **source** (*str*)

    The table to search (either 'documents' or 'queries').

- **queries** (*str | list[str]*)

    A string or list of query strings to search for.

- **batch_size** (*int*) – defaults to `64`

    The batch size for query processing.

- **top_k** (*int*) – defaults to `10`

    The number of top results to retrieve for each query.

- **top_k_token** (*int*) – defaults to `30000`

    The number of documents to score per token.

- **n_jobs** (*int*) – defaults to `-1`

    The number of parallel jobs to use. Default use available processors.

- **config** (*dict | None*) – defaults to `None`

    Optional configuration for DuckDB connection settings.

- **filters** (*str | None*) – defaults to `None`

    Optional SQL filters to apply during the search.

- **order_by** (*str | None*) – defaults to `None`

- **tqdm_bar** (*bool*) – defaults to `True`

    Whether to display a progress bar when searching.



## Examples

```python
>>> from ducksearch import search

>>> documents = search.search(
...     database="test.duckdb",
...     source_schema="bm25_tables",
...     schema="bm25_documents",
...     source="documents",
...     queries="random query",
...     top_k_token=10_000,
...     top_k=10,
... )

>>> assert len(documents) == 10
```

