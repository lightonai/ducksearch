# insert_documents_queries

Insert interactions between documents and queries into the documents_queries table.



## Parameters

- **database** (*str*)

    The name of the DuckDB database.

- **schema** (*str*)

    The schema in which the documents_queries table is located.

- **documents_queries** (*dict[dict[str, float]]*)

    A dictionary mapping document IDs to queries and their corresponding scores.

- **config** (*dict | None*) – defaults to `None`

    Optional configuration options for the DuckDB connection.



## Examples

```python
>>> from ducksearch import tables

>>> documents_queries = {
...     "1": {"query 1": 0.9, "query 2": 0.8},
...     "2": {"query 2": 0.9, "query 3": 3},
...     "3": {"query 1": 0.9, "query 3": 0.5},
... }

>>> tables.insert_documents_queries(
...     database="test.duckdb",
...     schema="bm25_tables",
...     documents_queries=documents_queries
... )
```

