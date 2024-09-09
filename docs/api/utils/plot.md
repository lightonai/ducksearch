# plot

Generate and display a markdown table with statistics of the specified dataset tables.



## Parameters

- **database** (*str*)

    The name of the DuckDB database.

- **config** (*None | dict*) – defaults to `None`

    Optional configuration options for the DuckDB connection.

- **tables** – defaults to `['bm25_tables.documents', 'bm25_tables.queries', 'bm25_documents.lengths', 'bm25_queries.lengths', 'bm25_tables.documents_queries']`

    A list of table names to plot statistics for. Defaults to common BM25 tables.



## Examples

```python
>>> from ducksearch import utils

>>> utils.plot(database="test.duckdb")
| Table     | Size |
|-----------|------|
| documents | 5183 |
| queries   | 300  |
```

