# select_documents_columns

Select the column names from the documents table, excluding the 'bm25id' column.



## Parameters

- **database** (*str*)

    The name of the DuckDB database.

- **schema** (*str*)

    The schema where the documents table is located.

- **config** (*dict | None*) – defaults to `None`

    Optional configuration options for the DuckDB connection.



## Examples

```python
>>> from ducksearch import tables

>>> tables.select_documents_columns(
...     database="test.duckdb",
...     schema="bm25_tables",
... )
['id', 'title', 'text']
```

