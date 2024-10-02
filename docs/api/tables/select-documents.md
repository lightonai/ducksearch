# select_documents

Select all documents from the documents table.



## Parameters

- **database** (*str*)

    The name of the DuckDB database.

- **schema** (*str*)

    The schema where the documents table is located.

- **limit** (*int | None*) – defaults to `None`

- **config** (*dict | None*) – defaults to `None`

    Optional configuration options for the DuckDB connection.



## Examples

```python
>>> from ducksearch import tables

>>> documents = tables.select_documents(
...     database="test.duckdb",
...     schema="bm25_tables",
... )

>>> assert len(documents) == 3
```

