# create_documents_queries

Create the documents_queries table in the DuckDB database.





## Examples

```python
>>> from ducksearch import tables

>>> tables.create_schema(
...     database="test.duckdb",
...     schema="bm25_tables"
... )

>>> tables.create_documents_queries(
...     database="test.duckdb",
...     schema="bm25_tables",
... )
```

