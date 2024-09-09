# create_queries

Create the queries table in the DuckDB database.





## Examples

```python
>>> from ducksearch import tables

>>> tables.create_schema(
...     database="test.duckdb",
...     schema="bm25_tables"
... )

>>> tables.create_queries(
...     database="test.duckdb",
...     schema="bm25_tables",
... )
```

