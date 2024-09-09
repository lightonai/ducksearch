# select_queries

Select all queries from the queries table.





## Examples

```python
>>> from ducksearch import tables

>>> queries = tables.select_queries(
...     database="test.duckdb",
...     schema="bm25_tables",
... )

>>> assert len(queries) == 3
```

