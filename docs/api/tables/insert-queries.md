# insert_queries

Insert a list of queries into the queries table.



## Parameters

- **database** (*str*)

    The name of the DuckDB database.

- **schema** (*str*)

    The schema in which the queries table is located.

- **queries** (*list[str]*)

    A list of query strings to insert into the table.

- **config** (*dict | None*) – defaults to `None`

    Optional configuration options for the DuckDB connection.



## Examples

```python
>>> from ducksearch import tables

>>> _ = tables.insert_queries(
...     database="test.duckdb",
...     schema="bm25_tables",
...     queries=["query 1", "query 2", "query 3"],
... )
```

