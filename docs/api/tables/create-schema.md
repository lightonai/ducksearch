# create_schema

Create the specified schema in the DuckDB database.



## Parameters

- **database** (*str*)

- **schema** (*str*)

- **config** (*dict | None*) – defaults to `None`



## Examples

```python
>>> from ducksearch import tables

>>> tables.create_schema(
...     database="test.duckdb",
...     schema="bm25_tables",
... )
```

