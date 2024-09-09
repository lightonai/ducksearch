# create_documents

Create the documents table in the DuckDB database.



## Parameters

- **database** (*str*)

- **schema** (*str*)

- **fields** (*str | list[str]*)

- **dtypes** (*dict[str, str] | None*) – defaults to `None`

- **config** (*dict | None*) – defaults to `None`



## Examples

```python
>>> from ducksearch import tables

>>> tables.create_schema(
...     database="test.duckdb",
...     schema="bm25_tables"
... )

>>> tables.create_documents(
...     database="test.duckdb",
...     schema="bm25_tables",
...     fields=["title", "text"],
...     dtypes={"text": "VARCHAR", "title": "VARCHAR"},
... )

>>> df = [
...     {"id": 1, "title": "title document 1", "text": "text document 1"},
...     {"id": 2, "title": "title document 2", "text": "text document 2"},
...     {"id": 3, "title": "title document 3", "text": "text document 3"},
... ]

>>> tables.insert_documents(
...     database="test.duckdb",
...     schema="bm25_tables",
...     key="id",
...     df=df,
...     fields=["title", "text"],
... )
```

