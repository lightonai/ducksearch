# insert_documents

Insert documents into the documents table with optional multi-threading.



## Parameters

- **database** (*str*)

    The name of the DuckDB database.

- **schema** (*str*)

    The schema in which the documents table is located.

- **df** (*list[dict] | str*)

    The list of document dictionaries or a string (URL) for a Hugging Face dataset to insert.

- **key** (*str*)

    The field that uniquely identifies each document (e.g., 'id').

- **columns** (*list[str] | str*)

    The list of document fields to insert. Can be a string if inserting a single field.

- **dtypes** (*dict[str, str] | None*) – defaults to `None`

    Optional dictionary specifying the DuckDB type for each field. Defaults to 'VARCHAR' for all unspecified fields.

- **batch_size** (*int*) – defaults to `30000`

    The number of documents to insert in each batch.

- **n_jobs** (*int*) – defaults to `-1`

    Number of parallel jobs to use for inserting documents. Default use all available processors.

- **config** (*dict | None*) – defaults to `None`

    Optional configuration options for the DuckDB connection.

- **limit** (*int | None*) – defaults to `None`



## Examples

```python
>>> from ducksearch import tables

>>> df = [
...     {"id": 1, "title": "title document 1", "text": "text document 1"},
...     {"id": 2, "title": "title document 2", "text": "text document 2"},
...     {"id": 3, "title": "title document 3", "text": "text document 3"},
... ]

>>> _ = tables.insert_documents(
...     database="test.duckdb",
...     schema="bm25_tables",
...     key="id",
...     columns=["title", "text"],
...     df=df
... )
```

