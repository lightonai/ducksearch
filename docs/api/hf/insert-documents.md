# insert_documents

Insert documents from a Hugging Face dataset into DuckDB.



## Parameters

- **database** (*str*)

    The name of the DuckDB database.

- **schema** (*str*)

    The schema in which the documents table is located.

- **key** (*str*)

    The key field that uniquely identifies each document (e.g., 'query_id').

- **url** (*str*)

    The URL of the Hugging Face dataset in Parquet format.

- **config** (*dict | None*) – defaults to `None`

    Optional configuration options for the DuckDB connection.

- **limit** (*int | None*) – defaults to `None`

- **dtypes** (*dict | None*) – defaults to `None`



## Examples

```python
>>> from ducksearch import upload

>>> upload.documents(
...     database="test.duckdb",
...     documents="hf://datasets/lightonai/lighton-ms-marco-mini/queries.parquet",
...     key="query_id",
...     fields=["query_id", "text"],
... )
| Table          | Size |
|----------------|------|
| documents      | 19   |
| bm25_documents | 19   |

>>> upload.documents(
...     database="test.duckdb",
...     documents="hf://datasets/lightonai/lighton-ms-marco-mini/documents.parquet",
...     key="document_id",
...     fields=["document_id", "text"],
... )
| Table          | Size |
|----------------|------|
| documents      | 51   |
| bm25_documents | 51   |
```

