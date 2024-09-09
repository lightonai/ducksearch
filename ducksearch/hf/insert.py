from ..decorators import execute_with_duckdb


@execute_with_duckdb(
    relative_path="hf/insert/documents.sql",
    fetch_df=False,
)
def _insert_documents() -> None:
    """Insert the documents from Hugging Face datasets into DuckDB."""


def insert_documents(
    database: str,
    schema: str,
    key: str,
    fields: str | list[str],
    url: str,
    config: dict | None = None,
    limit: int | None = None,
) -> None:
    """Insert documents from a Hugging Face dataset into DuckDB.

    Parameters
    ----------
    database
        The name of the DuckDB database.
    schema
        The schema in which the documents table is located.
    key
        The key field that uniquely identifies each document (e.g., 'query_id').
    fields
        A list of fields to be inserted from the dataset. If a single field is provided as a string, it will be converted to a list.
    url
        The URL of the Hugging Face dataset in Parquet format.
    config
        Optional configuration options for the DuckDB connection.

    Examples
    --------
    >>> from ducksearch import upload

    >>> upload.documents(
    ...     database="test.duckdb",
    ...     documents="hf://datasets/lightonai/lighton-ms-marco-mini/train.parquet",
    ...     fields=["document_ids", "scores"],
    ...     key="query_id",
    ... )
    | Table          | Size |
    |----------------|------|
    | documents      | 19   |
    | bm25_documents | 19   |

    """
    if isinstance(fields, str):
        fields = [fields]

    fields = [field for field in fields if field != "id"]

    limit_hf = f"LIMIT {limit}" if limit is not None else ""

    if not fields:
        fields.append(key)

    return _insert_documents(
        database=database,
        schema=schema,
        url=url,
        key_field=key,
        fields=", ".join(fields),
        limit_hf=limit_hf,
        config=config,
    )
