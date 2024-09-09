from ..decorators import execute_with_duckdb


@execute_with_duckdb(
    relative_path="hf/insert/documents.sql",
    fetch_df=False,
)
def _insert_documents() -> None:
    """Insert the documents from Hugging Face datasets."""


def insert_documents(
    database: str,
    schema: str,
    key: str,
    fields: str | list[str],
    url: str,
    config: dict | None = None,
) -> None:
    """Upload documents and qrels to duckdb.

    Parameters
    ----------
    documents
        Documents.
    documents
        Set of documents to upload.

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

    if not fields:
        fields.append(key)

    return _insert_documents(
        database=database,
        schema=schema,
        url=url,
        key_field=key,
        fields=", ".join(fields),
        config=config,
    )
