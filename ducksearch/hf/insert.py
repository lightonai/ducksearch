from ..decorators import execute_with_duckdb


@execute_with_duckdb(
    relative_path="hf/insert/documents.sql",
    fetch_df=False,
)
def _insert_documents() -> None:
    """Insert the documents from Hugging Face datasets."""


def insert_documents(
    database: str,
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
    >>> from ducksearch import hf

    >>> hf.insert_documents(
    ...     database="test.duckdb",
    ...     url="https://huggingface.co/datasets/Tevatron/scifact/resolve/main/test.jsonl.gz",
    ...     fields=["query_id", "query"],
    ...     key="query_id",
    ... )
    [(300,)]

    """
    if isinstance(fields, str):
        fields = [fields]

    fields = ", ".join([field for field in fields if field != key and field != "id"])

    return _insert_documents(
        database=database,
        url=url,
        key_field=key,
        fields=fields,
        config=config,
    )
