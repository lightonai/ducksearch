import pyarrow as pa
import pyarrow.parquet as pq

from ..decorators import execute_with_duckdb
from ..utils import plot


@execute_with_duckdb(
    relative_path="delete/delete/documents.sql",
)
def _drop_documents() -> None:
    """Delete documents from the documents table."""


def documents(
    database: str,
    keys: list[str],
    schema: str = "bm25_tables",
    config: dict | None = None,
) -> None:
    """Delete documents from the documents table.

    Parameters
    ----------
    database
        The name of the DuckDB database.
    keys
        Document keys to delete.
    config
        The configuration options for the DuckDB connection.

    Examples
    --------
    >>> from ducksearch import upload, delete

    >>> documents = [
    ...     {"id": 1, "title": "Document 1", "text": "This is the text of document 1."},
    ...     {"id": 2, "title": "Document 2", "text": "This is the text of document 2."},
    ...     {"id": 3, "title": "Document 3", "text": "This is the text of document 3."},
    ... ]

    >>> upload.documents(
    ...     database="test.duckdb",
    ...     key="id",
    ...     fields=["title", "text"],
    ...     documents=documents,
    ... )
    | Table          | Size |
    |----------------|------|
    | documents      | 3    |
    | bm25_documents | 3    |

    >>> delete.documents(
    ...    database="test.duckdb",
    ...    keys=[1, 2],
    ... )
    | Table          | Size |
    |----------------|------|
    | documents      | 1    |
    | bm25_documents | 3    |

    """
    documents_ids = pa.Table.from_pydict({"id": keys})

    pq.write_table(
        documents_ids,
        "_documents_ids.parquet",
        compression="snappy",
    )

    _drop_documents(
        database=database,
        schema=schema,
        parquet_file="_documents_ids.parquet",
        config=config,
    )

    return plot(
        database=database,
        config=config,
        tables=[
            f"{schema}.documents",
            f"{schema}.queries",
            "bm25_documents.docs",
            "bm25_queries.docs",
            "bm25_tables.documents_queries",
        ],
    )
