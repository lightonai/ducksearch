import os

import pyarrow as pa
import pyarrow.parquet as pq

from ..decorators import execute_with_duckdb
from ..utils import plot


@execute_with_duckdb(
    relative_path="delete/delete/documents.sql",
)
def _drop_documents() -> None:
    """Delete documents from the documents table in DuckDB."""


def documents(
    database: str,
    ids: list[str],
    schema: str = "bm25_tables",
    config: dict | None = None,
) -> None:
    """Delete specified documents from the documents table.

    Parameters
    ----------
    database
        The name of the DuckDB database.
    keys
        A list of document IDs to delete.
    schema
        The schema where the documents table is located.
    config
        Optional configuration options for the DuckDB connection.

    Returns
    -------
    None
        The function deletes the specified documents and updates the plots.

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
    ...    ids=[1, 2],
    ... )
    | Table          | Size |
    |----------------|------|
    | documents      | 1    |
    | bm25_documents | 3    |

    """
    # Convert the list of document keys into a pyarrow Table for deletion
    documents_ids = pa.Table.from_pydict({"id": ids})

    # Write the document IDs to a parquet file for deletion
    pq.write_table(
        documents_ids,
        "_documents_ids.parquet",
        compression="snappy",
    )

    # Call the SQL function to delete the documents
    _drop_documents(
        database=database,
        schema=schema,
        parquet_file="_documents_ids.parquet",
        config=config,
    )

    if os.path.exists("_documents_ids.parquet"):
        os.remove("_documents_ids.parquet")

    # Plot the current state of the tables after deletion
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
