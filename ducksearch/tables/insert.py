import collections
import os
import shutil

import pyarrow as pa
import pyarrow.parquet as pq
from joblib import Parallel, delayed

from ..decorators import execute_with_duckdb
from ..utils import batchify
from .create import (
    create_documents,
    create_documents_queries,
    create_queries,
)


@execute_with_duckdb(
    relative_path="tables/insert/documents.sql",
)
def _insert_documents() -> None:
    """Insert documents into the documents table.

    Parameters
    ----------
    database: str
        The name of the DuckDB database.
    config: dict, optional
        The configuration options for the DuckDB connection.
    """


def write_parquet(
    database: str,
    documents: list[dict],
    index: int,
    fields: list[str],
    key: str,
) -> None:
    """Write a parquet file with document data for upload.

    Parameters
    ----------
    documents
        A list of dictionaries representing the documents to be written to the parquet file.
    index
        The index of the current batch being processed.
    fields
        The list of document fields to be written to the parquet file.
    key
        The key field to uniquely identify each document.

    Notes
    -----
    This function writes documents to a temporary parquet file in preparation for bulk uploading into the database.
    """
    documents_table = collections.defaultdict(list)

    fields = set()
    for document in documents:
        for field in document.keys():
            if field != "id":
                fields.add(field)

    for document in documents:
        documents_table["id"].append(document[key])
        for field in fields:
            documents_table[field].append(document.get(field, None))

    documents_path = os.path.join(
        ".", f"{database}_tmp", "documents", f"{index}.parquet"
    )
    documents_table = pa.Table.from_pydict(documents_table)

    pq.write_table(
        documents_table,
        documents_path,
        compression="snappy",
    )


def insert_documents(
    database: str,
    schema: str,
    df: list[dict] | str,
    key: str,
    columns: list[str] | str,
    dtypes: dict[str, str] | None = None,
    batch_size: int = 30_000,
    n_jobs: int = -1,
    config: dict | None = None,
    limit: int | None = None,
) -> None:
    """Insert documents into the documents table with optional multi-threading.

    Parameters
    ----------
    database
        The name of the DuckDB database.
    schema
        The schema in which the documents table is located.
    df
        The list of document dictionaries or a string (URL) for a Hugging Face dataset to insert.
    key
        The field that uniquely identifies each document (e.g., 'id').
    columns
        The list of document fields to insert. Can be a string if inserting a single field.
    dtypes
        Optional dictionary specifying the DuckDB type for each field. Defaults to 'VARCHAR' for all unspecified fields.
    batch_size
        The number of documents to insert in each batch.
    n_jobs
        Number of parallel jobs to use for inserting documents. Default use all available processors.
    config
        Optional configuration options for the DuckDB connection.

    Examples
    --------
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

    """
    columns = [column for column in columns if column != "id"]

    create_documents(
        database=database,
        schema=schema,
        columns=columns,
        config=config,
        dtypes=dtypes,
    )

    documents_path = os.path.join(".", f"{database}_tmp", "documents")

    if os.path.exists(path=documents_path):
        shutil.rmtree(documents_path)

    os.makedirs(name=os.path.join(".", f"{database}_tmp"), exist_ok=True)
    os.makedirs(name=documents_path, exist_ok=True)

    Parallel(n_jobs=n_jobs, backend="threading")(
        delayed(function=write_parquet)(
            database,
            batch,
            index,
            columns,
            key,
        )
        for index, batch in enumerate(
            iterable=batchify(X=df, batch_size=batch_size, tqdm_bar=False)
        )
    )

    _insert_documents(
        database=database,
        schema=schema,
        parquet_files=os.path.join(documents_path, "*.parquet"),
        config=config,
        key_field=f"df.{key}",
        fields=", ".join(columns),
        df_fields=", ".join([f"df.{field}" for field in columns]),
        src_fields=", ".join([f"src.{field}" for field in columns]),
    )

    if os.path.exists(path=documents_path):
        shutil.rmtree(documents_path)

    if os.path.exists(path=os.path.join(".", f"{database}_tmp")):
        shutil.rmtree(os.path.join(".", f"{database}_tmp"))


@execute_with_duckdb(
    relative_path="tables/insert/queries.sql",
)
def _insert_queries() -> None:
    """Insert queries into the queries table.

    Parameters
    ----------
    database: str
        The name of the DuckDB database.
    config: dict, optional
        The configuration options for the DuckDB connection.
    """


def insert_queries(
    database: str,
    schema: str,
    queries: list[str],
    config: dict | None = None,
) -> None:
    """Insert a list of queries into the queries table.

    Parameters
    ----------
    database
        The name of the DuckDB database.
    schema
        The schema in which the queries table is located.
    queries
        A list of query strings to insert into the table.
    config
        Optional configuration options for the DuckDB connection.

    Examples
    --------
    >>> from ducksearch import tables

    >>> _ = tables.insert_queries(
    ...     database="test.duckdb",
    ...     schema="bm25_tables",
    ...     queries=["query 1", "query 2", "query 3"],
    ... )
    """
    create_queries(database=database, schema=schema, config=config)

    table = pa.Table.from_pydict({"query": queries})

    pq.write_table(
        table,
        "_queries.parquet",
        compression="snappy",
    )

    _insert_queries(
        database=database,
        schema=schema,
        parquet_file="_queries.parquet",
        config=config,
    )

    if os.path.exists("_queries.parquet"):
        os.remove("_queries.parquet")


@execute_with_duckdb(
    relative_path="tables/insert/documents_queries.sql",
)
def _insert_documents_queries() -> None:
    """Insert query-document interactions into the documents_queries table.

    Parameters
    ----------
    database: str
        The name of the DuckDB database.
    config: dict, optional
        The configuration options for the DuckDB connection.
    """


def insert_documents_queries(
    database: str,
    schema: str,
    documents_queries: dict[dict[str, float]],
    config: dict | None = None,
) -> None:
    """Insert interactions between documents and queries into the documents_queries table.

    Parameters
    ----------
    database
        The name of the DuckDB database.
    schema
        The schema in which the documents_queries table is located.
    documents_queries
        A dictionary mapping document IDs to queries and their corresponding scores.
    config
        Optional configuration options for the DuckDB connection.

    Examples
    --------
    >>> from ducksearch import tables

    >>> documents_queries = {
    ...     "1": {"query 1": 0.9, "query 2": 0.8},
    ...     "2": {"query 2": 0.9, "query 3": 3},
    ...     "3": {"query 1": 0.9, "query 3": 0.5},
    ... }

    >>> tables.insert_documents_queries(
    ...     database="test.duckdb",
    ...     schema="bm25_tables",
    ...     documents_queries=documents_queries
    ... )

    """
    create_queries(database=database, schema=schema, config=config)

    queries = set()
    for _, document_queries in documents_queries.items():
        for query in document_queries:
            queries.add(query)

    insert_queries(
        database=database, schema=schema, queries=list(queries), config=config
    )
    create_documents_queries(database=database, schema=schema, config=config)

    document_ids, queries, scores = [], [], []
    for document_id, document_queries in documents_queries.items():
        if isinstance(document_queries, list):
            document_queries = {query: 1.0 for query in document_queries}

        for query, score in document_queries.items():
            document_ids.append(str(document_id))
            queries.append(query)
            scores.append(score)

    table = pa.Table.from_pydict(
        {
            "document_id": document_ids,
            "query": queries,
            "score": scores,
        }
    )

    pq.write_table(
        table,
        "_documents_queries.parquet",
        compression="snappy",
    )

    _insert_documents_queries(
        database=database,
        schema=schema,
        parquet_file="_documents_queries.parquet",
        config=config,
    )

    if os.path.exists("_documents_queries.parquet"):
        os.remove("_documents_queries.parquet")
