import collections
import os
import shutil

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from joblib import Parallel, delayed

from ..decorators import execute_with_duckdb
from ..hf import insert_documents as hf_insert_documents
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
    documents: list[dict],
    index: int,
    fields: list[str],
    key: str,
) -> None:
    """Write a parquet file with the documents to upload them."""
    documents_table = collections.defaultdict(list)

    for document in documents:
        if key is not None:
            documents_table[key].append(document[key])
        for field in fields:
            documents_table[field].append(document.get(field, ""))

    documents_path = os.path.join(".", "duckdb_tmp", "documents", f"{index}.parquet")
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
    fields: list[str] | str,
    fields_types: dict[str, str] | None = None,
    batch_size: int = 30_000,
    n_jobs: int = -1,
    config: dict | None = None,
) -> None:
    """Insert documents into the documents table in multi-threading.

    Parameters
    ----------
    database: str
        The name of the DuckDB database.
    df: pd.DataFrame
        The DataFrame containing the documents to insert.
    config: dict, optional
        The configuration options for the DuckDB connection.
    update
        If True, insert the documents and then update the index.

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
    ...     fields=["title", "text"],
    ...     df=df
    ... )

    """
    if isinstance(fields, str):
        fields = [fields]

    fields = [field for field in fields if field != "id"]

    if isinstance(df, str):
        return hf_insert_documents(
            database=database,
            schema=schema,
            key=key,
            fields=fields,
            url=df,
            config=config,
        )

    if isinstance(df, pd.DataFrame):
        df = df.to_dict(orient="records")

    create_documents(
        database=database,
        schema=schema,
        fields=fields,
        config=config,
        fields_types=fields_types,
    )

    documents_path = os.path.join(".", "duckdb_tmp", "documents")

    if os.path.exists(path=documents_path):
        shutil.rmtree(documents_path)

    os.makedirs(name=os.path.join(".", "duckdb_tmp"), exist_ok=True)
    os.makedirs(name=documents_path, exist_ok=True)

    Parallel(n_jobs=n_jobs, backend="threading")(
        delayed(function=write_parquet)(
            batch,
            index,
            fields,
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
        fields=", ".join(fields),
        df_fields=", ".join([f"df.{field}" for field in fields]),
        src_fields=", ".join([f"src.{field}" for field in fields]),
    )

    if os.path.exists(path=documents_path):
        shutil.rmtree(documents_path)


@execute_with_duckdb(
    relative_path="tables/insert/queries.sql",
)
def _insert_queries() -> None:
    """Insert queries into the queries table.

    Parameters
    ----------
    database: str
        The name of the DuckDB database.
    df: pd.DataFrame
        The DataFrame containing the queries to insert.
    config: dict, optional
        The configuration options for the DuckDB connection.

    """


def insert_queries(
    database: str,
    schema: str,
    queries: list[str],
    config: dict | None = None,
) -> None:
    """Insert queries into the queries table.

    Parameters
    ----------
    database: str
        The name of the DuckDB database.
    queries: list[str]
        The list of queries to insert.
    config: dict, optional
        The configuration options for the DuckDB connection.

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


@execute_with_duckdb(
    relative_path="tables/insert/documents_queries.sql",
)
def _insert_documents_queries() -> None:
    """Insert queries documents interactions into the documents_queries table.

    Parameters
    ----------
    database: str
        The name of the DuckDB database.
    df: pd.DataFrame
        The DataFrame containing the queries documents interactions to insert.
    config: dict, optional
        The configuration options for the DuckDB connection.

    """


def insert_documents_queries(
    database: str,
    schema: str,
    documents_queries: dict[dict[str, float]],
    config: dict | None = None,
) -> None:
    """Insert queries documents interactions.

    Parameters
    ----------
    database: str
        The name of the DuckDB database.
    documents_queries: dict
        Mapping of document ids to queries and scores.
    config: dict, optional
        The configuration options for the DuckDB connection.

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
        for query, score in document_queries.items():
            document_ids.append(document_id)
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
        os.remove("_queries.parquet")
