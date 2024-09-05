import pandas as pd

from ..decorators import execute_with_duckdb
from ..hf import insert_documents as hf_insert_documents
from .create import (
    create_documents,
    create_documents_queries,
    create_queries,
)


@execute_with_duckdb(
    relative_path="tables/insert/documents.sql",
    df_name="df_documents",
)
def _insert_documents() -> None:
    """Insert documents into the documents table.

    Parameters
    ----------
    database: str
        The name of the DuckDB database.
    df: pd.DataFrame
        The DataFrame containing the documents to insert.
    config: dict, optional
        The configuration options for the DuckDB connection.

    """


def insert_documents(
    database: str,
    df: pd.DataFrame | list[dict] | str,
    key: str,
    fields: list[str] | str,
    fields_types: dict[str, str] | None = None,
    config: dict | None = None,
) -> None:
    """Insert documents into the documents table.

    Parameters
    ----------
    database: str
        The name of the DuckDB database.
    df: pd.DataFrame
        The DataFrame containing the documents to insert.
    config: dict, optional
        The configuration options for the DuckDB connection.

    Examples
    --------
    >>> from ducksearch import tables
    >>> import pandas as pd

    >>> df = pd.DataFrame([
    ...     {"id": 1, "title": "title document 1", "text": "text document 1"},
    ...     {"id": 2, "title": "title document 2", "text": "text document 2"},
    ...     {"id": 3, "title": "title document 3", "text": "text document 3"},
    ... ])

    >>> _ = tables.insert_documents(
    ...     database="test.duckdb",
    ...     key="id",
    ...     fields=["title", "text"],
    ...     df=df
    ... )

    """
    if isinstance(fields, str):
        fields = [fields]

    fields = [field for field in fields if field.lower() != "id"]

    if isinstance(df, list):
        df = pd.DataFrame(data=df)

    if isinstance(df, str):
        return hf_insert_documents(
            database=database,
            key=key,
            fields=fields,
            url=df,
            config=config,
        )

    create_documents(
        database=database,
        fields=fields,
        config=config,
        fields_types=fields_types,
    )

    return _insert_documents(
        database=database,
        df=df,
        config=config,
        key_field=f"df.{key}",
        fields=", ".join(fields),
        df_fields=", ".join([f"df.{field}" for field in fields]),
    )


@execute_with_duckdb(
    relative_path="tables/insert/queries.sql",
    df_name="df_queries",
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
    ...     queries=["query 1", "query 2", "query 3"],
    ... )

    """
    create_queries(database=database, config=config)

    return _insert_queries(
        database=database, df=pd.DataFrame(data={"query": queries}), config=config
    )


@execute_with_duckdb(
    relative_path="tables/insert/documents_queries.sql",
    df_name="df_documents_queries",
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
    >>> import pandas as pd

    >>> documents_queries = {
    ...     "1": {"query 1": 0.9, "query 2": 0.8},
    ...     "2": {"query 2": 0.9, "query 3": 3},
    ...     "3": {"query 1": 0.9, "query 3": 0.5},
    ... }

    >>> tables.insert_documents_queries(
    ...     database="test.duckdb",
    ...     documents_queries=documents_queries
    ... )
    [(6,)]

    """
    create_queries(database=database, config=config)

    queries = set()
    for _, document_queries in documents_queries.items():
        for query in document_queries:
            queries.add(query)

    insert_queries(database=database, queries=list(queries), config=config)
    create_documents_queries(database=database, config=config)

    df = []

    for document_id, document_queries in documents_queries.items():
        df.extend(
            [
                {
                    "document_id": document_id,
                    "query": query,
                    "score": score,
                }
                for query, score in document_queries.items()
            ]
        )

    df = pd.DataFrame(data=df)

    return _insert_documents_queries(database=database, df=df, config=config)
