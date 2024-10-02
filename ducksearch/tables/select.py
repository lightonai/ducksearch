import pandas as pd

from ..decorators import execute_with_duckdb


@execute_with_duckdb(
    relative_path="tables/select/documents.sql",
    read_only=True,
    fetch_df=True,
)
def _select_documents() -> list[dict]:
    """Select all documents from the documents table.

    Returns
    -------
    list[dict]
        A list of dictionaries representing the documents.

    Examples
    --------
    >>> from ducksearch import tables

    >>> documents = tables.select_documents(
    ...     database="test.duckdb",
    ...     schema="bm25_tables",
    ... )

    >>> assert len(documents) == 3
    """


def select_documents(
    database: str,
    schema: str,
    limit: int | None = None,
    config: dict | None = None,
) -> list[dict]:
    """Select all documents from the documents table.

    Parameters
    ----------
    database
        The name of the DuckDB database.
    schema
        The schema where the documents table is located.
    config
        Optional configuration options for the DuckDB connection.

    Returns
    -------
    list[dict]
        A list of dictionaries representing the documents.

    Examples
    --------
    >>> from ducksearch import tables

    >>> documents = tables.select_documents(
    ...     database="test.duckdb",
    ...     schema="bm25_tables",
    ... )

    >>> assert len(documents) == 3
    """
    return pd.DataFrame(
        _select_documents(
            database=database,
            schema=schema,
            limit="" if limit is None else f"LIMIT {limit}",
            config=config,
        )
    )


@execute_with_duckdb(
    relative_path="tables/select/queries.sql",
    read_only=True,
    fetch_df=True,
)
def select_queries() -> list[dict]:
    """Select all queries from the queries table.

    Returns
    -------
    list[dict]
        A list of dictionaries representing the queries.

    Examples
    --------
    >>> from ducksearch import tables

    >>> queries = tables.select_queries(
    ...     database="test.duckdb",
    ...     schema="bm25_tables",
    ... )

    >>> assert len(queries) == 3
    """


@execute_with_duckdb(
    relative_path="tables/select/columns.sql",
    read_only=True,
    fields=["column"],
)
def select_columns() -> list[dict]:
    """Retrieve the list of columns from a specified table.

    Returns
    -------
    list[dict]
        A list of dictionaries containing the column names of the table.
    """


def select_documents_columns(
    database: str,
    schema: str,
    config: dict | None = None,
) -> list[str]:
    """Select the column names from the documents table, excluding the 'bm25id' column.

    Parameters
    ----------
    database
        The name of the DuckDB database.
    schema
        The schema where the documents table is located.
    config
        Optional configuration options for the DuckDB connection.

    Returns
    -------
    list[str]
        A list of column names from the documents table.

    Examples
    --------
    >>> from ducksearch import tables

    >>> tables.select_documents_columns(
    ...     database="test.duckdb",
    ...     schema="bm25_tables",
    ... )
    ['id', 'title', 'text']
    """
    return [
        column["column"]
        for column in select_columns(
            database=database, schema=schema, table_name="documents", config=config
        )
        if column["column"] != "bm25id"
    ]
