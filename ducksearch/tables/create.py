from ..decorators import execute_with_duckdb


@execute_with_duckdb(
    relative_path="tables/create/documents.sql",
)
def _create_documents() -> None:
    """Create the documents table in the DuckDB database.

    Parameters
    ----------
    database: str
        The name of the DuckDB database.
    config: dict, optional
        The configuration options for the DuckDB connection.
    """


@execute_with_duckdb(
    relative_path="tables/create/schema.sql",
)
def _create_schema() -> None:
    """Create a schema in the DuckDB database.

    Parameters
    ----------
    database: str
        The name of the DuckDB database.
    schema: str
        The schema to be created in the database.
    config: dict, optional
        The configuration options for the DuckDB connection.
    """


def create_schema(
    database: str,
    schema: str,
    config: dict | None = None,
) -> None:
    """Create the specified schema in the DuckDB database.

    Parameters
    ----------
    database: str
        The name of the DuckDB database.
    schema: str
        The schema to create within the DuckDB database.
    config: dict, optional
        The configuration options for the DuckDB connection.

    Examples
    --------
    >>> from ducksearch import tables

    >>> tables.create_schema(
    ...     database="test.duckdb",
    ...     schema="bm25_tables",
    ... )
    """
    return _create_schema(database=database, schema=schema, config=config)


def create_documents(
    database: str,
    schema: str,
    columns: str | list[str],
    dtypes: dict[str, str] | None = None,
    config: dict | None = None,
) -> None:
    """Create the documents table in the DuckDB database.

    Parameters
    ----------
    database: str
        The name of the DuckDB database.
    schema: str
        The schema in which to create the documents table.
    columns: str or list[str]
        The list of columns for the documents table. If a string is provided, it will be converted into a list.
    dtypes: dict[str, str], optional
        A dictionary specifying field names as keys and their DuckDB types as values. Defaults to 'VARCHAR' if not provided.
    config: dict, optional
        The configuration options for the DuckDB connection.

    Examples
    --------
    >>> from ducksearch import tables

    >>> tables.create_schema(
    ...     database="test.duckdb",
    ...     schema="bm25_tables"
    ... )

    >>> tables.create_documents(
    ...     database="test.duckdb",
    ...     schema="bm25_tables",
    ...     columns=["title", "text"],
    ...     dtypes={"text": "VARCHAR", "title": "VARCHAR"},
    ... )

    >>> df = [
    ...     {"id": 1, "title": "title document 1", "text": "text document 1"},
    ...     {"id": 2, "title": "title document 2", "text": "text document 2"},
    ...     {"id": 3, "title": "title document 3", "text": "text document 3"},
    ... ]

    >>> tables.insert_documents(
    ...     database="test.duckdb",
    ...     schema="bm25_tables",
    ...     key="id",
    ...     df=df,
    ...     columns=["title", "text"],
    ... )
    """
    if not dtypes:
        dtypes = {}

    return _create_documents(
        database=database,
        schema=schema,
        fields=", ".join(
            [f"{field} {dtypes.get(field, 'VARCHAR')}" for field in columns]
        ),
        config=config,
    )


@execute_with_duckdb(
    relative_path="tables/create/queries.sql",
)
def create_queries() -> None:
    """Create the queries table in the DuckDB database.

    Parameters
    ----------
    database: str
        The name of the DuckDB database.
    config: dict, optional
        The configuration options for the DuckDB connection.

    Examples
    --------
    >>> from ducksearch import tables

    >>> tables.create_schema(
    ...     database="test.duckdb",
    ...     schema="bm25_tables"
    ... )

    >>> tables.create_queries(
    ...     database="test.duckdb",
    ...     schema="bm25_tables",
    ... )
    """


@execute_with_duckdb(
    relative_path=[
        "tables/create/queries.sql",
        "tables/create/documents_queries.sql",
    ]
)
def create_documents_queries() -> None:
    """Create the documents_queries table in the DuckDB database.

    Parameters
    ----------
    database: str
        The name of the DuckDB database.
    config: dict, optional
        The configuration options for the DuckDB connection.

    Examples
    --------
    >>> from ducksearch import tables

    >>> tables.create_schema(
    ...     database="test.duckdb",
    ...     schema="bm25_tables"
    ... )

    >>> tables.create_documents_queries(
    ...     database="test.duckdb",
    ...     schema="bm25_tables",
    ... )
    """
