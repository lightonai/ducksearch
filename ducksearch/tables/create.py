from ..decorators import execute_with_duckdb


@execute_with_duckdb(
    relative_path="tables/create/documents.sql",
)
def _create_documents() -> None:
    """Create the documents table.

    Parameters
    ----------
    database: str
        The name of the DuckDB database.
    config: dict, optional
        The configuration options for the DuckDB connection.

    """


def create_documents(
    database: str,
    fields: str | list[str],
    fields_types: dict[str, str] | None = None,
    config: dict | None = None,
) -> None:
    """Create the documents table.

    Parameters
    ----------
    database: str
        The name of the DuckDB database.
    fields: list[str]
        The fields to create the documents table.
    config: dict, optional
        The configuration options for the DuckDB connection.

    Examples
    --------
    >>> from ducksearch import tables

    >>> tables.create_documents(
    ...     database="test.duckdb",
    ...     fields=["title", "text"],
    ...     fields_types={"text": "VARCHAR", "title": "VARCHAR"},
    ... )

    >>> import pandas as pd

    >>> df = pd.DataFrame([
    ...     {"id": 1, "title": "title document 1", "text": "text document 1"},
    ...     {"id": 2, "title": "title document 2", "text": "text document 2"},
    ...     {"id": 3, "title": "title document 3", "text": "text document 3"},
    ... ])

    >>> tables.insert_documents(
    ...     database="test.duckdb",
    ...     key="id",
    ...     df=df,
    ...     fields=["title", "text"],
    ... )
    [(3,)]

    """
    if isinstance(fields, str):
        fields = [fields]

    if not fields_types:
        fields_types = {}

    fields = ", ".join(
        [f"{field} {fields_types.get(field, 'VARCHAR')}" for field in fields]
    )
    return _create_documents(database=database, fields=fields, config=config)


@execute_with_duckdb(
    relative_path="tables/create/queries.sql",
)
def create_queries() -> None:
    """Create the queries table.

    Parameters
    ----------
    database: str
        The name of the DuckDB database.
    config: dict, optional
        The configuration options for the DuckDB connection.

    Examples
    --------
    >>> from ducksearch import tables

    >>> tables.create_queries(database="test.duckdb")

    """


@execute_with_duckdb(
    relative_path=[
        "tables/create/queries.sql",
        "tables/create/documents_queries.sql",
    ]
)
def create_documents_queries() -> None:
    """Create the documents_queries table.

    Parameters
    ----------
    database: str
        The name of the DuckDB database.
        The configuration options for the DuckDB connection.

    Examples
    --------
    >>> from ducksearch import tables

    >>> tables.create_documents_queries(database="test.duckdb")

    """
