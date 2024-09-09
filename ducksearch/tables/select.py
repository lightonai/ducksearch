from ..decorators import execute_with_duckdb


@execute_with_duckdb(
    relative_path="tables/select/documents.sql",
    read_only=True,
    fetch_df=True,
)
def select_documents() -> list[dict]:
    """Select documents from the documents table.

    Examples
    --------
    >>> from ducksearch import tables

    >>> documents = tables.select_documents(
    ...     database="test.duckdb",
    ...     schema="bm25_tables",
    ... )

    >>> assert len(documents) == 3

    """


@execute_with_duckdb(
    relative_path="tables/select/queries.sql",
    read_only=True,
    fetch_df=True,
)
def select_queries() -> list[dict]:
    """Select queries from the queries table.

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
    """Get the list of columns from a table."""


def select_documents_columns(
    database: str,
    schema: str,
    config: dict | None = None,
) -> list[str]:
    """Select the columns from the documents table.

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
