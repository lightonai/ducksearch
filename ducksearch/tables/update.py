from ..decorators import execute_with_duckdb


@execute_with_duckdb(
    relative_path="tables/update/documents.sql",
)
def _add_columns_documents() -> None:
    """Add columns to the documents table in the DuckDB database.

    Parameters
    ----------
    database: str
        The name of the DuckDB database.
    config: dict, optional
        The configuration options for the DuckDB connection.
    """


def add_columns_documents(
    database: str,
    schema: str,
    columns: list[str] | str,
    dtypes: dict = None,
    config: dict = None,
) -> None:
    """Add columns to the documents table in the DuckDB database.

    Parameters
    ----------
    database:
        The name of the DuckDB database.
    schema:
        The schema in which the documents table is located.
    columns:
        The columns to add to the documents table.
    dtypes:
        The data types for the columns to add.
    config:
        The configuration options for the DuckDB connection.

    """
    if isinstance(columns, str):
        columns = [columns]

    if dtypes is None:
        dtypes = {}

    _add_columns_documents(
        database=database,
        schema=schema,
        fields=", ".join(
            [f"ADD COLUMN {field} {dtypes.get(field, 'VARCHAR')}" for field in columns]
        ),
        config=config,
    )
