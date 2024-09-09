import pathlib
from functools import wraps

import duckdb


def connect_to_duckdb(
    database: str,
    read_only: bool = False,
    config: dict | None = None,
):
    """Establish a connection to the DuckDB database.

    Parameters
    ----------
    database
        The name or path of the DuckDB database to connect to.
    read_only
        Whether to open the database in read-only mode. Default is False.
    config
        Optional configuration settings for the DuckDB connection.

    Returns
    -------
    duckdb.DuckDBPyConnection
        A DuckDB connection object.

    """
    return (
        duckdb.connect(database=database, read_only=read_only, config=config)
        if config
        else duckdb.connect(database=database, read_only=read_only)
    )


def execute_with_duckdb(
    relative_path: str | list[str],
    read_only: bool = False,
    fields: list[str] | None = None,
    fetch_df: bool = False,
    **kwargs,
):
    """Decorator to execute a SQL query using DuckDB.

    Parameters
    ----------
    relative_path
        A string or list of strings specifying the path(s) to the SQL file(s).
    read_only
        Whether the DuckDB connection should be read-only. Default is False.
    fields
        A list of fields to use as keys for the result rows if returning records.
    fetch_df
        If True, fetch the result as a pandas DataFrame and return it as a list of dictionaries.
    kwargs
        Additional keyword arguments to be passed to the SQL query, useful for string formatting.

    Returns
    -------
    A decorator function that executes the SQL query and returns the result.

    """

    def decorator(func):
        @wraps(func)
        def wrapper(
            *args,
            database: str,
            config: dict | None = None,
            df: list[dict] = None,
            relative_path: str | list[str] = relative_path,
            **kwargs,
        ):
            """Connect to DuckDB and execute the query from the provided SQL file path(s)."""
            conn = connect_to_duckdb(
                database=database, read_only=read_only, config=config
            )

            # Ensure relative_path is treated as a list
            if isinstance(relative_path, str):
                relative_path = [relative_path]

            try:
                # Loop through and execute all SQL files in relative_path
                for path in relative_path:
                    # Build the full path to the SQL file
                    path = pathlib.Path(__file__).parent.parent.joinpath(path)

                    # Read the SQL query from the file
                    with open(file=path, mode="r") as sql_file:
                        query = sql_file.read()

                    # Format the query with any additional kwargs
                    if kwargs:
                        query = query.format(**kwargs)

                    # Fetch the result as a DataFrame or a list of rows
                    if fetch_df:
                        data = conn.execute(query).fetchdf()
                        data.columns = data.columns.str.lower()
                        data = data.to_dict(orient="records")
                    else:
                        data = conn.execute(query).fetchall()

                        # If fields are provided, map the result rows to dictionaries with the specified field names
                        if fields is not None:
                            data = [dict(zip(fields, row)) for row in data]

            # Handle DuckDB-specific exceptions (e.g., too many open files)
            except duckdb.duckdb.IOException:
                message = "\n--------\nDuckDB exception, too many files open.\nGet current ulimit: ulimit -n\nIncrease ulimit with `ulimit -n 4096` or more.\n--------\n"
                raise duckdb.duckdb.IOException(message)

            # Handle other exceptions and provide more detailed error information
            except Exception as error:
                raise ValueError(
                    "\n{}:\n{}\n{}:\n{}".format(
                        type(error).__name__, path, error, query
                    )
                )

            # Close the DuckDB connection in the end
            finally:
                conn.close()

            # Return the fetched data, if applicable
            if fetch_df:
                return data

            if data:
                return data

        return wrapper

    return decorator
