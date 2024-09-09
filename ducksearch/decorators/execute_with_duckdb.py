import pathlib
from functools import wraps

import duckdb


def connect_to_duckdb(
    database: str,
    read_only: bool = False,
    config: dict | None = None,
):
    """Connect to the DuckDB database."""
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
    """Decorator to execute a SQL query using DuckDB."""

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
            """Connect to the database and execute the query."""

            # Open the DuckDB connection
            conn = connect_to_duckdb(
                database=database, read_only=read_only, config=config
            )

            if isinstance(relative_path, str):
                relative_path = [relative_path]

            try:
                # Execute the SQL query
                for path in relative_path:
                    # Get the directory of the current file (the file where this decorator is used)
                    path = pathlib.Path(__file__).parent.parent.joinpath(path)

                    # Load the SQL query from the file specified by the path
                    with open(file=path, mode="r") as sql_file:
                        query = sql_file.read()

                    if kwargs:
                        query = query.format(**kwargs)

                    if fetch_df:
                        data = conn.execute(query).fetchdf()
                        data.columns = data.columns.str.lower()
                        data = data.to_dict(orient="records")

                    else:
                        data = conn.execute(query).fetchall()
                        if fields is not None:
                            data = [dict(zip(fields, row)) for row in data]
            except duckdb.duckdb.IOException:
                message = "\n--------\nDuckDB exception, too many files open.\nGet current ulimit: ulimit -n\nIncrease ulimit with `ulimit -n 4096` or more.\n--------\n"
                raise duckdb.duckdb.IOException(message)
            except Exception as error:
                raise ValueError(
                    "\n{}:\n{}\n{}:\n{}".format(
                        type(error).__name__, path, error, query
                    )
                )
            finally:
                conn.close()

            if fetch_df:
                return data

            if data:
                return data

        return wrapper

    return decorator
