# execute_with_duckdb

Decorator to execute a SQL query using DuckDB.



## Parameters

- **relative_path** (*str | list[str]*)

    A string or list of strings specifying the path(s) to the SQL file(s).

- **read_only** (*bool*) – defaults to `False`

    Whether the DuckDB connection should be read-only. Default is False.

- **fields** (*list[str] | None*) – defaults to `None`

    A list of fields to use as keys for the result rows if returning records.

- **fetch_df** (*bool*) – defaults to `False`

    If True, fetch the result as a pandas DataFrame and return it as a list of dictionaries.

- **kwargs**

    Additional keyword arguments to be passed to the SQL query, useful for string formatting.




