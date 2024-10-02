# connect_to_duckdb

Establish a connection to the DuckDB database. Retry connecting if an error occurs.



## Parameters

- **database** (*str*)

    The name or path of the DuckDB database to connect to.

- **read_only** (*bool*) – defaults to `False`

    Whether to open the database in read-only mode. Default is False.

- **config** (*dict | None*) – defaults to `None`

    Optional configuration settings for the DuckDB connection.

- **max_retry** (*int*) – defaults to `20`

    The maximum number of times to retry connecting to DuckDB.

- **sleep_time** (*float*) – defaults to `0.1`

    The time to sleep between retries.

- **kwargs**




