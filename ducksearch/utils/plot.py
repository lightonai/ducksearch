import pandas as pd

from ..decorators import execute_with_duckdb


def create_aligned_markdown_table(data: dict) -> str:
    """Create an aligned markdown table from a dictionary of data.

    Parameters
    ----------
    data
        A dictionary where keys are the table names and values are their sizes.

    Returns
    -------
    str
        A formatted markdown table showing table names and sizes.
    """
    # Define the headers
    headers = ["Table", "Size"]

    # Find the maximum width for each column
    max_key_len = max(len(key) for key in data.keys())
    max_val_len = max(len(str(value)) for value in data.values())

    # Ensure the headers fit as well
    max_key_len = max(max_key_len, len(headers[0]))
    max_val_len = max(max_val_len, len(headers[1]))

    # Format the header
    header_row = (
        f"| {headers[0].ljust(max_key_len)} | {headers[1].ljust(max_val_len)} |\n"
    )
    separator_row = f"|{'-' * (max_key_len + 2)}|{'-' * (max_val_len + 2)}|\n"

    # Format the rows with aligned columns
    table_rows = ""
    for key, value in data.items():
        table_rows += (
            f"| {key.ljust(max_key_len)} | {str(value).ljust(max_val_len)} |\n"
        )

    # Combine the header, separator, and rows into the final markdown table
    table = f"{header_row}{separator_row}{table_rows}".strip()
    return f"\n{table}\n"


@execute_with_duckdb(
    relative_path="utils/plot/plot.sql",
    read_only=True,
    fetch_df=True,
)
def _plot_queries_documents():
    """Fetch the table statistics from the DuckDB database.

    Returns
    -------
    list[dict]
        A list of dictionaries where each dictionary contains table statistics.
    """


def plot(
    database: str,
    config: None | dict = None,
    tables=[
        "bm25_tables.documents",
        "bm25_tables.queries",
        "bm25_documents.lengths",
        "bm25_queries.lengths",
        "bm25_tables.documents_queries",
    ],
) -> str:
    """Generate and display a markdown table with statistics of the specified dataset tables.

    Parameters
    ----------
    database
        The name of the DuckDB database.
    config
        Optional configuration options for the DuckDB connection.
    tables
        A list of table names to plot statistics for. Defaults to common BM25 tables.

    Returns
    -------
    str
        A markdown table representing the sizes of the specified tables.

    Examples
    --------
    >>> from ducksearch import utils

    >>> utils.plot(database="test.duckdb")
    | Table     | Size |
    |-----------|------|
    | documents | 5183 |
    | queries   | 300  |
    """
    data = {}
    for table in tables:
        try:
            # Fetch the table statistics for each specified table
            data.update(
                _plot_queries_documents(database=database, table=table, config=config)[
                    0
                ]
            )
        except Exception:
            continue

    # Clean up table names and filter out empty tables
    data = {
        table.replace(".docs", "").replace("bm25_tables.", ""): size
        for table, size in data.items()
        if size > 0
    }

    if len(data) > 0 and data is not None:
        return print(create_aligned_markdown_table(data=data))


def plot_shards(
    databases: list[str],
    config: None | dict = None,
    tables=[
        "bm25_tables.documents",
        "bm25_tables.queries",
        "bm25_documents.lengths",
        "bm25_queries.lengths",
        "bm25_tables.documents_queries",
    ],
) -> str:
    """Generate and display a markdown table with statistics of the specified dataset tables.

    Parameters
    ----------
    database
        The name of the DuckDB database.
    config
        Optional configuration options for the DuckDB connection.
    tables
        A list of table names to plot statistics for. Defaults to common BM25 tables.

    Returns
    -------
    str
        A markdown table representing the sizes of the specified tables.

    Examples
    --------
    >>> from ducksearch import utils

    >>> utils.plot(database="test.duckdb")
    | Table     | Size |
    |-----------|------|
    | documents | 5183 |
    | queries   | 300  |
    """
    statistics = []
    for database in databases:
        data = {}
        for table in tables:
            try:
                # Fetch the table statistics for each specified table
                data.update(
                    _plot_queries_documents(
                        database=database, table=table, config=config
                    )[0]
                )
            except Exception:
                continue

        # Clean up table names and filter out empty tables
        data = {
            table.replace(".docs", "").replace("bm25_tables.", ""): size
            for table, size in data.items()
            if size > 0
        }

        data = {
            "Database": database,
            **data,
        }

        if len(data) > 0 and data is not None:
            statistics.append(data)

    try:
        statistics = pd.DataFrame(statistics)
        total = statistics.sum(numeric_only=True)
        total["Database"] = "Total"
        statistics = pd.concat([statistics, total.to_frame().T], ignore_index=True)
        statistics = "\n" + statistics.to_markdown(index=False) + "\n"
        print(statistics)
    except Exception:
        pass
