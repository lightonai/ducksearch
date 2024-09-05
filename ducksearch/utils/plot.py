from ..decorators import execute_with_duckdb


def create_aligned_markdown_table(data):
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
    """Plot the queries."""


def plot(
    database: str, config: None | dict = None, tables=["documents", "queries"]
) -> str:
    """Plot statistics about the dataset.

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
            data.update(
                _plot_queries_documents(database=database, table=table, config=config)[
                    0
                ]
            )
        except Exception:
            continue

    data = {
        table.replace(".lengths", ""): size for table, size in data.items() if size > 0
    }
    return print(create_aligned_markdown_table(data=data))
