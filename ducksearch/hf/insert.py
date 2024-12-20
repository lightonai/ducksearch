from ..decorators import execute_with_duckdb
from ..tables import add_columns_documents, create_documents


@execute_with_duckdb(
    relative_path="hf/insert/documents.sql",
    fetch_df=False,
)
def _insert_documents() -> None:
    """Insert the documents from Hugging Face datasets into DuckDB."""


@execute_with_duckdb(
    relative_path="hf/select/count.sql",
    fetch_df=True,
)
def count_rows() -> None:
    """Insert the documents from Hugging Face datasets into DuckDB."""


@execute_with_duckdb(
    relative_path="hf/select/columns.sql",
    fetch_df=True,
    read_only=True,
)
def _select_columns() -> None:
    """Select all columns from the HuggingFace documents table."""


@execute_with_duckdb(
    relative_path="hf/select/exists.sql",
    fetch_df=True,
    read_only=True,
)
def _table_exists() -> None:
    """Check if the table exists in the DuckDB database."""


@execute_with_duckdb(
    relative_path="hf/insert/tmp.sql",
    fetch_df=False,
)
def _insert_tmp_documents() -> None:
    """Insert the documents from Hugging Face datasets into DuckDB."""


@execute_with_duckdb(
    relative_path="hf/drop/tmp.sql",
    fetch_df=True,
)
def _drop_tmp_table() -> None:
    """Drop the temporary HF table."""


def insert_documents(
    database: str,
    schema: str,
    key: str,
    url: list[str] | str,
    config: dict | None = None,
    limit: int | None = None,
    offset: int | None = None,
    dtypes: dict | None = None,
) -> None:
    """Insert documents from a Hugging Face dataset into DuckDB.

    Parameters
    ----------
    database
        The name of the DuckDB database.
    schema
        The schema in which the documents table is located.
    key
        The key field that uniquely identifies each document (e.g., 'query_id').
    fields
        A list of fields to be inserted from the dataset. If a single field is provided as a string, it will be converted to a list.
    url
        The URL of the Hugging Face dataset in Parquet format.
    config
        Optional configuration options for the DuckDB connection.

    Examples
    --------
    >>> from ducksearch import upload

    >>> upload.documents(
    ...     database="test.duckdb",
    ...     documents="hf://datasets/lightonai/lighton-ms-marco-mini/queries.parquet",
    ...     key="query_id",
    ...     fields=["query_id", "text"],
    ... )
    | Table          | Size |
    |----------------|------|
    | documents      | 19   |
    | bm25_documents | 19   |

    >>> upload.documents(
    ...     database="test.duckdb",
    ...     documents="hf://datasets/lightonai/lighton-ms-marco-mini/documents.parquet",
    ...     key="document_id",
    ...     fields=["document_id", "text"],
    ... )
    | Table          | Size |
    |----------------|------|
    | documents      | 51   |
    | bm25_documents | 51   |

    """
    if isinstance(url, list):
        for single_url in url:
            _insert_documents(
                database=database,
                schema=schema,
                key=key,
                url=single_url,
                config=config,
                limit=limit,
                dtypes=dtypes,
            )

    offset_hf = f"OFFSET {offset}" if offset is not None else ""
    limit_hf = f"LIMIT {limit}" if limit is not None else ""

    _insert_tmp_documents(
        database=database,
        schema=schema,
        url=url,
        key_field=key,
        config=config,
        offset_hf=offset_hf,
        limit_hf=limit_hf,
    )

    exists = _table_exists(
        database=database,
        schema=schema,
        table_name="documents",
    )[0]["table_exists"]

    _hf_tmp_columns = _select_columns(
        database=database,
        schema=schema,
        table_name="_hf_tmp",
    )

    _hf_tmp_columns = [
        column["column"] for column in _hf_tmp_columns if column["column"] != "id"
    ]

    if exists:
        documents_columns = _select_columns(
            database=database,
            schema=schema,
            table_name="documents",
        )

        documents_columns = set(
            [column["column"] for column in documents_columns if column != "id"]
        )

        columns_to_add = list(set(_hf_tmp_columns) - documents_columns)

        if columns_to_add:
            add_columns_documents(
                database=database,
                schema=schema,
                columns=columns_to_add,
                dtypes=dtypes,
                config=config,
            )
    else:
        create_documents(
            database=database,
            schema=schema,
            columns=_hf_tmp_columns,
            dtypes=dtypes,
            config=config,
        )

    _insert_documents(
        database=database,
        schema=schema,
        url=url,
        key_field=key,
        _hf_tmp_columns=", ".join(_hf_tmp_columns),
        limit_hf=limit_hf,
        config=config,
    )

    _drop_tmp_table(
        database=database,
        schema=schema,
        config=config,
    )
