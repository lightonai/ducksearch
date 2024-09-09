import collections
import logging
import os

import pyarrow as pa
import pyarrow.parquet as pq
from joblib import Parallel, delayed

from ..decorators import execute_with_duckdb
from ..utils import batchify
from .create import _select_settings


@execute_with_duckdb(
    relative_path="search/create/queries_index.sql",
)
def _create_queries_index() -> None:
    """Create an index for the queries table in the DuckDB database."""


@execute_with_duckdb(
    relative_path="search/insert/queries.sql",
)
def _insert_queries() -> None:
    """Insert queries into the queries index."""


@execute_with_duckdb(
    relative_path="search/select/search.sql",
    read_only=True,
    fetch_df=True,
)
def _search_query():
    """Perform a search on the documents or queries table in DuckDB."""


@execute_with_duckdb(
    relative_path="search/select/search_filters.sql",
    read_only=True,
    fetch_df=True,
)
def _search_query_filters():
    """Perform a filtered search on the documents or queries table in DuckDB."""


def documents(
    database: str,
    queries: str | list[str],
    batch_size: int = 30,
    top_k: int = 10,
    top_k_token: int = 10_000,
    n_jobs: int = -1,
    config: dict | None = None,
    filters: str | None = None,
    **kwargs,
) -> list[list[dict]]:
    """Search for documents in the documents table using specified queries.

    Parameters
    ----------
    database
        The name of the DuckDB database.
    queries
        A string or list of query strings to search for.
    batch_size
        The batch size for query processing. Default is 30.
    top_k
        The number of top documents to retrieve for each query. Default is 10.
    top_k_token
        The number of documents to score per token. Default is 10,000.
    n_jobs
        The number of parallel jobs to use. Default is -1 (use all available processors).
    config
        Optional configuration for DuckDB connection settings.
    filters
        Optional SQL filters to apply during the search.

    Returns
    -------
    list[list[dict]]
        A list of lists where each sublist contains the top matching documents for a query.

    Examples
    --------
    >>> from ducksearch import evaluation, upload, search
    >>> documents, queries, qrels = evaluation.load_beir("scifact", split="test")
    >>> scores = search.documents(database="test.duckdb", queries=queries, top_k_token=1000)
    >>> evaluation_scores = evaluation.evaluate(scores=scores, qrels=qrels, queries=queries)
    >>> assert evaluation_scores["ndcg@10"] > 0.68

    """
    return search(
        database=database,
        schema="bm25_documents",
        source_schema="bm25_tables",
        source="documents",
        queries=queries,
        config=config,
        batch_size=batch_size,
        top_k=top_k,
        top_k_token=top_k_token,
        n_jobs=n_jobs,
        filters=filters,
    )


def queries(
    database: str,
    queries: str | list[str],
    batch_size: int = 30,
    top_k: int = 10,
    top_k_token: int = 10_000,
    n_jobs: int = -1,
    config: dict | None = None,
    filters: str | None = None,
    **kwargs,
) -> list[list[dict]]:
    """Search for queries in the queries table using specified queries.

    Parameters
    ----------
    database
        The name of the DuckDB database.
    queries
        A string or list of query strings to search for.
    batch_size
        The batch size for query processing. Default is 30.
    top_k
        The number of top matching queries to retrieve. Default is 10.
    top_k_token
        The number of documents to score per token. Default is 10,000.
    n_jobs
        The number of parallel jobs to use. Default is -1 (use all available processors).
    config
        Optional configuration for DuckDB connection settings.
    filters
        Optional SQL filters to apply during the search.

    Returns
    -------
    list[list[dict]]
        A list of lists where each sublist contains the top matching queries for a query.

    Examples
    --------
    >>> from ducksearch import evaluation, upload, search

    >>> documents, queries, qrels = evaluation.load_beir("scifact", split="test")

    >>> scores = search.queries(database="test.duckdb", queries=queries)

    >>> n = sum(1 for sample, query in zip(scores, queries) if sample[0]["query"] == query)
    >>> assert n >= 290
    """
    return search(
        database=database,
        schema="bm25_queries",
        source_schema="bm25_tables",
        source="queries",
        queries=queries,
        config=config,
        batch_size=batch_size,
        top_k=top_k,
        top_k_token=top_k_token,
        n_jobs=n_jobs,
        filters=filters,
    )


def _search(
    database: str,
    schema: str,
    source_schema: str,
    source: str,
    queries: list[str],
    top_k: int,
    top_k_token: int,
    index: int,
    config: dict | None = None,
    filters: str | None = None,
) -> list:
    """Perform a search on the specified source table (documents or queries).

    Parameters
    ----------
    database
        The name of the DuckDB database.
    schema
        The name of the schema containing the source table.
    source_schema
        The name of the schema containing the source data.
    source
        The source table to search (either 'documents' or 'queries').
    queries
        A list of query strings to search for.
    top_k
        The number of top results to retrieve for each query.
    top_k_token
        The number of documents to score per token. Default is 10,000.
    index
        The index of the current query batch.
    config
        Optional configuration for DuckDB connection settings.
    filters
        Optional SQL filters to apply during the search.

    Returns
    -------
    list
        A list of search results for each query in the batch.
    """
    search_function = _search_query_filters if filters is not None else _search_query

    index_table = pa.Table.from_pydict({"query": queries})
    pq.write_table(index_table, f"_queries_{index}.parquet", compression="snappy")

    matchs = search_function(
        database=database,
        schema=schema,
        source_schema=source_schema,
        source=source,
        top_k=top_k,
        top_k_token=top_k_token,
        parquet_file=f"_queries_{index}.parquet",
        filters=filters,
        config=config,
    )

    if os.path.exists(f"_queries_{index}.parquet"):
        os.remove(f"_queries_{index}.parquet")

    candidates = collections.defaultdict(list)
    for match in matchs:
        query = match.pop("_query")
        candidates[query].append(match)
    return [candidates[query] for query in queries]


def search(
    database: str,
    schema: str,
    source_schema: str,
    source: str,
    queries: str | list[str],
    batch_size: int = 30,
    top_k: int = 10,
    top_k_token: int = 10_000,
    n_jobs: int = -1,
    config: dict | None = None,
    filters: str | None = None,
) -> list[list[dict]]:
    """Run the search for documents or queries in parallel.

    Parameters
    ----------
    database
        The name of the DuckDB database.
    schema
        The name of the schema containing the indexed documents or queries.
    source_schema
        The name of the schema containing the original documents or queries.
    source
        The table to search (either 'documents' or 'queries').
    queries
        A string or list of query strings to search for.
    batch_size
        The batch size for query processing. Default is 30.
    top_k
        The number of top results to retrieve for each query. Default is 10.
    top_k_token
        The number of documents to score per token. Default is 10,000.
    n_jobs
        The number of parallel jobs to use. Default is -1 (use all available processors).
    config
        Optional configuration for DuckDB connection settings.
    filters
        Optional SQL filters to apply during the search.

    Returns
    -------
    list[list[dict]]
        A list of lists where each sublist contains the top matching results for a query.

    Examples
    --------
    >>> from ducksearch import search

    >>> documents = search.search(
    ...     database="test.duckdb",
    ...     source_schema="bm25_tables",
    ...     schema="bm25_documents",
    ...     source="documents",
    ...     queries="random query",
    ...     top_k_token=10_000,
    ...     top_k=10,
    ... )

    >>> assert len(documents) == 1
    >>> assert len(documents[0]) == 10

    """
    if isinstance(queries, str):
        queries = [queries]

    logging.info("Indexing queries.")
    index_table = pa.Table.from_pydict({"query": queries})

    settings = _select_settings(
        database=database,
        schema=schema,
        config=config,
    )[0]

    pq.write_table(
        index_table,
        "_queries.parquet",
        compression="snappy",
    )

    _insert_queries(
        database=database,
        schema=schema,
        parquet_file="_queries.parquet",
        config=config,
    )

    _create_queries_index(
        database=database,
        schema=schema,
        **settings,
        config=config,
    )

    matchs = []
    for match in Parallel(
        n_jobs=1 if len(queries) <= batch_size else n_jobs, backend="threading"
    )(
        delayed(_search)(
            database,
            schema,
            source_schema,
            source,
            batch_queries,
            top_k,
            top_k_token,
            index,
            config,
            filters=filters,
        )
        for index, batch_queries in enumerate(
            batchify(queries, batch_size=batch_size, desc="Searching")
        )
    ):
        matchs.extend(match)

    return matchs
