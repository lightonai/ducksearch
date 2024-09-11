import collections
import logging
import os
import resource

import pyarrow as pa
import pyarrow.parquet as pq
from joblib import Parallel, delayed

from ..decorators import execute_with_duckdb
from ..utils import batchify
from .create import _select_settings
from .select import _create_queries_index, _insert_queries


@execute_with_duckdb(
    relative_path="search/select/search_graph.sql",
    read_only=True,
    fetch_df=True,
)
def _search_graph_query():
    """Execute a graph-based search query in DuckDB."""


@execute_with_duckdb(
    relative_path="search/select/search_graph_filters.sql",
    read_only=True,
    fetch_df=True,
)
def _search_graph_filters_query():
    """Execute a graph-based search query in DuckDB with filters."""


def _search_graph(
    database: str,
    queries: list[str],
    top_k: int,
    top_k_token: int,
    index: int,
    config: dict | None = None,
    filters: str | None = None,
) -> list:
    """Perform a graph-based search in DuckDB.

    Parameters
    ----------
    database
        The name of the DuckDB database.
    queries
        The list of queries to search.
    top_k
        The number of top results to retrieve for each query.
    top_k_token
        The number of top tokens to retrieve. Used to select top documents per token.
    index
        The index of the current batch of queries.
    config
        Optional configuration settings for the DuckDB connection.
    filters
        Optional SQL filters to apply during the search.

    Returns
    -------
    list
        A list of search results for each query in the batch.
    """
    search_function = (
        _search_graph_filters_query if filters is not None else _search_graph_query
    )

    index_table = pa.Table.from_pydict({"query": queries})
    pq.write_table(index_table, f"_queries_{index}.parquet", compression="snappy")

    matchs = search_function(
        database=database,
        queries_schema="bm25_queries",
        documents_schema="bm25_documents",
        source_schema="bm25_tables",
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


def graphs(
    database: str,
    queries: str | list[str],
    batch_size: int = 30,
    top_k: int = 1000,
    top_k_token: int = 30_000,
    n_jobs: int = -1,
    config: dict | None = None,
    filters: str | None = None,
) -> list[dict]:
    """Search for graphs in DuckDB using the provided queries.

    Parameters
    ----------
    database
        The name of the DuckDB database.
    queries
        A string or list of query strings to search for.
    batch_size
        The batch size for processing queries.
    top_k
        The number of top documents to retrieve for each query.
    top_k_token
        The number of top tokens to retrieve.
    n_jobs
        The number of parallel jobs to use. Default use all available processors.
    config
        Optional configuration settings for the DuckDB connection.
    filters
        Optional SQL filters to apply during the search.

    Returns
    -------
    list[dict]
        A list of search results, where each result corresponds to a query.

    Examples
    --------
    >>> from ducksearch import evaluation, upload, search

    >>> documents, queries, qrels = evaluation.load_beir("scifact", split="train")

    >>> upload.documents(
    ...     database="test.duckdb",
    ...     key="id",
    ...     fields=["title", "text"],
    ...     documents=documents,
    ... )
    | Table          | Size |
    |----------------|------|
    | documents      | 5183 |
    | bm25_documents | 5183 |

    >>> upload.queries(
    ...     database="test.duckdb",
    ...     queries=queries,
    ...     documents_queries=qrels,
    ... )
    | Table             | Size |
    |-------------------|------|
    | documents         | 5183 |
    | queries           | 807  |
    | bm25_documents    | 5183 |
    | bm25_queries      | 807  |
    | documents_queries | 916  |

    >>> scores = search.graphs(
    ...     database="test.duckdb",
    ...     queries=queries,
    ...     top_k=10,
    ... )

    """
    resource.setrlimit(
        resource.RLIMIT_CORE, (resource.RLIM_INFINITY, resource.RLIM_INFINITY)
    )

    if isinstance(queries, str):
        queries = [queries]

    logging.info("Indexing queries.")
    index_table = pa.Table.from_pydict({"query": queries})
    pq.write_table(index_table, "_queries.parquet", compression="snappy")

    _insert_queries(
        database=database,
        schema="bm25_documents",
        parquet_file="_queries.parquet",
        config=config,
    )

    if os.path.exists("_queries.parquet"):
        os.remove("_queries.parquet")

    settings = _select_settings(
        database=database, schema="bm25_documents", config=config
    )[0]

    _create_queries_index(
        database=database,
        schema="bm25_documents",
        **settings,
        config=config,
    )

    matchs = []
    for match in Parallel(
        n_jobs=1 if len(queries) <= batch_size else n_jobs, backend="threading"
    )(
        delayed(_search_graph)(
            database,
            batch_queries,
            top_k,
            top_k_token,
            index,
            config,
            filters,
        )
        for index, batch_queries in enumerate(
            batchify(queries, batch_size=batch_size, desc="Searching")
        )
    ):
        matchs.extend(match)

    return matchs
