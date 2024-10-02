import collections
import logging
import os
import resource

import pyarrow as pa
import pyarrow.parquet as pq
import tqdm
from joblib import delayed

from ..decorators import execute_with_duckdb
from ..utils import ParallelTqdm, batchify, generate_random_hash
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
    group_id: int,
    random_hash: str,
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
    group_id
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

    matchs = search_function(
        database=database,
        queries_schema="bm25_queries",
        documents_schema="bm25_documents",
        source_schema="bm25_tables",
        top_k=top_k,
        group_id=group_id,
        random_hash=random_hash,
        top_k_token=top_k_token,
        filters=filters,
        config=config,
    )

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
    tqdm_bar: bool = True,
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



    """
    resource.setrlimit(
        resource.RLIMIT_CORE, (resource.RLIM_INFINITY, resource.RLIM_INFINITY)
    )

    if isinstance(queries, str):
        queries = [queries]

    logging.info("Indexing queries.")
    random_hash = generate_random_hash()

    batchs = {
        group_id: batch
        for group_id, batch in enumerate(
            iterable=batchify(
                X=queries, batch_size=batch_size, desc="Searching", tqdm_bar=False
            )
        )
    }

    parquet_file = f"_queries_{random_hash}.parquet"
    pa_queries, pa_group_ids = [], []
    for group_id, batch_queries in batchs.items():
        pa_queries.extend(batch_queries)
        pa_group_ids.extend([group_id] * len(batch_queries))

    logging.info("Indexing queries.")
    index_table = pa.Table.from_pydict({"query": pa_queries, "group_id": pa_group_ids})

    pq.write_table(index_table, parquet_file, compression="snappy")

    _insert_queries(
        database=database,
        schema="bm25_documents",
        parquet_file=parquet_file,
        random_hash=random_hash,
        config=config,
    )

    if os.path.exists(parquet_file):
        os.remove(parquet_file)

    settings = _select_settings(
        database=database, schema="bm25_documents", config=config
    )[0]

    _create_queries_index(
        database=database,
        schema="bm25_documents",
        random_hash=random_hash,
        **settings,
        config=config,
    )

    matchs = []
    if n_jobs == 1 or len(batchs) == 1:
        if tqdm_bar:
            bar = tqdm.tqdm(
                total=len(batchs),
                position=0,
                desc="Searching",
            )

        for group_id, batch_queries in batchs.items():
            matchs.extend(
                _search_graph(
                    database=database,
                    queries=batch_queries,
                    top_k=top_k,
                    top_k_token=top_k_token,
                    group_id=group_id,
                    random_hash=random_hash,
                    config=config,
                    filters=filters,
                )
            )
            if tqdm_bar:
                bar.update(1)
    else:
        for match in ParallelTqdm(
            n_jobs=n_jobs,
            backend="threading",
            total=len(batchs),
            desc="Searching",
            tqdm_bar=tqdm_bar,
        )(
            delayed(_search_graph)(
                database,
                batch_queries,
                top_k,
                top_k_token,
                group_id,
                random_hash,
                config,
                filters,
            )
            for group_id, batch_queries in batchs.items()
        ):
            matchs.extend(match)

    return matchs
