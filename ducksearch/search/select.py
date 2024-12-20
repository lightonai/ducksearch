import collections
import logging
import os

import pyarrow as pa
import pyarrow.parquet as pq
import tqdm
from joblib import Parallel, delayed

from ..decorators import execute_with_duckdb
from ..utils import ParallelTqdm, batchify, generate_random_hash
from .create import _select_settings


@execute_with_duckdb(
    relative_path="search/create/queries_index.sql",
)
def _create_queries_index() -> None:
    """Create an index for the queries table in the DuckDB database."""


@execute_with_duckdb(
    relative_path="search/drop/queries.sql",
)
def _delete_queries_index() -> None:
    """Delete the queries index from the DuckDB database."""


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
    relative_path="search/select/search_order_by.sql",
    read_only=True,
    fetch_df=True,
)
def _search_query_order_by():
    """Perform a search on the documents or queries table in DuckDB."""


@execute_with_duckdb(
    relative_path="search/select/search_filters.sql",
    read_only=True,
    fetch_df=True,
)
def _search_query_filters():
    """Perform a filtered search on the documents or queries table in DuckDB."""


def documents(
    database: str | list[str],
    queries: str | list[str],
    batch_size: int = 32,
    top_k: int = 10,
    top_k_token: int = 30_000,
    n_jobs: int = -1,
    config: dict | None = None,
    filters: str | None = None,
    order_by: str | None = None,
    tqdm_bar: bool = True,
) -> list[list[dict]]:
    """Search for documents in the documents table using specified queries.

    Parameters
    ----------
    database
        The name of the DuckDB database.
    queries
        A string or list of query strings to search for.
    batch_size
        The batch size for query processing.
    top_k
        The number of top documents to retrieve for each query.
    top_k_token
        The number of documents to score per token.
    n_jobs
        The number of parallel jobs to use. Default use all available processors.
    config
        Optional configuration for DuckDB connection settings.
    filters
        Optional SQL filters to apply during the search.
    tqdm_bar
        Whether to display a progress bar when searching.

    Returns
    -------
    list[list[dict]]
        A list of lists where each sublist contains the top matching documents for a query.

    Examples
    --------
    >>> from ducksearch import evaluation, upload, search

    >>> documents, queries, qrels = evaluation.load_beir(
    ...     "scifact",
    ...     split="test",
    ... )

    >>> scores = search.documents(
    ...     database="test.duckdb",
    ...     queries=queries,
    ...     top_k_token=1000,
    ... )

    """
    if isinstance(database, str):
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
            order_by=order_by,
            tqdm_bar=tqdm_bar,
        )

    database = [shard for shard in database if os.path.exists(shard)]

    if not database:
        raise FileNotFoundError("No database shards found.")

    candidates = Parallel(n_jobs=n_jobs, backend="threading")(
        delayed(search)(
            shard,
            "bm25_documents",
            "bm25_tables",
            "documents",
            queries,
            batch_size,
            top_k,
            top_k_token,
            n_jobs,
            config,
            filters,
            order_by,
            tqdm_bar,
        )
        for shard in database
    )

    return aggregate_top_candidates(
        candidates=candidates,
        top_n=top_k,
    )


def queries(
    database: str,
    queries: str | list[str],
    batch_size: int = 32,
    top_k: int = 10,
    top_k_token: int = 30_000,
    n_jobs: int = -1,
    config: dict | None = None,
    filters: str | None = None,
    tqdm_bar: bool = True,
) -> list[list[dict]]:
    """Search for queries in the queries table using specified queries.

    Parameters
    ----------
    database
        The name of the DuckDB database.
    queries
        A string or list of query strings to search for.
    batch_size
        The batch size for query processing.
    top_k
        The number of top matching queries to retrieve.
    top_k_token
        The number of documents to score per token.
    n_jobs
        The number of parallel jobs to use. Default use all available processors.
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
        tqdm_bar=tqdm_bar,
    )


def _search(
    database: str,
    schema: str,
    source_schema: str,
    source: str,
    queries: list[str],
    top_k: int,
    top_k_token: int,
    group_id: int,
    random_hash: str,
    config: dict | None = None,
    filters: str | None = None,
    order_by: str | None = None,
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
        The number of documents to score per token.
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

    if filters is None and order_by is not None:
        search_function = _search_query_order_by

    order_by = f"ORDER BY {order_by}" if order_by is not None else "ORDER BY score DESC"

    matchs = search_function(
        database=database,
        schema=schema,
        source_schema=source_schema,
        source=source,
        top_k=top_k,
        top_k_token=top_k_token,
        random_hash=random_hash,
        group_id=group_id,
        filters=filters,
        config=config,
        order_by=order_by,
    )

    candidates = collections.defaultdict(list)
    for match in matchs:
        query = match.pop("_query")
        candidates[query].append(match)

    candidates = [candidates[query] for query in queries]
    return candidates


def search(
    database: str,
    schema: str,
    source_schema: str,
    source: str,
    queries: str | list[str],
    batch_size: int = 64,
    top_k: int = 10,
    top_k_token: int = 30_000,
    n_jobs: int = -1,
    config: dict | None = None,
    filters: str | None = None,
    order_by: str | None = None,
    tqdm_bar: bool = True,
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
        The batch size for query processing.
    top_k
        The number of top results to retrieve for each query.
    top_k_token
        The number of documents to score per token.
    n_jobs
        The number of parallel jobs to use. Default use available processors.
    config
        Optional configuration for DuckDB connection settings.
    filters
        Optional SQL filters to apply during the search.
    tqdm_bar
        Whether to display a progress bar when searching.

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

    >>> assert len(documents) == 10

    """
    is_query_str = False
    if isinstance(queries, str):
        queries = [queries]
        is_query_str = True

    settings = _select_settings(
        database=database,
        schema=schema,
        config=config,
    )[0]

    batchs = {
        group_id: batch
        for group_id, batch in enumerate(
            iterable=batchify(
                X=queries, batch_size=batch_size, desc="Searching", tqdm_bar=False
            )
        )
    }

    pa_queries, pa_group_ids = [], []
    for group_id, batch_queries in batchs.items():
        pa_queries.extend(batch_queries)
        pa_group_ids.extend([group_id] * len(batch_queries))

    logging.info("Indexing queries.")
    index_table = pa.Table.from_pydict({"query": pa_queries, "group_id": pa_group_ids})

    random_hash = generate_random_hash()
    parquet_file = f"_queries_{random_hash}.parquet"

    pq.write_table(
        index_table,
        parquet_file,
        compression="snappy",
    )

    _insert_queries(
        database=database,
        schema=schema,
        parquet_file=parquet_file,
        random_hash=random_hash,
        config=config,
    )

    if os.path.exists(path=parquet_file):
        os.remove(path=parquet_file)

    _create_queries_index(
        database=database,
        schema=schema,
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
                _search(
                    database=database,
                    schema=schema,
                    source_schema=source_schema,
                    source=source,
                    queries=batch_queries,
                    top_k=top_k,
                    top_k_token=top_k_token,
                    group_id=group_id,
                    random_hash=random_hash,
                    config=config,
                    filters=filters,
                    order_by=order_by,
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
            delayed(_search)(
                database,
                schema,
                source_schema,
                source,
                batch_queries,
                top_k,
                top_k_token,
                group_id,
                random_hash,
                config,
                filters,
                order_by,
            )
            for group_id, batch_queries in batchs.items()
        ):
            matchs.extend(match)

    _delete_queries_index(
        database=database,
        schema=schema,
        random_hash=random_hash,
        config=config,
    )

    return matchs[0] if is_query_str else matchs


def aggregate_top_candidates(candidates, top_n=20):
    """
    Aggregates and selects the top N candidates across all databases for each query.

    Args:
        candidates:
            A list where each element corresponds to a database,
            containing a list of queries, each query containing a list of candidate dicts.
            Shape: (num_databases, num_queries, num_candidates_per_db)
        top_n:
            Number of top candidates to select per query (default is 20).

    Returns:
        A list of queries, each containing the top N candidates across all databases.
        Shape: (num_queries, top_n)
    """
    if not candidates:
        return []

    num_queries, num_databases = len(candidates[0]), len(candidates)

    aggregated = []

    for q_idx in range(num_queries):
        all_candidates = []
        for db_idx in range(num_databases):
            query_candidates = candidates[db_idx][q_idx]
            all_candidates.extend(query_candidates)

        # Sort all candidates by 'score' in descending order
        sorted_candidates = sorted(
            all_candidates,
            key=lambda candidate: candidate.get("score", float("-inf")),
            reverse=True,
        )

        # Select the top N candidates
        top_candidates = sorted_candidates[:top_n]
        aggregated.append(top_candidates)

    return aggregated
