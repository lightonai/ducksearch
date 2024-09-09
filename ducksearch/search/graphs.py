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
    """Search in duckdb."""


@execute_with_duckdb(
    relative_path="search/select/search_graph_filters.sql",
    read_only=True,
    fetch_df=True,
)
def _search_graph_filters_query():
    """Search in duckdb with filters."""


def _search_graph(
    database: str,
    queries: list[str],
    top_k: int,
    top_k_token: int,
    index: int,
    config: dict | None = None,
    filters: str | None = None,
) -> list:
    """Search in duckdb.

    Parameters
    ----------
    queries
        The list of queries to search.
    top_k
        The number of top documents to retrieve.
    top_k_token
        The number of top tokens to retrieve. It will be used to select top k documents per
        token.
    index
        The index of the batch.

    """
    search_function = (
        _search_graph_filters_query if filters is not None else _search_graph_query
    )

    index_table = pa.Table.from_pydict(
        {
            "query": queries,
        }
    )

    pq.write_table(
        index_table,
        f"_queries_{index}.parquet",
        compression="snappy",
    )

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
    top_k_token: int = 10_000,
    n_jobs: int = -1,
    config: dict | None = None,
    filters: str | None = None,
) -> list[dict]:
    """Search in duckdb.

    Examples
    --------
    >>> from ducksearch import evaluation, upload, search

    >>> documents, queries, qrels = evaluation.load_beir(
    ...     "scifact",
    ...     split="train"
    ... )

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

    >>> documents, queries, qrels = evaluation.load_beir(
    ...     "scifact",
    ...     split="test"
    ... )

    >>> scores = search.graphs(
    ...    database="test.duckdb",
    ...    queries=queries,
    ...    top_k=10,
    ... )

    >>> evaluation_scores = evaluation.evaluate(
    ...     scores=scores,
    ...     qrels=qrels,
    ...     queries=queries,
    ...     metrics=["ndcg@10", "hits@1", "hits@2", "hits@3", "hits@4", "hits@5", "hits@10"],
    ... )

    >>> assert evaluation_scores["ndcg@10"] > 0.74

    >>> scores = search.graphs(
    ...    database="test.duckdb",
    ...    queries=queries,
    ...    top_k=10,
    ...    filters="id = '11360768' OR id = '11360768'",
    ... )

    >>> for sample in scores:
    ...   for document in sample:
    ...     assert document["id"] == "11360768" or document["id"] == "11360768"

    """
    resource.setrlimit(
        resource.RLIMIT_CORE, (resource.RLIM_INFINITY, resource.RLIM_INFINITY)
    )

    if isinstance(queries, str):
        queries = [queries]

    logging.info("Indexing queries.")
    index_table = pa.Table.from_pydict(
        {
            "query": queries,
        }
    )

    pq.write_table(
        index_table,
        "_queries.parquet",
        compression="snappy",
    )

    _insert_queries(
        database=database,
        schema="bm25_documents",
        parquet_file="_queries.parquet",
        config=config,
    )

    settings = _select_settings(
        database=database,
        schema="bm25_documents",
        config=config,
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
        delayed(function=_search_graph)(
            database,
            batch_queries,
            top_k,
            top_k_token,
            index,
            config,
            filters,
        )
        for index, batch_queries in enumerate(
            iterable=batchify(X=queries, batch_size=batch_size, desc="Searching")
        )
    ):
        matchs.extend(match)

    return matchs
