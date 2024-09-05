import collections
import os
import shutil

import pyarrow as pa
import pyarrow.parquet as pq
from joblib import Parallel, delayed
from lenlp import counter

from ..decorators import execute_with_duckdb
from .select import batchify


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
    ngram_range: tuple,
    analyzer: str,
    normalize: bool,
    top_k: int,
    top_k_token: int,
    index: int,
    config: dict | None,
    filters: str | None,
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
    queries_tokens = counter.count(
        queries,
        ngram_range=ngram_range,
        analyzer=analyzer,
        normalize=normalize,
    )

    queries_path = os.path.join(".", "duckdb_tmp", "graph", f"{index}.parquet")

    queries_ids, tokens, tfs = [], [], []
    for step, (query, document_tokens) in enumerate(
        iterable=zip(queries, queries_tokens)
    ):
        for token, frequency in document_tokens.items():
            queries_ids.append(query)
            tokens.append(token)
            tfs.append(frequency)

    index_table = pa.Table.from_pydict(
        {
            "query": queries_ids,
            "token": tokens,
            "tf": tfs,
        }
    )

    pq.write_table(
        index_table,
        queries_path,
        compression="snappy",
    )

    search_function = (
        _search_graph_filters_query if filters is not None else _search_graph_query
    )

    matchs = search_function(
        database=database,
        parquet_files=queries_path,
        top_k=top_k,
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
    ngram_range=(1, 1),
    analyzer: str = "word",
    normalize: bool = True,
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

    >>> search.update_index_documents(
    ...     database="test.duckdb",
    ... )
    '5183 documents indexed.'

    >>> upload.queries(
    ...     database="test.duckdb",
    ...     queries=queries,
    ...     documents_queries=qrels,
    ... )

    >>> search.update_index_queries(
    ...     database="test.duckdb",
    ... )
    '807 queries indexed.'

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

    >>> assert evaluation_scores["ndcg@10"] > 0.70

    """
    queries = queries if isinstance(queries, list) else [queries]

    queries_path = os.path.join(".", "duckdb_tmp", "graph")

    if os.path.exists(path=queries_path):
        shutil.rmtree(queries_path)

    os.makedirs(name=os.path.join(".", "duckdb_tmp"), exist_ok=True)
    os.makedirs(name=queries_path, exist_ok=True)

    matchs = []
    for match in Parallel(n_jobs=n_jobs, backend="threading")(
        delayed(function=_search_graph)(
            database,
            batch_queries,
            ngram_range,
            analyzer,
            normalize,
            top_k,
            top_k_token,
            index,
            config,
            filters,
        )
        for index, batch_queries in enumerate(
            iterable=batchify(X=queries, batch_size=batch_size, desc="Search.")
        )
    ):
        matchs.extend(match)

    if os.path.exists(path=queries_path):
        shutil.rmtree(queries_path)

    return matchs
