import collections
import os
import shutil

import pyarrow as pa
import pyarrow.parquet as pq
import tqdm
from joblib import Parallel, delayed
from lenlp import counter

from ..decorators import execute_with_duckdb


def documents(
    database: str,
    queries: str | list[str],
    ngram_range=(1, 1),
    analyzer: str = "word",
    normalize: bool = True,
    batch_size: int = 30,
    top_k: int = 10,
    top_k_token: int = 10_000,
    n_jobs: int = -1,
    config: dict | None = None,
    filters: str | None = None,
    **kwargs,
) -> list[list[dict]]:
    """Search for documents from the documents table.

    Parameters
    ----------
    database
        The name of the DuckDB database.
    queries
        The list of queries to search.
    ngram_range
        The ngram range to use.
    analyzer
        The analyzer to use. Either "word" or "char" or "char_wb".
    normalize
        Normalize the text.
    batch_size
        The batch size to use.
    top_k
        The number of top documents to retrieve.
    top_k_token
        The number of top tokens to retrieve. It will be used to select top k documents per
        token.
    n_jobs
        The number of parallel jobs to run. -1 means using all processors.
    config
        The configuration options for the DuckDB connection

    Examples
    --------
    >>> from ducksearch import evaluation, upload, search

    >>> documents, queries, qrels = evaluation.load_beir(
    ...     "scifact",
    ...     split="test"
    ... )

    >>> scores = search.documents(
    ...     database="test.duckdb",
    ...     queries=queries,
    ... )

    >>> evaluation_scores = evaluation.evaluate(
    ...     scores=scores,
    ...     qrels=qrels,
    ...     queries=queries,
    ...     metrics=["ndcg@10", "hits@1", "hits@2", "hits@3", "hits@4", "hits@5", "hits@10"],
    ... )

    >>> assert evaluation_scores["ndcg@10"] > 0.65

    >>> for sample_documents in scores:
    ...     for document in sample_documents:
    ...         assert "title" in document
    ...         assert "text" in document
    ...         assert "score" in document
    ...         assert "id" in document

    >>> scores = search.documents(
    ...     database="test.duckdb",
    ...     queries=queries,
    ...     filters="id = '11360768' OR id = '11360768'",
    ... )

    >>> for sample in scores:
    ...   for document in sample:
    ...     assert document["id"] == "11360768" or document["id"] == "11360768"

    """
    return search(
        database=database,
        schema="bm25_documents",
        source="documents",
        queries=queries,
        ngram_range=ngram_range,
        analyzer=analyzer,
        normalize=normalize,
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
    ngram_range=(1, 1),
    analyzer: str = "word",
    normalize: bool = True,
    batch_size: int = 30,
    top_k: int = 10,
    top_k_token: int = 10_000,
    n_jobs: int = -1,
    config: dict | None = None,
    filters: str | None = None,
    **kwargs,
) -> list[list[dict]]:
    """Search for queries from the queries table.

    Parameters
    ----------
    database
        The name of the DuckDB database.
    queries
        The list of queries to search.
    ngram_range
        The ngram range to use.
    analyzer
        The analyzer to use. Either "word" or "char" or "char_wb".
    normalize
        Normalize the text.
    batch_size
        The batch size to use.
    top_k
        The number of top documents to retrieve.
    top_k_token
        The number of top tokens to retrieve. It will be used to select top k documents per
        token.
    n_jobs
        The number of parallel jobs to run. -1 means using all processors.
    config
        The configuration options for the DuckDB connection

    Examples
    --------
    >>> from ducksearch import evaluation, upload, search

    >>> documents, queries, qrels = evaluation.load_beir(
    ...     "scifact",
    ...     split="test"
    ... )

    >>> scores = search.queries(
    ...     database="test.duckdb",
    ...     queries=queries,
    ... )

    >>> n = 0
    >>> for sample, query in zip(scores, queries):
    ...   if sample[0]["query"] == query:
    ...     n += 1

    >>> assert n >= 290

    >>> scores = search.queries(
    ...     database="test.duckdb",
    ...     queries=queries,
    ...     filters="id = 1 OR id = 2",
    ... )

    >>> for sample in scores:
    ...   for document in sample:
    ...     assert document["id"] == "1" or document["id"] == "2"

    """
    return search(
        database=database,
        schema="bm25_queries",
        source="queries",
        queries=queries,
        ngram_range=ngram_range,
        analyzer=analyzer,
        normalize=normalize,
        config=config,
        batch_size=batch_size,
        top_k=top_k,
        top_k_token=top_k_token,
        n_jobs=n_jobs,
        filters=filters,
    )


def batchify(
    X: list[str], batch_size: int, desc: str = "", tqdm_bar: bool = True
) -> list:
    batchs = [X[pos : pos + batch_size] for pos in range(0, len(X), batch_size)]

    if tqdm_bar:
        for batch in tqdm.tqdm(
            batchs,
            position=0,
            total=1 + len(X) // batch_size,
            desc=desc,
        ):
            yield batch
    else:
        yield from batchs


@execute_with_duckdb(
    relative_path="search/select/search.sql",
    read_only=True,
    fetch_df=True,
)
def _search_query():
    """Search in duckdb."""


@execute_with_duckdb(
    relative_path="search/select/search_filters.sql",
    read_only=True,
    fetch_df=True,
)
def _search_query_filters():
    """Search in duckdb."""


def _search(
    database: str,
    schema: str,
    source: str,
    queries: list[str],
    ngram_range: tuple,
    analyzer: str,
    normalize: bool,
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
    queries_tokens = counter.count(
        queries,
        ngram_range=ngram_range,
        analyzer=analyzer,
        normalize=normalize,
    )

    queries_path = os.path.join(".", "duckdb_tmp", "queries", f"{index}.parquet")

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

    search_function = _search_query_filters if filters is not None else _search_query

    matchs = search_function(
        database=database,
        parquet_files=queries_path,
        schema=schema,
        source=source,
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


def search(
    database: str,
    schema: str,
    source: str,
    queries: str | list[str],
    ngram_range=(1, 1),
    analyzer: str = "word",
    normalize: bool = True,
    batch_size: int = 30,
    top_k: int = 10,
    top_k_token: int = 10_000,
    n_jobs: int = -1,
    config: dict | None = None,
    filters: str | None = None,
) -> None:
    """Run the search in parallel.

    Parameters
    ----------
    database
        The name of the DuckDB database.
    schema
        The name of the schema to search.
    queries
        The list of queries to search.
    ngram_range
        The ngram range to use.
    analyzer
        The analyzer to use. Either "word" or "char" or "char_wb".
    normalize
        Normalize the text.
    batch_size
        The batch size to use.
    top_k
        The number of top documents to retrieve.
    top_k_token
        The number of top tokens to retrieve. It will be used to select top k documents per
        token.
    n_jobs
        The number of parallel jobs to run. -1 means using all processors.
    config
        The configuration options for the DuckDB connection

    Examples
    --------
    >>> from ducksearch import search

    >>> documents = search.search(
    ...     database="test.duckdb",
    ...     schema="bm25_documents",
    ...     source="documents",
    ...     queries=["Random query"],
    ...     top_k_token = 10_000,
    ...     top_k = 10,
    ... )

    >>> assert len(documents) == 1
    >>> assert len(documents[0]) == 10


    """
    queries = queries if isinstance(queries, list) else [queries]
    queries_path = os.path.join(".", "duckdb_tmp", "queries")

    if os.path.exists(path=queries_path):
        shutil.rmtree(queries_path)

    os.makedirs(name=os.path.join(".", "duckdb_tmp"), exist_ok=True)
    os.makedirs(name=queries_path, exist_ok=True)

    matchs = []
    for match in Parallel(n_jobs=n_jobs, backend="threading")(
        delayed(function=_search)(
            database,
            schema,
            source,
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
