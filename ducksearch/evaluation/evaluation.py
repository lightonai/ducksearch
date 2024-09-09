import collections
from typing import Dict

__all__ = ["evaluate", "load_beir"]


def load_beir(dataset_name: str, split: str = "test") -> tuple[list, list, dict]:
    """Load BEIR dataset for document and query retrieval tasks.

    Parameters
    ----------
    dataset_name
        The name of the dataset to load (e.g., 'scifact').
    split
        The dataset split to load (e.g., 'test'). Default is 'test'.

    Returns
    -------
    tuple
        A tuple containing three elements:
        - A list of document dictionaries, each containing 'id', 'title', and 'text' fields.
        - A list of queries.
        - A dictionary of qrels (query relevance judgments).

    Examples
    --------
    >>> documents, queries, qrels = load_beir("scifact", split="test")

    >>> len(documents)
    5183

    >>> len(queries)
    300

    """
    from beir import util
    from beir.datasets.data_loader import GenericDataLoader

    data_path = util.download_and_unzip(
        url=f"https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/{dataset_name}.zip",
        out_dir="./evaluation_datasets/",
    )

    documents, queries, qrels = GenericDataLoader(data_folder=data_path).load(
        split=split
    )

    # Format documents
    documents = [
        {
            "id": document_id,
            "title": document["title"],
            "text": document["text"],
        }
        for document_id, document in documents.items()
    ]

    # Filter queries
    _queries = [queries[query_id] for query_id, _ in qrels.items()]

    # Format qrels (relevance judgments)
    _qrels = collections.defaultdict(dict)
    for query_id, query_documents in qrels.items():
        for document in list(query_documents.keys()):
            if query_id in queries:
                _qrels[document][queries[query_id]] = 1

    return (
        documents,
        _queries,
        _qrels,
    )


def evaluate(
    scores: list[list[dict]],
    qrels: dict,
    queries: list[str],
    metrics: list = [],
) -> Dict[str, float]:
    """Evaluate the performance of document retrieval using relevance judgments.

    Parameters
    ----------
    scores
        A list of lists, where each sublist contains dictionaries representing the retrieved documents for a query.
    qrels
        A dictionary mapping queries to relevant documents and their relevance scores.
    queries
        A list of queries.
    metrics
        A list of metrics to compute. Default includes "ndcg@10" and hits at various levels (e.g., hits@1, hits@10).

    Returns
    -------
    dict
        A dictionary mapping each metric to its computed value.

    Examples
    --------
    >>> from ducksearch import evaluation, upload, search

    >>> documents, queries, qrels = evaluation.load_beir("scifact", split="test")

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

    >>> scores = search.documents(
    ...     database="test.duckdb",
    ...     queries=queries,
    ...     top_k=10,
    ... )

    >>> evaluation_scores = evaluation.evaluate(
    ...     scores=scores,
    ...     qrels=qrels,
    ...     queries=queries,
    ...     metrics=["ndcg@10", "hits@1", "hits@2", "hits@3", "hits@4", "hits@5", "hits@10"],
    ... )

    >>> assert evaluation_scores["ndcg@10"] > 0.68
    >>> assert evaluation_scores["hits@1"] > 0.54
    >>> assert evaluation_scores["hits@10"] > 0.90

    """
    from ranx import Qrels, Run, evaluate

    # Format qrels for evaluation
    _qrels = collections.defaultdict(dict)
    for document_id, document_queries in qrels.items():
        for query, score in document_queries.items():
            _qrels[query][document_id] = score

    qrels = Qrels(qrels=_qrels)

    # Create a run dict to map queries to their respective retrieved documents and scores
    run_dict = {
        query: {
            match["id"]: 1 - (rank / len(query_matchs))
            for rank, match in enumerate(iterable=query_matchs)
        }
        for query, query_matchs in zip(queries, scores)
    }

    run = Run(run=run_dict)

    # Default metrics if none are provided
    if not metrics:
        metrics = ["ndcg@10"] + [f"hits@{k}" for k in [1, 2, 3, 4, 5, 10]]

    # Evaluate using ranx and return results
    return evaluate(
        qrels=qrels,
        run=run,
        metrics=metrics,
        make_comparable=True,
    )
