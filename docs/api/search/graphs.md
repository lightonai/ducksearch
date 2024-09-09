# graphs

Search for graphs in DuckDB using the provided queries.



## Parameters

- **database** (*str*)

    The name of the DuckDB database.

- **queries** (*str | list[str]*)

    A string or list of query strings to search for.

- **batch_size** (*int*) – defaults to `30`

    The batch size for processing queries.

- **top_k** (*int*) – defaults to `1000`

    The number of top documents to retrieve for each query.

- **top_k_token** (*int*) – defaults to `10000`

    The number of top tokens to retrieve.

- **n_jobs** (*int*) – defaults to `-1`

    The number of parallel jobs to use. Default use all available processors.

- **config** (*dict | None*) – defaults to `None`

    Optional configuration settings for the DuckDB connection.

- **filters** (*str | None*) – defaults to `None`

    Optional SQL filters to apply during the search.



## Examples

```python
>>> from ducksearch import evaluation, upload, search

>>> documents, queries, qrels = evaluation.load_beir("scifact", split="train")

>>> upload.documents(
...     database="test.duckdb",
...     key="id",
...     fields=["title", "text"],
...     documents=documents,
... )

>>> upload.queries(
...     database="test.duckdb",
...     queries=queries,
...     documents_queries=qrels,
... )

>>> scores = search.graphs(
...     database="test.duckdb",
...     queries=queries,
...     top_k=10,
... )

>>> assert len(scores) > 0

>>> evaluation_scores = evaluation.evaluate(
...     scores=scores,
...     qrels=qrels,
...     queries=queries,
...     metrics=["ndcg@10", "hits@1", "hits@10"]
... )

>>> assert evaluation_scores["ndcg@10"] > 0.74
```

