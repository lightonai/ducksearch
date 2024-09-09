# documents

Search for documents in the documents table using specified queries.



## Parameters

- **database** (*str*)

    The name of the DuckDB database.

- **queries** (*str | list[str]*)

    A string or list of query strings to search for.

- **batch_size** (*int*) – defaults to `30`

    The batch size for query processing.

- **top_k** (*int*) – defaults to `10`

    The number of top documents to retrieve for each query.

- **top_k_token** (*int*) – defaults to `10000`

    The number of documents to score per token.

- **n_jobs** (*int*) – defaults to `-1`

    The number of parallel jobs to use. Default use all available processors.

- **config** (*dict | None*) – defaults to `None`

    Optional configuration for DuckDB connection settings.

- **filters** (*str | None*) – defaults to `None`

    Optional SQL filters to apply during the search.

- **kwargs**



## Examples

```python
>>> from ducksearch import evaluation, upload, search
>>> documents, queries, qrels = evaluation.load_beir("scifact", split="test")
>>> scores = search.documents(database="test.duckdb", queries=queries, top_k_token=1000)
>>> evaluation_scores = evaluation.evaluate(scores=scores, qrels=qrels, queries=queries)
>>> assert evaluation_scores["ndcg@10"] > 0.68
```

