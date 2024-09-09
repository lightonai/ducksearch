# evaluate

Evaluate the performance of document retrieval using relevance judgments.



## Parameters

- **scores** (*list[list[dict]]*)

    A list of lists, where each sublist contains dictionaries representing the retrieved documents for a query.

- **qrels** (*dict*)

    A dictionary mapping queries to relevant documents and their relevance scores.

- **queries** (*list[str]*)

    A list of queries.

- **metrics** (*list*) – defaults to `[]`

    A list of metrics to compute. Default includes "ndcg@10" and hits at various levels (e.g., hits@1, hits@10).



## Examples

```python
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
```

