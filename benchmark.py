import time

from ducksearch import evaluation, search, upload

dataset_name = "quora"

documents, queries, qrels = evaluation.load_beir(
    dataset_name=dataset_name, split="test"
)

upload.documents(
    database=dataset_name, documents=documents, key="id", fields=["title", "text"]
)

upload.indexes(database=dataset_name)


start = time.time()

scores = search.documents(
    database=dataset_name,
    queries=queries,
    top_k=10,
    top_k_token=10_000,
    batch_size=30,
)

end = time.time()

print(f"Search took {end - start:.2f} seconds, QPS: {len(queries) / (end - start):.2f}")

evaluation_scores = evaluation.evaluate(
    scores=scores,
    qrels=qrels,
    queries=queries,
    metrics=["ndcg@10", "hits@1", "hits@2", "hits@3", "hits@4", "hits@5", "hits@10"],
)

print(evaluation_scores)
