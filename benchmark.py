from nltk import download
from nltk.corpus import stopwords

from ducksearch import evaluation, search, upload

download("stopwords")

stopword = list(stopwords.words("english"))

dataset_name = "quora"

documents, queries, qrels = evaluation.load_beir(
    dataset_name=dataset_name,
    split="test",
)

upload.documents(
    database=dataset_name,
    documents=documents,
    key="id",
    fields=["title", "text"],
    stopwords=stopword,
)

scores = search.documents(
    database=dataset_name,
    queries=queries,
    top_k=10,
    top_k_token=30_000,
    batch_size=32,
)

evaluation_scores = evaluation.evaluate(
    scores=scores,
    qrels=qrels,
    queries=queries,
    metrics=["ndcg@10", "hits@1", "hits@10", "mrr@10", "map@10", "r-precision"],
)

print(evaluation_scores)
