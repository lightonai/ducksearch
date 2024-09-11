## Graph

The `search.graphs` function can be used to search documents with a graph-based query. This function is useful if we have paired documents and queries. The search will retrieve the set of documents and queries that match the input query. Then it will build a graph and compute the weight of each document using a graph-based scoring function.

The `search.graphs` function is much slower than the `search.documents` function, but might provide better results with decent amount of paired documents / queries.

### Documents queries interactions

We can upload documents queries interactions in order to call the `search.graphs` function. The following example demonstrates how to upload documents queries interactions:

```python
from ducksearch import search, upload

documents = [
    {
        "id": 0,
        "title": "Hotel California",
        "style": "rock",
        "date": "1977-02-22",
        "popularity": 9,
    },
    {
        "id": 1,
        "title": "Here Comes the Sun",
        "style": "rock",
        "date": "1969-06-10",
        "popularity": 10,
    },
    {
        "id": 2,
        "title": "Alive",
        "style": "electro, punk",
        "date": "2007-11-19",
        "popularity": 9,
    },
]

upload.documents(
    database="ducksearch.duckdb",
    key="id",
    fields=["title", "style", "date", "popularity"],
    documents=documents,
    dtypes={
        "date": "DATE",
        "popularity": "INT",
    },
)

# Mapping between documents ids and queries
documents_queries = {
    0: ["the beatles", "rock band"],
    1: ["rock band", "california"],
    2: ["daft"],
}

upload.queries(
	database="ducksearch.duckdb",
	documents_queries=documents_queries,
)
```

???+ tip
    We can write documents queries mapping as a list of dict with the weight between the document and the query. The weight is used to compute the score in the `search.graphs` function:

    ```python
    documents_queries = {
        0: {"the beatles": 30, "rock band": 10},
        1: {"rock band": 10, "california": 1},
        2: {"daft": 60},
    }
    ```

    When the weight is not specified, the default value is 1.

### Search Graphs

The following example demonstrates how to search documents with a graph-based query:

```python
from ducksearch import search

search.graphs(
	database="ducksearch.duckdb",
	queries="daft punk",
	top_k=10,
)
```

```python
[
    {
        "id": "2",
        "title": "Alive",
        "style": "electro, punk",
        "date": Timestamp("2007-11-19 00:00:00"),
        "popularity": 9,
        "score": 2.877532958984375,
    }
]
```