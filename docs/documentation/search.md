???+ note
    Before we can search for documents, we need to upload them to DuckDB. We can use the `upload.documents` function to upload a list of dictionaries to DuckDB.

## Search

All the search functions require a DuckDB database name as the first argument. The database name is the name of the DuckDB database where the documents are stored. The database name is the same as the one used in the `upload.documents` function. Each search function can take additional parameters to control the search behavior such as the number of documents to return, the number of documents to score for each query token, and the number of parallel jobs to use as well as optional SQL filters.

### Documents

Once the documents are uploaded, we can search for them using the `search.documents` function.
The search function returns a list of list of documents ordered by their BM25 score.

```python
search.documents(
    database="ducksearch.duckdb",
    queries=["daft punk", "rock"],
    top_k=10,
    top_k_token=10_000,
    batch_size=32,
    n_jobs=-1,
)
```

```python
[
    [
        {
            "id": "2",
            "title": "Alive",
            "style": "electro, punk",
            "date": Timestamp("2007-11-19 00:00:00"),
            "popularity": 9,
            "score": 0.16131360828876495,
        }
    ],
    [
        {
            "id": "1",
            "title": "Here Comes the Sun",
            "style": "rock",
            "date": Timestamp("1969-06-10 00:00:00"),
            "popularity": 10,
            "score": 0.09199773520231247,
        },
        {
            "id": "0",
            "title": "Hotel California",
            "style": "rock",
            "date": Timestamp("1977-02-22 00:00:00"),
            "popularity": 9,
            "score": 0.07729987800121307,
        },
    ],
]
```

???+ info
    The search function is executed in parallel using the `n_jobs` parameter. We can control the number of documents to return using the `top_k` parameter and the number of documents to score for each query token using the `top_k_token` parameter. Reducing `top_k_token` can further speed up the search but may result in lower quality results.

### Filters

We can apply filters to the search using the `filters` parameter. The filters are SQL expressions that are applied to the search results.

```python
from ducksearch import search

search.documents(
    database="ducksearch.duckdb",
    queries=["rock", "california"],
    top_k=10,
    top_k_token=10_000,
    batch_size=32,
    filters="YEAR(date) <= 1990 AND YEAR(date) >= 1970",
    n_jobs=-1,
)
```

```python
[
    [
        {
            "score": 0.07729987800121307,
            "id": "0",
            "title": "Hotel California",
            "style": "rock",
            "date": Timestamp("1977-02-22 00:00:00"),
            "popularity": 9,
        }
    ],
    [
        {
            "score": 0.16131360828876495,
            "id": "0",
            "title": "Hotel California",
            "style": "rock",
            "date": Timestamp("1977-02-22 00:00:00"),
            "popularity": 9,
        }
    ],
]
```

???+ info
    The filters are evaluated by DuckDB, so all DuckDB functions are available for use in the filters. You can find more information about DuckDB functions in the [DuckDB documentation](https://duckdb.org/docs/sql/functions/overview).

