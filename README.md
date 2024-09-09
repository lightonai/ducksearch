<div align="center">
  <h1>DuckSearch</h1>
  <p>Efficient BM25 with DuckDB 🦆</p>
</div>

<p align="center"><img width=500 src="docs/img/logo.png"/></p>

<div align="center">
  <!-- Documentation -->
  <a href="https://lightonai.github.io/ducksearch/"><img src="https://img.shields.io/badge/Documentation-purple.svg?style=flat-square" alt="documentation"></a>
  <!-- License -->
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/MIT-blue.svg?style=flat-square" alt="license"></a>
</div>

<p align="justify">
DuckSearch is a lightweight and easy-to-use library that allows to index and search documents. DuckSearch is built on top of DuckDB, a high-performance analytical database. DuckDB is designed to execute analytical SQL queries fast, and DuckSearch leverages this to provide efficient and scallable search / filtering capabilities.
</p>

## Installation

We can install DuckSearch using pip:

```bash
pip install ducksearch
```

For evaluation dependencies, we can install DuckSearch with the `eval` extra:

```bash
pip install "ducksearch[eval]"
```

## Documentation 

The complete documentation is available [here](https://lightonai.github.io/ducksearch/), which includes in-depth guides, examples, and API references.

### Upload

We can upload documents to DuckDB using the `upload.documents` function. The documents are stored in a DuckDB database, and the fields are indexed with BM25.

```python
from ducksearch import upload

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
    key="id", # unique document identifier
    fields=["title", "style", "date", "popularity"], # list of fields to index
    documents=documents,
    dtypes={
        "date": "DATE",
        "popularity": "INT",
    },
)
```

## Search

We can search documents using the `search.documents` function. The function returns the documents that match the query, sorted by the BM25 score. The `top_k` parameter controls the number of documents to return, and the `top_k_token` parameter controls the number of documents to score for each query token. Increasing `top_k_token` can improve the quality of the results but also increase the computation time.

```python
from ducksearch import search

search.documents(
    database="ducksearch.duckdb",
    queries=["punk", "california"],
    top_k=10,
    top_k_token=10_000,
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
            "score": 0.17841622233390808,
        }
    ],
    [
        {
            "id": "0",
            "title": "Hotel California",
            "style": "rock, pop",
            "date": Timestamp("1977-02-22 00:00:00"),
            "popularity": 9,
            "score": 0.156318798661232,
        }
    ],
]
```

### Filters

We can also filter the results using SQL syntax which will be evaluated by DuckDB, therefore all DuckDB functions are available.

```python
from ducksearch import search

search.documents(
    database="ducksearch.duckdb",
    queries="rock",
    top_k=10,
    top_k_token=10_000,
    filters="YEAR(date) < 1970 AND popularity > 9 AND style LIKE '%rock%'",
)
```

```python
[
    {
        "score": 0.08725740015506744,
        "id": "1",
        "title": "Here Comes the Sun",
        "style": "rock",
        "date": Timestamp("1969-06-10 00:00:00"),
        "popularity": 10,
    }
]
```

List of DuckDB functions such as date functions can be found [here](https://duckdb.org/docs/sql/functions/date).

## Extra features

### HuggingFace

The `upload.documents` function can also index HuggingFace datasets directly from the url. 
The following example demonstrates how to index the FineWeb dataset from HuggingFace:

```python
from ducksearch import upload

upload.documents(
    database="fineweb.duckdb",
    key="id",
    fields=["text", "url", "date", "language", "token_count", "language_score"],
    documents="https://huggingface.co/datasets/HuggingFaceFW/fineweb/resolve/main/sample/10BT/000_00000.parquet",
    dtypes={
        "date": "DATE",
        "token_count": "INT",
        "language_score": "FLOAT",
    },
    limit=1000, # demonstrate with a small dataset
)
```

We can then search the FineWeb dataset with the `search.documents` function:

```python
from ducksearch import search

search.documents(
    database="fineweb.duckdb",
    queries="earth science",
    top_k=2,
    top_k_token=10_000,
    filters="token_count > 200",
)
```

```python

[
    {
        "id": "<urn:uuid:1e6ae53b-e0d7-431b-8d46-290244e597e9>",
        "text": "Earth Science Tutors in Rowland ...",
        "date": Timestamp("2017-08-19 00:00:00"),
        "language": "en",
        "token_count": 313,
        "language_score": 0.8718525171279907,
        "score": 1.1588547229766846,
    },
    {
        "score": 1.6727683544158936,
        "id": "<urn:uuid:c732ce90-2fbf-41ad-8916-345f6c08e452>",
        "text": "The existing atmosphere surrounding the earth contains ...",
        "url": "http://www.accuracyingenesis.com/atmargon.html",
        "date": Timestamp("2015-04-02 00:00:00"),
        "language": "en",
        "token_count": 1348,
        "language_score": 0.9564403295516968,
    },
]
```

### Graphs

The `search.graphs` function can be used to search documents with a graph query. This function is useful if we have paired documents and queries. The search will retrieve the set of documents and queries that match the input query. Then it will build a graph and compute the weight of each document using a graph-based scoring function.

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

documents_queries = {
    0: ["the beatles", "rock band"],
    1: ["rock band", "california"],
    2: ["daft"],
}

upload.queries(
	database="ducksearch.duckdb",
	documents_queries=documents_queries,
)

search.graphs(
	database="ducksearch.duckdb",
	queries="daft punk",
	top_k=10,
	top_k_token=10_000,
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

## Lightning fast


## License

DuckSearch is released under the MIT license.

## Citation

```
@misc{PyLate,
  title={DuckSearch, efficient search with DuckDB},
  author={Sourty, Raphael},
  url={https://github.com/lightonai/ducksearch},
  year={2024}
}
```