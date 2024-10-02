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
DuckSearch is a lightweight and easy-to-use library to search documents. DuckSearch is built on top of DuckDB, a high-performance analytical database. DuckDB is designed to execute analytical SQL queries fast, and DuckSearch leverages this to provide efficient search and filtering features. DuckSearch index can be updated with new documents and documents can be deleted as well. DuckSearch also supports HuggingFace datasets, allowing to index datasets directly from the HuggingFace Hub.
</p>

## Installation

Install DuckSearch using pip:

```bash
pip install ducksearch
```

## Documentation 

The complete documentation is available [here](https://lightonai.github.io/ducksearch/), which includes in-depth guides, examples, and API references.

### Upload

We can upload documents to DuckDB using the `upload.documents` function. The documents are stored in a DuckDB database, and the `fields` are indexed with BM25.

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
    key="id", # Unique document identifier
    fields=["title", "style"], # List of fields to use for search.
    documents=documents,
    dtypes={
        "date": "DATE",
        "popularity": "INT",
    },
)
```

## Search

`search.documents` returns a list of list of documents ordered by relevance. We can control the number of documents to return using the `top_k` parameter. The following example demonstrates how to search for documents with the queries "punk" and "california" while filtering the results to include only documents with a date after 1970 and a popularity score greater than 8.

```python
from ducksearch import search

search.documents(
    database="ducksearch.duckdb",
    queries=["punk", "california"],
    top_k=10,
    filters="YEAR(date) >= 1970 AND popularity > 8",
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

Filters are SQL expressions that are applied to the search results. We can use every filtering function DuckDB provides such as [date functions](https://duckdb.org/docs/sql/functions/date).

## Delete and update index

We can delete documents and update the BM25 weights accordingly using the `delete.documents` function.

```python
from ducksearch import delete

delete.documents(
    database="ducksearch.duckdb",
    ids=[0, 1],
)
```

To update the index, we should first delete the documents and then upload the updated documents.

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

## Tables

Ducksearch creates two distinct schemas: `bm25_tables`, `bm25_documents`.

- We can find the uploaded documents in the `bm25_tables.documents` table.

- We can find the inverted index in the `bm25_documents.scores` table. You can update the scores as you wish. Just note that tokens scores will be updated each time you upload documents (every tokens scores mentionned in the set of uploaded documents).

- We can update the set of stopwords in the `bm25_documents.stopwords` table.

## Benchmark


| Dataset           | ndcg@10   | hits@1  | hits@10  | mrr@10   | map@10  | r-precision | qps            | Indexation Time (s) | Number of Documents and Queries |
|-------------------|-----------|---------|----------|----------|---------|-------------|----------------|---------------------|--------------------------------|
| arguana            | 0.3779    | 0.0     | 0.8267   | 0.2491   | 0.2528  | 0.0108      | 117.80         | 1.42                | 1,406 queries, 8.67K documents |
| climate-fever      | 0.1184    | 0.1068  | 0.3648   | 0.1644   | 0.0803  | 0.0758      | 5.88           | 302.39              | 1,535 queries, 5.42M documents |
| dbpedia-entity     | 0.6046    | 0.7669  | 5.6241   | 0.8311   | 0.0649  | 0.0741      | 113.20         | 181.42              | 400 queries, 4.63M documents   |
| fever              | 0.3861    | 0.2583  | 0.5826   | 0.3525   | 0.3329  | 0.2497      | 74.40          | 329.70              | 6,666 queries, 5.42M documents |
| fiqa               | 0.2445    | 0.2207  | 0.6790   | 0.3002   | 0.1848  | 0.1594      | 545.77         | 6.04                | 648 queries, 57K documents     |
| hotpotqa           | 0.4487    | 0.5059  | 0.9699   | 0.5846   | 0.3642  | 0.3388      | 48.15          | 163.14              | 7,405 queries, 5.23M documents |
| msmarco            | 0.8951    | 1.0     | 8.6279   | 1.0      | 0.0459  | 0.0473      | 35.11          | 202.37              | 6,980 queries, 8.84M documents |
| nfcorpus           | 0.3301    | 0.4396  | 2.4087   | 0.5292   | 0.1233  | 0.1383      | 3464.66        | 0.99                | 323 queries, 3.6K documents    |
| nq                 | 0.2451    | 0.1272  | 0.4574   | 0.2099   | 0.1934  | 0.1240      | 150.23         | 71.43               | 3,452 queries, 2.68M documents |
| quora              | 0.7705    | 0.6783  | 1.1749   | 0.7606   | 0.7206  | 0.6502      | 741.13         | 3.78                | 10,000 queries, 523K documents |
| scidocs            | 0.1025    | 0.1790  | 0.8240   | 0.2754   | 0.0154  | 0.0275      | 879.11         | 4.46                | 1,000 queries, 25K documents   |
| scifact            | 0.6908    | 0.5533  | 0.9133   | 0.6527   | 0.6416  | 0.5468      | 2153.64        | 1.22                | 300 queries, 5K documents      |
| trec-covid         | 0.9533    | 1.0     | 9.4800   | 1.0      | 0.0074  | 0.0077      | 112.38         | 22.15               | 50 queries, 171K documents     |
| webis-touche2020   | 0.4130    | 0.5510  | 3.7347   | 0.7114   | 0.0564  | 0.0827      | 104.65         | 44.14               | 49 queries, 382K documents     |

## References

- [DuckDB](https://duckdb.org/)

- [DuckDB Full Text Search](https://duckdb.org/docs/extensions/full_text_search.html): Note that DuckSearch rely partially on the DuckDB Full Text Search extension but accelerate the search process via `top_k_token` approximation, pre-computation of scores and multi-threading.

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