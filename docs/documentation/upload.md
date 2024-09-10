## Upload

When working with DuckSearch, the first step is to upload documents to DuckDB using the `upload.documents` function. The documents are stored in a DuckDB database, and the fields are indexed with BM25. DuckSearch won't re-index a document if it already exists in the database. Index will be updated along with the new documents.

### Upload documents

The following example demonstrates how to upload a list of documents:

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
    stopwords="english",
    stemmer="porter",
    lower=True,
    strip_accents=True,
    dtypes={
        "date": "DATE",
        "popularity": "INT",
    },
)
```

???+ info
    stopwords: List of stop words to filter Defaults to 'english' for a pre-defined list of 571 English stopwords.

    stemmer: Stemmer to use. Defaults to 'porter' for the Porter stemmer. Possible values are: 'arabic', 'basque', 'catalan', 'danish', 'dutch', 'english', 'finnish', 'french', 'german', 'greek', 'hindi', 'hungarian', 'indonesian', 'irish', 'italian', 'lithuanian', 'nepali', 'norwegian', 'porter', 'portuguese', 'romanian', 'russian', 'serbian', 'spanish', 'swedish', 'tamil', 'turkish', or `None` if no stemming is to be used.

    lower: Whether to convert the text to lowercase. Defaults to `True`.

    strip_accents: Whether to strip accents from the text. Defaults to `True`.

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

???+ info
    More informations about DuckDB and HuggingFace compatibility can be found [here](https://huggingface.co/docs/hub/en/datasets-duckdb) and [here](https://duckdb.org/2024/05/29/access-150k-plus-datasets-from-hugging-face-with-duckdb.html).
