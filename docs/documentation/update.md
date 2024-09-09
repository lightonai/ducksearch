## Update

To update a document, you to first delete the document and then upload the updated document. The delete operation will remove the document from the database and update the index. Finally, the upload operation will add the updated document to the database and update the index.

```python
from ducksearch import delete, upload

delete.documents(
    database="ducksearch.duckdb",
    ids=[0, 1],
)

documents_updated = [
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
]

upload.documents(
    database="ducksearch.duckdb",
    key="id",
    fields=["title", "style", "date", "popularity"],
    documents=documents_updated,
    dtypes={
        "date": "DATE",
        "popularity": "INT",
    },
)
```
