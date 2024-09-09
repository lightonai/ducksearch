## Delete

To delete a document, you need to provide the document's ID. The delete operation will remove the document from the database and update the index.

```python
from ducksearch import delete, upload

delete.documents(
    database="ducksearch.duckdb",
    ids=[0, 1],
)
```