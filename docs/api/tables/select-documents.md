# select_documents

Select all documents from the documents table.





## Examples

```python
>>> from ducksearch import tables

>>> documents = tables.select_documents(
...     database="test.duckdb",
...     schema="bm25_tables",
... )

>>> assert len(documents) == 3
```

