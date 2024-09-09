# load_beir

Load BEIR dataset for document and query retrieval tasks.



## Parameters

- **dataset_name** (*str*)

    The name of the dataset to load (e.g., 'scifact').

- **split** (*str*) – defaults to `test`

    The dataset split to load (e.g., 'test').



## Examples

```python
>>> documents, queries, qrels = load_beir("scifact", split="test")

>>> len(documents)
5183

>>> len(queries)
300
```

