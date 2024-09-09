# batchify

Split a list into batches and optionally display a progress bar.



## Parameters

- **X** (*list[str]*)

    A list of items to be batched.

- **batch_size** (*int*)

    The number of items in each batch.

- **desc** (*str*) – defaults to ``

    A description to display in the progress bar.

- **tqdm_bar** (*bool*) – defaults to `True`

    Whether to display a progress bar using `tqdm`.



## Examples

```python
>>> items = ["a", "b", "c", "d", "e", "f"]
>>> batches = list(batchify(items, batch_size=2))
>>> for batch in batches:
...     print(batch)
['a', 'b']
['c', 'd']
['e', 'f']
```

