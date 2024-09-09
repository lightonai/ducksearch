import tqdm


def batchify(
    X: list[str], batch_size: int, desc: str = "", tqdm_bar: bool = True
) -> list:
    """Split a list into batches and optionally display a progress bar.

    Parameters
    ----------
    X
        A list of items to be batched.
    batch_size
        The number of items in each batch.
    desc
        A description to display in the progress bar. Default is an empty string.
    tqdm_bar
        Whether to display a progress bar using `tqdm`. Default is True.

    Yields
    ------
    list
        A list representing a batch of items from `X`.

    Examples
    --------
    >>> items = ["a", "b", "c", "d", "e", "f"]
    >>> batches = list(batchify(items, batch_size=2))
    >>> for batch in batches:
    ...     print(batch)
    ['a', 'b']
    ['c', 'd']
    ['e', 'f']

    """
    # Split the input list `X` into batches
    batches = [X[pos : pos + batch_size] for pos in range(0, len(X), batch_size)]

    # Use tqdm to show a progress bar if `tqdm_bar` is set to True
    if tqdm_bar:
        for batch in tqdm.tqdm(
            batches,
            position=0,
            total=len(batches),
            desc=desc,
        ):
            yield batch
    else:
        # If no progress bar is needed, simply yield the batches
        yield from batches
