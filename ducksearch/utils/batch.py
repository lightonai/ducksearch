import tqdm


def batchify(
    X: list[str], batch_size: int, desc: str = "", tqdm_bar: bool = True
) -> list:
    """Iterate over a list in batches."""
    batchs = [X[pos : pos + batch_size] for pos in range(0, len(X), batch_size)]

    if tqdm_bar:
        for batch in tqdm.tqdm(
            batchs,
            position=0,
            total=1 + len(X) // batch_size,
            desc=desc,
        ):
            yield batch
    else:
        yield from batchs
