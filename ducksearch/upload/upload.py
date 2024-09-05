import pandas as pd

from ..search import update_index_documents, update_index_queries
from ..tables import (
    create_documents,
    create_documents_queries,
    create_queries,
    insert_documents,
    insert_documents_queries,
    insert_queries,
)
from ..utils import plot


def documents(
    database: str,
    key: str,
    fields: str | list[str],
    documents: pd.DataFrame | list[dict] | str,
    config: dict | None = None,
) -> str:
    """Upload documents and qrels to duckdb.

    Parameters
    ----------
    database
        Database name.
    key
        Key identifier for the documents. Will be renamed to `id` in the database.
    fields
        Fields of the documents to upload.
    documents
        Documents to upload.
    config
        Configuration options for the DuckDB connection.

    """
    create_queries(database=database, config=config)
    create_documents(database=database, fields=fields, config=config)
    create_documents_queries(database=database, config=config)

    insert_documents(
        database=database, df=documents, key=key, fields=fields, config=config
    )

    return plot(
        database=database,
        config=config,
        tables=[
            "documents",
            "queries",
            "bm25_documents.lengths",
            "bm25_queries.lengths",
        ],
    )


def queries(
    database: str,
    queries: list[str] | None = None,
    documents_queries: dict[list] = None,
    config: dict | None = None,
) -> str:
    """Upload documents and qrels to duckdb.

    Parameters
    ----------
    database
        Database name.
    queries
        Queries to upload.
    documents_queries
        Mapping between documents ids and queries.
    config
        Configuration options for the DuckDB connection.

    """
    create_queries(database=database, config=config)
    create_documents_queries(database=database, config=config)

    if queries is not None:
        insert_queries(database=database, queries=queries, config=config)

    if documents_queries is not None:
        insert_documents_queries(
            database=database, documents_queries=documents_queries, config=config
        )

    return plot(
        database=database,
        config=config,
        tables=[
            "documents",
            "queries",
            "bm25_documents.lengths",
            "bm25_queries.lengths",
        ],
    )


def indexes(
    database: str,
    ngram_range=(1, 1),
    analyzer: str = "word",
    normalize: bool = True,
    b: float = 0.75,
    k1: float = 1.5,
    batch_size: int = 10_000,
    config: dict | None = None,
) -> str:
    """Create the index dedicated to search for documents.

    Parameters
    ----------
    database
        Database name.
    ngram_range
        The range of n-grams to consider.
    analyzer
        The analyzer to use.
    normalize
        Whether to normalize the tokens.
    b
        The impact of document length normalization.  Default is `0.75`, Higher will
        penalize longer documents more.
    k1
        How quickly the impact of term frequency saturates.  Default is `1.5`, Higher
        will make term frequency more influential.
    batch_size
        The batch size to use.
    config
        Configuration options for the DuckDB connection.

    Examples
    --------
    >>> from ducksearch import evaluation, upload, search

    >>> documents, queries, qrels = evaluation.load_beir(
    ...     "scifact",
    ...     split="train"
    ... )

    >>> upload.documents(
    ...     database="test.duckdb",
    ...     key="id",
    ...     fields=["title", "text"],
    ...     documents=documents,
    ... )
    | Table     | Size |
    |-----------|------|
    | documents | 5183 |

    >>> upload.queries(
    ...     database="test.duckdb",
    ...     queries=queries,
    ...     documents_queries=qrels,
    ... )
    | Table     | Size |
    |-----------|------|
    | documents | 5183 |
    | queries   | 807  |

    >>> upload.indexes(
    ...     database="test.duckdb",
    ... )
    | Table          | Size |
    |----------------|------|
    | documents      | 5183 |
    | queries        | 807  |
    | bm25_documents | 5183 |
    | bm25_queries   | 807  |

    """
    update_index_documents(
        database=database,
        ngram_range=ngram_range,
        analyzer=analyzer,
        normalize=normalize,
        b=b,
        k1=k1,
        batch_size=batch_size,
        config=config,
    )

    update_index_queries(
        database=database,
        ngram_range=ngram_range,
        analyzer=analyzer,
        normalize=normalize,
        b=b,
        k1=k1,
        batch_size=batch_size,
        config=config,
    )

    return plot(
        database=database,
        config=config,
        tables=[
            "documents",
            "queries",
            "bm25_documents.lengths",
            "bm25_queries.lengths",
        ],
    )
