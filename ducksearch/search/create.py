import os
import shutil

import pyarrow as pa
import pyarrow.parquet as pq
from lenlp import counter
from tqdm import tqdm

from ..decorators import execute_with_duckdb
from ..tables import select_documents_columns


@execute_with_duckdb(
    relative_path="search/create/tables.sql",
)
def _create_tables() -> None:
    """Create the dedicated index tables."""


@execute_with_duckdb(
    relative_path="search/select/count_documents_to_index.sql",
    read_only=True,
    fetch_df=True,
)
def _count_documents_to_index() -> list:
    """Count the number of documents to index."""


@execute_with_duckdb(
    relative_path="search/select/get_documents_to_index.sql",
    read_only=True,
    fetch_df=True,
)
def _get_documents_to_index() -> list:
    """Get the documents to index."""


@execute_with_duckdb(
    relative_path="search/create/count.sql",
    fetch_df=True,
)
def _count_missing_documents():
    """Count the documents not in index."""


@execute_with_duckdb(
    relative_path="search/insert/documents.sql",
)
def _insert_documents() -> None:
    """Create the search index."""


@execute_with_duckdb(
    relative_path="search/insert/documents_lengths.sql",
)
def _insert_documents_lengths() -> None:
    """Create the search index."""


@execute_with_duckdb(
    relative_path="search/update/scores.sql",
)
def _update_scores():
    """Update the search index."""


@execute_with_duckdb(
    relative_path="search/select/count_documents_indexed.sql",
    read_only=True,
    fetch_df=True,
)
def _count_documents_indexed():
    """Count the number of documents indexed."""


def _update_index(
    database: str,
    schema: str,
    source: str,
    fields: str,
    ngram_range=(1, 1),
    analyzer: str = "word",
    normalize: bool = True,
    b: float = 0.75,
    k1: float = 1.5,
    config: dict | None = None,
    batch_size: int = 10_000,
    desc: str = "documents indexed",
) -> None:
    """Create the search index.

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

    """

    _create_tables(database=database, schema=schema, config=config)

    count = _count_documents_to_index(
        database=database,
        source=source,
        schema=schema,
        config=config,
    )[0]["count"]

    if count == 0:
        return "All documents are indexed."

    batch_size = max(min(batch_size, count), 1)
    steps = max(count // batch_size, 1)

    bar = tqdm(total=steps, desc="Updating index", position=0, leave=True)

    folder = os.path.join(".", "duckdb_tmp")
    index_path = os.path.join(".", "duckdb_tmp", "index")
    length_path = os.path.join(".", "duckdb_tmp", "length")

    if os.path.exists(path=folder):
        shutil.rmtree(folder)

    os.makedirs(name=index_path, exist_ok=True)
    os.makedirs(name=length_path, exist_ok=True)

    index = 0

    while True:
        data = _get_documents_to_index(
            database=database,
            schema=schema,
            source=source,
            fields=fields,
            batch_size=batch_size,
            config=config,
        )

        if len(data) == 0:
            break

        corpus_tokens = counter.count(
            [row["_search"] for row in data],
            ngram_range=ngram_range,
            analyzer=analyzer,
            normalize=normalize,
        )

        document_ids, tokens, tfs = [], [], []
        document_ids_lengths, document_lengths = [], []
        for step, (document_id, document_tokens) in enumerate(
            iterable=zip([row["id"] for row in data], corpus_tokens)
        ):
            document_ids_lengths.append(document_id)
            document_lengths.append(len(document_tokens))
            for token, frequency in document_tokens.items():
                document_ids.append(document_id)
                tokens.append(token)
                tfs.append(frequency)

        index_table = pa.Table.from_pydict(
            {
                "id": document_ids,
                "token": tokens,
                "tf": tfs,
            }
        )

        document_lengths_table = pa.Table.from_pydict(
            {
                "id": document_ids_lengths,
                "document_length": document_lengths,
            }
        )

        pq.write_table(
            index_table,
            os.path.join(index_path, f"{index}.parquet"),
            compression="snappy",
        )

        pq.write_table(
            document_lengths_table,
            os.path.join(length_path, f"{index}.parquet"),
            compression="snappy",
        )

        _insert_documents_lengths(
            database=database,
            schema=schema,
            parquet_files=os.path.join(length_path, f"{index}.parquet"),
            config=config,
        )

        index += 1
        bar.update()

    bar.close()

    _insert_documents(
        database=database,
        schema=schema,
        parquet_files=os.path.join(index_path, "*.parquet"),
        config=config,
    )

    if os.path.exists(path=folder):
        shutil.rmtree(folder)

    _update_scores(
        database=database,
        schema=schema,
        b=b,
        k1=k1,
        config=config,
    )

    count_documents_indexed = _count_documents_indexed(
        database=database, schema=schema, config=config
    )[0]["count"]

    return f"{count_documents_indexed} {desc}."


def update_index_documents(
    database: str,
    ngram_range=(1, 1),
    analyzer: str = "word",
    normalize: bool = True,
    b: float = 0.75,
    k1: float = 1.5,
    batch_size: int = 10_000,
    config: dict | None = None,
) -> None:
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
    ...     split="test"
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

    >>> search.update_index_documents(
    ...     database="test.duckdb",
    ... )
    '5183 documents indexed.'

    """
    fields = ", ".join(
        [
            f"s.{field}"
            for field in select_documents_columns(database=database, config=config)
        ]
    )

    return _update_index(
        database=database,
        schema="bm25_documents",
        source="documents",
        fields=fields,
        ngram_range=ngram_range,
        analyzer=analyzer,
        normalize=normalize,
        b=b,
        k1=k1,
        config=config,
        batch_size=batch_size,
        desc="documents indexed",
    )


def update_index_queries(
    database: str,
    ngram_range=(1, 1),
    analyzer: str = "word",
    normalize: bool = True,
    b: float = 0.75,
    k1: float = 1.5,
    batch_size: int = 10_000,
    config: dict | None = None,
) -> None:
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
    ...     split="test"
    ... )

    >>> upload.queries(
    ...     database="test.duckdb",
    ...     queries=queries,
    ...     documents_queries=qrels,
    ... )
    | Table          | Size |
    |----------------|------|
    | documents      | 5183 |
    | queries        | 300  |
    | bm25_documents | 5183 |

    >>> search.update_index_queries(
    ...     database="test.duckdb",
    ... )
    '300 queries indexed.'

    """
    return _update_index(
        database=database,
        schema="bm25_queries",
        source="queries",
        fields="s.query",
        ngram_range=ngram_range,
        analyzer=analyzer,
        normalize=normalize,
        b=b,
        k1=k1,
        config=config,
        batch_size=batch_size,
        desc="queries indexed",
    )
