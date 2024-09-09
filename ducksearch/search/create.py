import logging
import os

import pyarrow as pa
import pyarrow.parquet as pq

from ..decorators import execute_with_duckdb
from ..tables import select_documents_columns
from ..utils import batchify


@execute_with_duckdb(
    relative_path="search/create/tables.sql",
)
def _create_tables() -> None:
    """Create the necessary index tables in the DuckDB database."""


@execute_with_duckdb(
    relative_path="search/drop/scores.sql",
)
def _drop_scores_to_recompute() -> None:
    """Drop the BM25 scores to recompute from the DuckDB database."""


@execute_with_duckdb(
    relative_path="search/select/settings_exists.sql",
    fetch_df=True,
)
def _settings_exists() -> None:
    """Check if index settings already exist in the DuckDB database."""


@execute_with_duckdb(
    relative_path="search/create/settings.sql",
)
def _create_settings() -> None:
    """Create index settings in the DuckDB database."""


@execute_with_duckdb(
    relative_path="search/insert/settings.sql",
)
def _insert_settings() -> None:
    """Insert index settings into the DuckDB database."""


@execute_with_duckdb(
    relative_path="search/create/stopwords.sql",
)
def _insert_stopwords() -> None:
    """Insert custom stopwords into the DuckDB database."""


@execute_with_duckdb(
    relative_path="search/select/settings.sql",
    read_only=True,
    fetch_df=True,
)
def _select_settings() -> None:
    """Select index settings from the DuckDB database."""


@execute_with_duckdb(
    relative_path="search/create/index.sql",
)
def _create_index() -> None:
    """Create the search index in the DuckDB database by parsing document tokens."""


@execute_with_duckdb(
    relative_path=[
        "search/update/dict.sql",
        "search/insert/dict.sql",
    ],
)
def _update_dict() -> None:
    """Update the dictionary for the search index."""


@execute_with_duckdb(
    relative_path=[
        "search/insert/docs.sql",
    ],
)
def _update_docs() -> None:
    """Update document tokens in the search index."""


@execute_with_duckdb(
    relative_path=[
        "search/update/stats.sql",
    ],
)
def _update_stats() -> None:
    """Update the statistics of the search index."""


@execute_with_duckdb(
    relative_path="search/insert/terms.sql",
)
def _update_terms() -> None:
    """Update the terms in the search index."""


@execute_with_duckdb(
    relative_path="search/select/termids_to_score.sql",
    read_only=True,
    fetch_df=True,
)
def _termids_to_score() -> None:
    """Retrieve term IDs for scoring in the search index."""


@execute_with_duckdb(
    relative_path="search/select/stats.sql",
    fetch_df=True,
    read_only=True,
)
def _stats():
    """Fetch the current statistics of the search index."""


@execute_with_duckdb(
    relative_path="search/update/scores.sql",
)
def _update_scores() -> None:
    """Update the BM25 scores in the search index."""


@execute_with_duckdb(
    relative_path="search/drop/schema.sql",
)
def _drop_schema() -> None:
    """Drop the schema from the DuckDB database."""


@execute_with_duckdb(
    relative_path="search/drop/_documents.sql",
)
def _drop_documents() -> None:
    """Drop the documents table from the DuckDB database."""


@execute_with_duckdb(
    relative_path="search/update/bm25id.sql",
)
def _update_bm25id() -> None:
    """Update the BM25 index ID in the DuckDB database."""


def update_index(
    database: str,
    bm25_schema: str,
    source_schema: str,
    source: str,
    key: str,
    fields: str,
    k1: float = 1.5,
    b: float = 0.75,
    stemmer: str = "porter",
    stopwords: str | list[str] = "english",
    ignore: str = "(\\.|[^a-z])+",
    strip_accents: bool = True,
    lower: bool = True,
    config: dict | None = None,
    batch_size: int = 10_000,
) -> None:
    """Create or update the BM25 search index for the documents or queries table.

    Parameters
    ----------
    database
        The name of the DuckDB database.
    bm25_schema
        The schema where the BM25 index is created.
    source_schema
        The schema where the original documents or queries are stored.
    source
        The name of the source table (either "documents" or "queries").
    key
        The key field identifying each document or query (e.g., "id").
    fields
        The fields to index for each document or query.
    k1
        The BM25 k1 parameter, controls term saturation.
    b
        The BM25 b parameter, controls document length normalization.
    stemmer
        The stemming algorithm to use (e.g., 'porter').
    stopwords
        The list of stopwords to exclude from indexing. Can be a list or a string specifying the language (e.g., "english").
    ignore
        A regex pattern to ignore characters during tokenization. Default ignores punctuation and non-alphabetic characters.
    strip_accents
        Whether to remove accents from characters during indexing.
    lower
        Whether to convert text to lowercase during indexing.
    config
        Optional configuration settings for the DuckDB connection.
    batch_size
        The number of documents or queries to process per batch.

    Examples
    --------
    >>> from ducksearch import evaluation, upload, search

    >>> documents, queries, qrels = evaluation.load_beir("scifact", split="test")

    >>> upload.documents(
    ...     database="test.duckdb",
    ...     key="id",
    ...     fields=["title", "text"],
    ...     documents=documents,
    ...     stopwords=["larva"],
    ... )
    | Table          | Size |
    |----------------|------|
    | documents      | 5183 |
    | bm25_documents | 5183 |

    """
    if stemmer is None or not stemmer:
        stemmer = "none"

    if stopwords is None or not stopwords:
        stopwords = []

    if isinstance(fields, list):
        fields = ", ".join(fields)

    _create_tables(
        database=database,
        schema=bm25_schema,
        source_schema=source_schema,
        source=source,
        key_field=key,
        fields=fields,
        config=config,
    )

    settings_exists = _settings_exists(
        database=database,
        schema=bm25_schema,
        config=config,
    )[0]["table_exists"]

    if not settings_exists:
        if isinstance(stopwords, list):
            stopwords_table = pa.Table.from_pydict({"sw": stopwords})
            pq.write_table(stopwords_table, "_stopwords.parquet", compression="snappy")

            _insert_stopwords(
                database=database,
                schema=bm25_schema,
                parquet_file="_stopwords.parquet",
                config=config,
            )
            stopwords = f"{bm25_schema}.stopwords"

            if os.path.exists("_stopwords.parquet"):
                os.remove("_stopwords.parquet")

        _create_settings(
            database=database,
            schema=bm25_schema,
            config=config,
        )

        _insert_settings(
            database=database,
            schema=bm25_schema,
            k1=k1,
            b=b,
            stemmer=stemmer,
            stopwords=stopwords,
            ignore=ignore,
            strip_accents=strip_accents,
            lower=lower,
            config=config,
        )

    settings = _select_settings(
        database=database,
        schema=bm25_schema,
        config=config,
    )[0]

    if (
        settings["k1"] != k1
        or settings["b"] != b
        or settings["stemmer"] != stemmer
        or settings["stopwords"] != stopwords
        or settings["ignore"] != ignore
        or settings["strip_accents"] != int(strip_accents)
        or settings["lower"] != int(lower)
    ):
        logging.warning(
            f"Original settings differ from the selected settings. Using original settings: {settings}"
        )

    logging.info("Parsing document tokens.")
    _create_index(
        database=database,
        schema=bm25_schema,
        **settings,
        config=config,
    )

    logging.info("Updating index metadata.")
    _update_dict(
        database=database,
        schema=bm25_schema,
        config=config,
    )

    _update_docs(
        database=database,
        schema=bm25_schema,
        config=config,
    )

    _update_stats(
        database=database,
        schema=bm25_schema,
        config=config,
    )

    _update_terms(
        database=database,
        schema=bm25_schema,
        config=config,
    )

    termids_to_score = _termids_to_score(
        database=database,
        schema=bm25_schema,
        config=config,
        max_df=100_000,
    )

    _drop_scores_to_recompute(
        database=database,
        schema=bm25_schema,
        config=config,
    )

    stats = _stats(
        database=database,
        schema=bm25_schema,
        config=config,
    )[0]

    num_docs = stats["num_docs"]
    avgdl = stats["avgdl"]

    for batch in batchify(
        termids_to_score,
        batch_size=batch_size,
        desc="Indexing",
    ):
        termids = pa.Table.from_pydict({"termid": [term["termid"] for term in batch]})
        pq.write_table(termids, "_termids.parquet", compression="snappy")

        _update_scores(
            database=database,
            schema=bm25_schema,
            num_docs=num_docs,
            avgdl=avgdl,
            parquet_file="_termids.parquet",
            k1=settings["k1"],
            b=settings["b"],
            config=config,
        )

    _drop_schema(
        database=database,
        schema=bm25_schema,
        config=config,
    )

    _drop_documents(
        database=database,
        schema=bm25_schema,
        config=config,
    )

    _update_bm25id(
        database=database,
        schema=bm25_schema,
        source_schema=source_schema,
        source=source,
        config=config,
    )

    if os.path.exists("_termids.parquet"):
        os.remove("_termids.parquet")


def update_index_documents(
    database: str,
    k1: float = 1.5,
    b: float = 0.75,
    stemmer: str = "porter",
    stopwords: str | list[str] = "english",
    ignore: str = "(\\.|[^a-z])+",
    strip_accents: bool = True,
    lower: bool = True,
    batch_size: int = 10_000,
    config: dict | None = None,
) -> None:
    """Update the BM25 search index for documents.

    Parameters
    ----------
    database
        The name of the DuckDB database.
    k1
        The BM25 k1 parameter, controls term saturation.
    b
        The BM25 b parameter, controls document length normalization.
    stemmer
        The stemming algorithm to use (e.g., 'porter').
    stopwords
        The list of stopwords to exclude from indexing. Can be a list or a string specifying the language (e.g., "english").
    ignore
        A regex pattern to ignore characters during tokenization. Default ignores punctuation and non-alphabetic characters.
    strip_accents
        Whether to remove accents from characters during indexing.
    batch_size
        The number of documents to process per batch.
    config
        Optional configuration settings for the DuckDB connection.

    Examples
    --------
    >>> from ducksearch import evaluation, upload, search

    >>> documents, queries, qrels = evaluation.load_beir("scifact", split="test")

    >>> upload.documents(
    ...     database="test.duckdb",
    ...     key="id",
    ...     fields=["title", "text"],
    ...     documents=documents,
    ...     stopwords=["larva"],
    ... )
    | Table          | Size |
    |----------------|------|
    | documents      | 5183 |
    | bm25_documents | 5183 |

    """
    fields = ", ".join(
        select_documents_columns(
            database=database,
            schema="bm25_tables",
            config=config,
        )
    )

    update_index(
        database=database,
        k1=k1,
        b=b,
        stemmer=stemmer,
        stopwords=stopwords,
        ignore=ignore,
        strip_accents=strip_accents,
        lower=lower,
        bm25_schema="bm25_documents",
        source_schema="bm25_tables",
        source="documents",
        key="id",
        fields=fields,
        config=config,
        batch_size=batch_size,
    )


def update_index_queries(
    database: str,
    k1: float = 1.5,
    b: float = 0.75,
    stemmer: str = "porter",
    stopwords: str | list[str] = "english",
    ignore: str = "(\\.|[^a-z])+",
    strip_accents: bool = True,
    lower: bool = True,
    batch_size: int = 10_000,
    config: dict | None = None,
) -> None:
    """Update the BM25 search index for queries.

    Parameters
    ----------
    database
        The name of the DuckDB database.
    k1
        The BM25 k1 parameter, controls term saturation.
    b
        The BM25 b parameter, controls document length normalization.
    stemmer
        The stemming algorithm to use (e.g., 'porter').
    stopwords
        The list of stopwords to exclude from indexing. Can be a list or a string specifying the language (e.g., "english").
    ignore
        A regex pattern to ignore characters during tokenization. Default ignores punctuation and non-alphabetic characters.
    strip_accents
        Whether to remove accents from characters during indexing.
    batch_size
        The number of queries to process per batch.
    config
        Optional configuration settings for the DuckDB connection.

    Examples
    --------
    >>> from ducksearch import evaluation, upload, search

    >>> documents, queries, qrels = evaluation.load_beir("scifact", split="test")

    >>> upload.queries(
    ...     database="test.duckdb",
    ...     queries=queries,
    ...     documents_queries=qrels,
    ... )
    | Table             | Size |
    |-------------------|------|
    | documents         | 5183 |
    | queries           | 300  |
    | bm25_documents    | 5183 |
    | bm25_queries      | 300  |
    | documents_queries | 339  |

    """
    update_index(
        database=database,
        k1=k1,
        b=b,
        stemmer=stemmer,
        stopwords=stopwords,
        ignore=ignore,
        strip_accents=strip_accents,
        lower=lower,
        bm25_schema="bm25_queries",
        source_schema="bm25_tables",
        source="queries",
        key="id",
        fields="query",
        config=config,
        batch_size=batch_size,
    )
