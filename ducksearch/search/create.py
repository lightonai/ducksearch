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
    """Create the dedicated index tables."""


@execute_with_duckdb(
    relative_path="search/select/settings_exists.sql",
    fetch_df=True,
)
def _settings_exists() -> None:
    """Check if the settings exist."""


@execute_with_duckdb(
    relative_path="search/create/settings.sql",
)
def _create_settings() -> None:
    """Check if the settings exist."""


@execute_with_duckdb(
    relative_path="search/insert/settings.sql",
)
def _insert_settings() -> None:
    """Check if the settings exist."""


@execute_with_duckdb(
    relative_path="search/create/stopwords.sql",
)
def _insert_stopwords() -> None:
    """Check if the settings exist."""


@execute_with_duckdb(
    relative_path="search/select/settings.sql",
    read_only=True,
    fetch_df=True,
)
def _select_settings() -> None:
    """Check if the settings exist."""


@execute_with_duckdb(
    relative_path="search/create/index.sql",
)
def _create_index() -> None:
    """Parse documents tokens."""


@execute_with_duckdb(
    relative_path=[
        "search/update/dict.sql",
        "search/insert/dict.sql",
    ],
)
def _update_dict() -> None:
    """Parse documents tokens."""


@execute_with_duckdb(
    relative_path=[
        "search/insert/docs.sql",
    ],
)
def _update_docs() -> None:
    """Parse documents tokens."""


@execute_with_duckdb(
    relative_path=[
        "search/update/stats.sql",
    ],
)
def _update_stats() -> None:
    """Parse documents tokens."""


@execute_with_duckdb(
    relative_path="search/insert/terms.sql",
)
def _update_terms() -> None:
    """Parse documents tokens."""


@execute_with_duckdb(
    relative_path="search/select/termids_to_score.sql",
    read_only=True,
    fetch_df=True,
)
def _termids_to_score() -> None:
    """Parse documents tokens."""


@execute_with_duckdb(
    relative_path="search/select/stats.sql",
    fetch_df=True,
    read_only=True,
)
def _stats():
    """Update the search index."""


@execute_with_duckdb(
    relative_path="search/update/scores.sql",
)
def _update_scores() -> None:
    """Update the search index."""


@execute_with_duckdb(
    relative_path="search/drop/schema.sql",
)
def _drop_schema() -> None:
    """Drop the schema."""


@execute_with_duckdb(
    relative_path="search/drop/_documents.sql",
)
def _drop_documents() -> None:
    """Drop the schema."""


@execute_with_duckdb(
    relative_path="search/update/bm25id.sql",
)
def _update_bm25id() -> None:
    """Update the search index."""


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
    if isinstance(fields, list):
        fields = ", ".join([f"{field}" for field in fields])

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
        if not isinstance(stopwords, str):
            stopwords_table = pa.Table.from_pydict(
                {
                    "sw": stopwords,
                }
            )

            pq.write_table(
                table=stopwords_table,
                where="_stopwords.parquet",
                compression="snappy",
            )

            _insert_stopwords(
                database=database,
                schema=bm25_schema,
                parquet_file="_stopwords.parquet",
                config=config,
            )

            stopwords = f"{bm25_schema}.stopwords"

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
            f"Original settings are different from the selected settings. Settings used: {settings}"
        )

    logging.info(msg="Parsing documents tokens.")
    _create_index(
        database=database,
        schema=bm25_schema,
        **settings,
        config=config,
    )

    logging.info(msg="Updating index metadata.")
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

    stats = _stats(
        database=database,
        schema=bm25_schema,
        config=config,
    )

    num_docs = stats[0]["num_docs"]
    avgdl = stats[0]["avgdl"]

    for batch in batchify(
        X=termids_to_score,
        batch_size=batch_size,
        desc="Indexing",
    ):
        termids = pa.Table.from_pydict(
            {
                "termid": [term["termid"] for term in batch],
            }
        )

        pq.write_table(
            table=termids,
            where="_termids.parquet",
            compression="snappy",
        )

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
        os.remove(path="_termids.parquet")


def update_index_documents(
    database: str,
    k1: float = 1.5,
    b: float = 0.75,
    stemmer: str = "porter",
    stopwords: str | list[str] = "english",
    ignore: str = "(\\.|[^a-z])+",
    strip_accents: bool = True,
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
    stop_words
        The list of stop words to drop. Default is `None`.
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
    ...     stopwords=["larva"],
    ... )
    | Table          | Size |
    |----------------|------|
    | documents      | 5183 |
    | bm25_documents | 5183 |

    """
    fields = ", ".join(
        [
            f"{field}"
            for field in select_documents_columns(
                database=database, schema="bm25_tables", config=config
            )
        ]
    )

    return update_index(
        database=database,
        k1=k1,
        b=b,
        stemmer=stemmer,
        stopwords=stopwords,
        ignore=ignore,
        strip_accents=strip_accents,
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
    stop_words
        The list of stop words to drop. Default is `None`.
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
    | Table             | Size |
    |-------------------|------|
    | documents         | 5183 |
    | queries           | 300  |
    | bm25_documents    | 5183 |
    | bm25_queries      | 300  |
    | documents_queries | 339  |

    """
    return update_index(
        database=database,
        k1=k1,
        b=b,
        stemmer=stemmer,
        stopwords=stopwords,
        ignore=ignore,
        strip_accents=strip_accents,
        bm25_schema="bm25_queries",
        source_schema="bm25_tables",
        source="queries",
        key="id",
        fields="query",
        config=config,
        batch_size=batch_size,
    )
