import pandas as pd

from ..hf import insert_documents as hf_insert_documents
from ..search import update_index_documents, update_index_queries
from ..tables import (
    add_columns_documents,
    create_documents,
    create_documents_queries,
    create_queries,
    create_schema,
    insert_documents,
    insert_documents_queries,
    insert_queries,
    select_documents_columns,
)
from ..utils import get_list_columns_df, plot


def documents(
    database: str,
    key: str,
    fields: str | list[str],
    documents: list[dict] | str,
    k1: float = 1.5,
    b: float = 0.75,
    stemmer: str = "porter",
    stopwords: str | list[str] = None,
    ignore: str = "(\\.|[^a-z])+",
    strip_accents: bool = True,
    lower: bool = True,
    batch_size: int = 30_000,
    n_jobs: int = -1,
    dtypes: dict[str, str] | None = None,
    config: dict | None = None,
    limit: int | None = None,
    tqdm_bar: bool = True,
) -> str:
    """Upload documents to DuckDB, create necessary schema, and index using BM25.

    Parameters
    ----------
    database
        Name of the DuckDB database.
    key
        Key identifier for the documents. The key will be renamed to `id` in the database.
    fields
        List of fields to upload from each document. If a single field is provided as a string, it will be converted to a list.
    documents
        Documents to upload. Can be a list of dictionaries or a Hugging Face (HF) URL string pointing to a dataset.
    k1
        BM25 k1 parameter, controls term saturation.
    b
        BM25 b parameter, controls document length normalization.
    stemmer
        Stemming algorithm to use (e.g., 'porter'). The type of stemmer to be used. One of 'arabic', 'basque',
        'catalan', 'danish', 'dutch', 'english', 'finnish', 'french', 'german', 'greek', 'hindi', 'hungarian', 'indonesian', 'irish', 'italian', 'lithuanian',
        'nepali', 'norwegian', 'porter', 'portuguese', 'romanian', 'russian', 'serbian', 'spanish', 'swedish',
        'tamil', 'turkish', or 'none' if no stemming is to be used.
    stopwords
        List of stopwords to exclude from indexing.  Can be a custom list or a language string.
    ignore
        Regular expression pattern to ignore characters when indexing. Default ignore punctuation and non-alphabetic characters.
    strip_accents
        Whether to remove accents from characters during indexing.
    batch_size
        Number of documents to process per batch.
    n_jobs
        Number of parallel jobs to use for uploading documents. Default use all available processors.
    config
        Optional configuration dictionary for the DuckDB connection and other settings.
    tqdm_bar
        Whether to display a progress bar when uploading documents

    Returns
    -------
    str
        A string representing a visual plot of the document and query tables after the indexing process.

    """
    schema = "bm25_tables"

    create_schema(
        database=database,
        schema=schema,
        config=config,
    )

    create_queries(
        database=database,
        schema=schema,
        config=config,
    )

    columns = get_list_columns_df(
        documents=documents,
    )

    if isinstance(documents, str):
        hf_insert_documents(
            database=database,
            schema=schema,
            key=key,
            url=documents,
            config=config,
            limit=limit,
            dtypes=dtypes,
        )

    else:
        if isinstance(documents, pd.DataFrame):
            documents = documents.to_dict(orient="records")

        create_documents(
            database=database,
            schema=schema,
            dtypes=dtypes,
            columns=columns,
            config=config,
        )

        existing_columns = select_documents_columns(
            database=database,
            schema=schema,
            config=config,
        )

        existing_columns = set(existing_columns)
        columns_to_add = set(columns) - existing_columns
        if columns_to_add:
            add_columns_documents(
                database=database,
                schema=schema,
                columns=list(columns_to_add),
                dtypes=dtypes,
                config=config,
            )

        insert_documents(
            database=database,
            schema=schema,
            df=documents,
            key=key,
            columns=columns,
            batch_size=batch_size,
            dtypes=dtypes,
            n_jobs=n_jobs,
            config=config,
            limit=limit,
        )

    create_documents_queries(
        database=database,
        schema=schema,
        config=config,
    )

    fields = [field for field in fields if field != "id"]
    update_index_documents(
        database=database,
        fields=fields,
        b=b,
        k1=k1,
        stemmer=stemmer,
        stopwords=stopwords,
        ignore=ignore,
        strip_accents=strip_accents,
        lower=lower,
        batch_size=batch_size,
        config=config,
    )

    return plot(
        database=database,
        config=config,
        tables=[
            f"{schema}.documents",
            f"{schema}.queries",
            "bm25_documents.docs",
            "bm25_queries.docs",
            "bm25_tables.documents_queries",
        ],
    )


def queries(
    database: str,
    queries: list[str] | None = None,
    documents_queries: dict[list] = None,
    k1: float = 1.5,
    b: float = 0.75,
    stemmer: str = "porter",
    stopwords: str | list[str] = None,
    ignore: str = "(\\.|[^a-z])+",
    strip_accents: bool = True,
    lower: bool = True,
    batch_size: int = 30_000,
    config: dict | None = None,
) -> str:
    """Upload queries to DuckDB, map documents to queries, and index using BM25.

    Parameters
    ----------
    database
        Name of the DuckDB database.
    queries
        List of queries to upload. Each query is a string.
    documents_queries
        Dictionary mapping document IDs to a list of queries.
    k1
        BM25 k1 parameter, controls term saturation.
    b
        BM25 b parameter, controls document length normalization.
    stemmer
        Stemming algorithm to use (e.g., 'porter'). The type of stemmer to be used. One of 'arabic', 'basque',
        'catalan', 'danish', 'dutch', 'english', 'finnish', 'french', 'german', 'greek', 'hindi', 'hungarian', 'indonesian', 'irish', 'italian', 'lithuanian',
        'nepali', 'norwegian', 'porter', 'portuguese', 'romanian', 'russian', 'serbian', 'spanish', 'swedish',
        'tamil', 'turkish', or 'none' if no stemming is to be used.
    stopwords
        List of stopwords to exclude from indexing. Default can be a custom list or a language string.
    ignore
        Regular expression pattern to ignore characters when indexing. Default ignore punctuation and non-alphabetic characters.
    strip_accents
        Whether to remove accents from characters during indexing.
    batch_size
        Number of queries to process per batch.
    config
        Optional configuration dictionary for the DuckDB connection and other settings.

    Returns
    -------
    str
        A string representing a visual plot of the document and query tables after the indexing process.

    """
    schema = "bm25_tables"

    create_schema(
        database=database,
        schema=schema,
        config=config,
    )

    create_queries(
        database=database,
        schema=schema,
        config=config,
    )

    create_documents_queries(
        database=database,
        schema=schema,
        config=config,
    )

    if queries is not None:
        insert_queries(
            database=database,
            schema=schema,
            queries=queries,
            config=config,
        )

    if documents_queries is not None:
        insert_documents_queries(
            database=database,
            schema=schema,
            documents_queries=documents_queries,
            config=config,
        )

    update_index_queries(
        database=database,
        b=b,
        k1=k1,
        stemmer=stemmer,
        stopwords=stopwords,
        ignore=ignore,
        strip_accents=strip_accents,
        lower=lower,
        batch_size=batch_size,
        config=config,
    )

    return plot(
        database=database,
        config=config,
        tables=[
            f"{schema}.documents",
            f"{schema}.queries",
            "bm25_documents.docs",
            "bm25_queries.docs",
            "bm25_tables.documents_queries",
        ],
    )
