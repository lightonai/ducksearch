import pandas as pd


def get_list_columns_df(
    documents: list[dict] | pd.DataFrame,
) -> list[str]:
    """Get a list of columns from a list of dictionaries or a DataFrame."""
    columns = None
    if isinstance(documents, pd.DataFrame):
        return list(documents.columns)

    if isinstance(documents, list):
        columns = set()
        for document in documents:
            for column in document.keys():
                if column != "id":
                    columns.add(column)
        return list(columns)

    return None
