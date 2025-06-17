"""
This module contains inner config variables instanciation.
(to be deferenciated from deploy config variables used for deploying prefect's workflow in production)."
"""

# Mapping for columns to be cleaned within each source of data
COLS_CLEAN_MAPPING = {
    "drugs": {
        "date_columns": [],
        "drop_na_columns": ["drug"],
        "text_search_columns": ["drug"],
        "id_prefix": "drug",
        "id_column": "atccode"
    },
    "pubmed": {
            "date_columns": ["date"],
            "drop_na_columns": ["title", "journal"],
            "text_search_columns": ["title"],
            "id_prefix": "pubmed",
            "id_column": "id"
    },
    "clinical": {
            "date_columns": ["date"],
            "drop_na_columns": ["scientific_title", "journal"],
            "text_search_columns": ["scientific_title"],
            "id_prefix": "clinical",
            "id_column": "id"
    }
}

# Mapping for handling matching drugs within each source of publications data
COLS_MATCH_MAPPING = {
    "drugs_clinical": {
        "drug_col_name": "drug",
        "pub_title_col_name": "scientific_title",
        "journal_col_name": "journal",
        "date_col_name": "date",
        "data_source": "clinical"
    },
    "drugs_pubmed": {
        "drug_col_name": "drug",
        "pub_title_col_name": "title",
        "journal_col_name": "journal",
        "date_col_name": "date",
        "data_source": "pubmed"
    }
}