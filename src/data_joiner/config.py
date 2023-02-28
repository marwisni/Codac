"""Application configuration module.
    - LOGS - here put data about your logging preferences.
    - SOURCES - here put paths for sources csv files and/or countries you would like to filter.
    - CHANGES - dictionary for column name changes in schema "old: "new" (can be empty)
    - OUTPUT - path to output directory to save results.
"""
import pathlib

LOGS = {
    'level': 'INFO',
    'path': pathlib.Path(__file__).parents[2].joinpath('logs'),
    'maxBytes': 1024,
    'backupCount': 3
}

SOURCES = {
    'first': str(pathlib.Path(__file__).parent.joinpath('source_data/dataset_one.csv')),
    'second': str(pathlib.Path(__file__).parent.joinpath('source_data/dataset_two.csv')),
    'countries': 'United Kingdom, Netherlands',
}

RENAME = {
    'id': 'client_identifier',
    'btc_a': 'bitcoin_address',
    'cc_t':  'credit_card_type'
}

SELECT = ['id', 'email', 'country']
DROP = ['cc_n']
JOIN = ['id']
OUTPUT = str(pathlib.Path(__file__).parent.joinpath('client_data'))
