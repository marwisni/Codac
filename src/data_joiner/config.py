"""Application configuration module.
    - LOGS - here put data about your logging preferences.
    - SOURCES - here put paths for sources csv files and/or countries you would like to filter.
    - CHANGES - dictionary for column name changes in schema "old: "new" (can be empty)
    - OUTPUT - path to output directory to save results.
"""
LOGS = {
    'level': 'INFO',
    'path': './logs',
    'maxBytes': 1024 * 2,
    'backupCount': 3
}

SOURCES = {
    'first': './source_data/dataset_one.csv',
    'second': './source_data/dataset_two.csv',
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
OUTPUT = './client_data'
