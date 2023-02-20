import pathlib

LOGS = {
    'level': 20,
    'path': pathlib.Path(__file__).parents[2].joinpath('logs'),
    'maxBytes': 1024,
    'backupCount': 3
}

SOURCES = {
    'first': str(pathlib.Path(__file__).parent.joinpath('source_data/dataset_one.csv')),
    'second': str(pathlib.Path(__file__).parent.joinpath('source_data/dataset_two.csv')),
    'countries': 'United Kingdom, Netherlands',
}

CHANGES = {
    'id': 'client_identifier',
    'btc_a': 'bitcoin_address',
    'cc_t':  'credit_card_type'
}

OUTPUT = str(pathlib.Path(__file__).parent.joinpath('client_data'))
