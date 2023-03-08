"""Test module for functions from functions.py module."""
from data_joiner import functions


def test_log_level_parser_warn(warn):
    """Testing log_level_parser with correct name"""
    assert functions.log_level_parser('WARN') == warn


def test_log_level_parser_unexpected_argument(notset):
    """Testing log_level_parser with correct name"""
    assert functions.log_level_parser('unexpected') == notset
