"""Test module for functions from functions.py module."""
from data_joiner import functions

__docformat__ = 'restructuredtext'

def test_log_level_parser_warn(warn: int) -> None:
    """Testing log_level_parser with correct name"""
    assert functions.parser_log_levels('WARN') == warn


def test_log_level_parser_unexpected_argument(notset: int) -> None:
    """Testing log_level_parser with correct name"""
    assert functions.parser_log_levels('unexpected') == notset
