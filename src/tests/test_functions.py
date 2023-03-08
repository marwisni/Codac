"""Test module for functions from functions.py module."""
# import chispa
from data_joiner import functions


def test_log_level_parser_warn(warn):
    """Testing log_level_parser with correct name"""
    assert functions.log_level_parser('WARN') == warn


def test_log_level_parser_unexpected_argument(notset):
    """Testing log_level_parser with correct name"""
    assert functions.log_level_parser('unexpected') == notset


# def test_column_rename_all_3_changes_should_be_done(source_df_5x1, expected_df_5x1_3chg, logger):
#     """Testing column_rename when all 3 changes should been done."""
#     actual_df = functions.columns_rename(source_df_5x1, {'a': 'f', 'c': 'g', 'e': 'd3'}, logger)
#     chispa.assert_df_equality(actual_df, expected_df_5x1_3chg)


# def test_column_rename_1_column_not_in_df(source_df_5x1, expected_df_5x1_2chg, logger):
#     """Testing column_rename when column which should be changed is not in dataframe"""
#     actual_df = functions.columns_rename(source_df_5x1, {'z': 'f', 'c': 'g', 'e': 'd3'}, logger)
#     chispa.assert_df_equality(actual_df, expected_df_5x1_2chg)


# def test_column_rename_with_empty_changes_dict(source_df_5x1, logger):
#     """Testing column_rename with empty changes dictionary"""
#     actual_df = functions.columns_rename(source_df_5x1, {}, logger)
#     chispa.assert_df_equality(actual_df, source_df_5x1)


# def test_country_filter_leaves_specified_countries_in_results(source_df_5x5, expected_df_5x2, logger):
#     """Testing country_filter when in Dataframe should stay only records from 2 specified countries"""
#     actual_df = functions.country_filter(source_df_5x5, 'country_2, country_5', logger)
#     chispa.assert_df_equality(actual_df, expected_df_5x2)


# def test_country_filter_country_not_in_df(source_df_5x5, expected_df_5x1_c, logger):
#     """Testing country_filter when one of the country expected to be filtered not exist in the dataframe."""
#     actual_df = functions.country_filter(source_df_5x5, 'country_2, country_6', logger)
#     chispa.assert_df_equality(actual_df, expected_df_5x1_c)


# def test_country_filter_empty_country_str(source_df_5x5, logger):
#     """Testing country_filter with empty country list - no change expected."""
#     actual_df = functions.country_filter(source_df_5x5, '', logger)
#     chispa.assert_df_equality(actual_df, source_df_5x5)
