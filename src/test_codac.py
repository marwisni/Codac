"""Test module for functions from tools.py module."""
from logging import getLogger
import chispa
from pyspark.sql import SparkSession
from pytest import fixture
from data_joiner.functions import columns_rename, country_filter

@fixture(name="spark")
def fixture_spark():
    """Initialization of test spark session"""
    return SparkSession.builder.appName('test_codac').getOrCreate()


@fixture(name="logger")
def fixture_logger():
    """Initialization of test logger"""
    return getLogger()


@fixture(name="source_df_5x1")
def fixture_source_df_5x1(spark):
    """Generating source dummy DataFrame with 5 columns and 1 row"""
    data = [('abc', 'abc', 'abc', 'abc', 'abc')]
    return spark.createDataFrame(data, ['a', 'b', 'c', 'd', 'e'])

@fixture(name="expected_df_5x1_3chg")
def fixture_expected_df_5x1_3chg(spark):
    """Generating expected dummy DataFrame with 5 columns and 1 row after 3 changes."""
    data = [('abc', 'abc', 'abc', 'abc', 'abc')]
    return spark.createDataFrame(data, ['f', 'b', 'g', 'd', 'd3'])


@fixture(name="expected_df_5x1_2chg")
def fixture_expected_df_5x1_2chg(spark):
    """Generating expected dummy DataFrame with 5 columns and 1 row after 2 changes"""
    data = [('abc', 'abc', 'abc', 'abc', 'abc')]
    return spark.createDataFrame(data, ['a', 'b', 'g', 'd', 'd3'])


@fixture(name="source_df_5x5")
def fixture_source_df_5x5(spark):
    """Generating source dummy DataFrame with 5 columns and 5 row"""
    data = [('col_1@country_1', 'col_2@country_1', 'country_1', 'col_4@country_1', 'col_5@country_1'),
            ('col_1@country_2', 'col_2@country_2', 'country_2', 'col_4@country_2', 'col_5@country_2'),
            ('col_1@country_3', 'col_2@country_3', 'country_3', 'col_4@country_3', 'col_5@country_3'),
            ('col_1@country_4', 'col_2@country_4', 'country_4', 'col_4@country_4', 'col_5@country_4'),
            ('col_1@country_5', 'col_2@country_5', 'country_5', 'col_4@country_5', 'col_5@country_5')]
    return spark.createDataFrame(data, ['col_1', 'col_2', 'country', 'col_4', 'col_5'])


@fixture(name="expected_df_5x2")
def fixture_expected_df_5x2(spark):
    """Generating expected dummy DataFrame with 5 columns and 2 row"""
    data = [('col_1@country_2', 'col_2@country_2', 'country_2', 'col_4@country_2', 'col_5@country_2'),
            ('col_1@country_5', 'col_2@country_5', 'country_5', 'col_4@country_5', 'col_5@country_5')]
    return spark.createDataFrame(data, ['col_1', 'col_2', 'country', 'col_4', 'col_5'])


@fixture(name="expected_df_5x1_c")
def fixture_expected_df_5x1_c(spark):
    """Generating expected dummy DataFrame with 5 columns and 1 row for country_filter function"""
    data = [('col_1@country_2', 'col_2@country_2', 'country_2', 'col_4@country_2', 'col_5@country_2'), ]
    return spark.createDataFrame(data, ['col_1', 'col_2', 'country', 'col_4', 'col_5'])


def test_column_rename_all_3_changes_should_be_done(source_df_5x1, expected_df_5x1_3chg, logger):
    """Testing column_rename when all 3 changes should been done."""    
    actual_df = columns_rename(source_df_5x1, {'a': 'f', 'c': 'g', 'e': 'd3'}, logger)
    chispa.assert_df_equality(actual_df, expected_df_5x1_3chg)


def test_column_rename_1_column_not_in_df(source_df_5x1, expected_df_5x1_2chg, logger):
    """Testing column_rename when column which should be changed is not in dataframe"""
    actual_df = columns_rename(source_df_5x1, {'z': 'f', 'c': 'g', 'e': 'd3'}, logger)
    chispa.assert_df_equality(actual_df, expected_df_5x1_2chg)


def test_column_rename_with_empty_changes_dict(source_df_5x1, logger):
    """Testing column_rename with empty changes dictionary"""
    actual_df = columns_rename(source_df_5x1, {}, logger)
    chispa.assert_df_equality(actual_df, source_df_5x1)


def test_country_filter_leaves_specified_countries_in_results(source_df_5x5, expected_df_5x2, logger):
    """Testing country_filter when in Dataframe should stay only records from 2 specified countries"""
    actual_df = country_filter(source_df_5x5, 'country_2, country_5', logger)
    chispa.assert_df_equality(actual_df, expected_df_5x2)


def test_country_filter_country_not_in_df(source_df_5x5, expected_df_5x1_c, logger):
    """Testing country_filter when one of the country expected to be filtered not exist in the dataframe."""
    actual_df = country_filter(source_df_5x5, 'country_2, country_6', logger)
    chispa.assert_df_equality(actual_df, expected_df_5x1_c)


def test_country_filter_empty_country_str(source_df_5x5, logger):
    """Testing country_filter with empty country list - no change expected."""
    actual_df = country_filter(source_df_5x5, '', logger)
    chispa.assert_df_equality(actual_df, source_df_5x5)
