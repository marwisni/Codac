"""Test configuration file."""
import logging
from pyspark.sql import SparkSession
from pytest import fixture


@fixture(name="spark", scope="session")
def fixture_spark():
    """Initialization of test spark session"""
    return SparkSession.builder.appName('test_codac').getOrCreate()


@fixture(name="logger", scope="session")
def fixture_logger():
    """Initialization of test logger"""
    return logging.getLogger()


@fixture(name="warn")
def fixture_warn():
    """Genarating logging.WARN constant for testing log_level_parser function"""
    return logging.WARN


@fixture(name="notset")
def fixture_notset():
    """Genarating logging.NOSET constant for testing log_level_parser function"""
    return logging.NOTSET


@fixture(name="source_df_5x1", scope="session")
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


@fixture(name="source_df_5x5", scope="session")
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
