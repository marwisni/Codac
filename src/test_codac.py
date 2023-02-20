"""Docstring for module, TODO Maybe add more tests?"""
import chispa
from pyspark.sql import SparkSession
from data_joiner.tools import column_rename, country_filter


spark = SparkSession.builder.appName('test_codac').getOrCreate()


def test_column_rename_happy_path():
    """Testing column_rename happy path"""
    data = [('abc', 'abc', 'abc', 'abc', 'abc')]
    source_df = spark.createDataFrame(data, ['a', 'b', 'c', 'd', 'e'])
    actual_df = column_rename(source_df, {
        'a': 'f',
        'c': 'g',
        'e': 'd3'
    }
    )
    expected_df = spark.createDataFrame(data, ['f', 'b', 'g', 'd', 'd3'])
    chispa.assert_df_equality(actual_df, expected_df)


def test_column_rename_column_not_in_df():
    """Testing column_rename when column which is wanted to change is not in dataframe"""
    data = [('abc', 'abc', 'abc', 'abc', 'abc')]
    source_df = spark.createDataFrame(data, ['a', 'b', 'c', 'd', 'e'])
    actual_df = column_rename(source_df, {
        'z': 'f',
        'c': 'g',
        'e': 'd3'
    }
    )
    expected_df = spark.createDataFrame(data, ['a', 'b', 'g', 'd', 'd3'])
    chispa.assert_df_equality(actual_df, expected_df)


def test_column_rename_with_empty_changes_dict():
    """Testing column_rename with empty changes dictionary"""
    data = [('abc', 'abc', 'abc', 'abc', 'abc')]
    source_df = spark.createDataFrame(data, ['a', 'b', 'c', 'd', 'e'])
    actual_df = column_rename(source_df, {})
    chispa.assert_df_equality(actual_df, source_df)


def test_country_filter_happy_path():
    """Testing country_filter happy path"""
    data = [('col_1@country_1', 'col_2@country_1', 'country_1', 'col_4@country_1', 'col_5@country_1'),
            ('col_1@country_2', 'col_2@country_2', 'country_2', 'col_4@country_2', 'col_5@country_2'),
            ('col_1@country_3', 'col_2@country_3', 'country_3', 'col_4@country_3', 'col_5@country_3'),
            ('col_1@country_4', 'col_2@country_4', 'country_4', 'col_4@country_4', 'col_5@country_4'),
            ('col_1@country_5', 'col_2@country_5', 'country_5', 'col_4@country_5', 'col_5@country_5')]
    source_df = spark.createDataFrame(data, ['col_1', 'col_2', 'country', 'col_4', 'col_5'])
    actual_df = country_filter(source_df, 'country_2, country_5')
    expected_data = [('col_1@country_2', 'col_2@country_2', 'country_2', 'col_4@country_2', 'col_5@country_2'),
                     ('col_1@country_5', 'col_2@country_5', 'country_5', 'col_4@country_5', 'col_5@country_5')]
    expected_df = spark.createDataFrame(expected_data, ['col_1', 'col_2', 'country', 'col_4', 'col_5'])
    chispa.assert_df_equality(actual_df, expected_df)


def test_country_filter_country_not_in_df():
    """Testing country_filter when one of the country expected to be filtered not exist in the dataframe."""
    data = [('col_1@country_1', 'col_2@country_1', 'country_1', 'col_4@country_1', 'col_5@country_1'),
            ('col_1@country_2', 'col_2@country_2', 'country_2', 'col_4@country_2', 'col_5@country_2'),
            ('col_1@country_3', 'col_2@country_3', 'country_3', 'col_4@country_3', 'col_5@country_3'),
            ('col_1@country_4', 'col_2@country_4', 'country_4', 'col_4@country_4', 'col_5@country_4'),
            ('col_1@country_5', 'col_2@country_5', 'country_5', 'col_4@country_5', 'col_5@country_5')]
    source_df = spark.createDataFrame(data, ['col_1', 'col_2', 'country', 'col_4', 'col_5'])
    actual_df = country_filter(source_df, 'country_2, country_6')
    expected_data = [('col_1@country_2', 'col_2@country_2', 'country_2', 'col_4@country_2', 'col_5@country_2'),]
    expected_df = spark.createDataFrame(expected_data, ['col_1', 'col_2', 'country', 'col_4', 'col_5'])
    chispa.assert_df_equality(actual_df, expected_df)


def test_country_filter_empty_country_str():
    """Testing country_filter happy path"""
    data = [('col_1@country_1', 'col_2@country_1', 'country_1', 'col_4@country_1', 'col_5@country_1'),
            ('col_1@country_2', 'col_2@country_2', 'country_2', 'col_4@country_2', 'col_5@country_2'),
            ('col_1@country_3', 'col_2@country_3', 'country_3', 'col_4@country_3', 'col_5@country_3'),
            ('col_1@country_4', 'col_2@country_4', 'country_4', 'col_4@country_4', 'col_5@country_4'),
            ('col_1@country_5', 'col_2@country_5', 'country_5', 'col_4@country_5', 'col_5@country_5')]
    source_df = spark.createDataFrame(data, ['col_1', 'col_2', 'country', 'col_4', 'col_5'])
    actual_df = country_filter(source_df, '')
    chispa.assert_df_equality(actual_df, source_df)
