"""Docstring for module, TODO Maybe add more tests?"""
import chispa
from pyspark.sql import SparkSession
from data_joiner.tools import column_rename, country_filter


spark = SparkSession.builder.appName('test_codac').getOrCreate()


def test_column_rename():
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


def test_country_filter():
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
