"""Test module of data_joiner.df.rename() method."""
from logging import Logger
import chispa
from pytest import fixture
from pyspark.sql import SparkSession, DataFrame
from data_joiner.df import DF


@fixture(name="source_df_5x5_df")
def fixture_source_df_5x5_df(spark: SparkSession, logger: Logger):
    """Generating source dummy DataFrame with 5 columns and 5 row"""
    data = [('col_1@country_1', 'col_2@country_1', 'country_1', 'col_4@country_1', 'col_5@country_1'),
            ('col_1@country_2', 'col_2@country_2', 'country_2', 'col_4@country_2', 'col_5@country_2'),
            ('col_1@country_3', 'col_2@country_3', 'country_3', 'col_4@country_3', 'col_5@country_3'),
            ('col_1@country_4', 'col_2@country_4', 'country_4', 'col_4@country_4', 'col_5@country_4'),
            ('col_1@country_5', 'col_2@country_5', 'country_5', 'col_4@country_5', 'col_5@country_5')]
    return DF(spark,
              'source_df_5x5_df',
              None,
              logger,
              dataframe = spark.createDataFrame(data, ['col_1', 'col_2', 'country', 'col_4', 'col_5']))


def test_country_filter_leaves_specified_countries_in_results(source_df_5x5_df: DF, expected_df_5x2: DataFrame):
    """Testing country_filter when in Dataframe should stay only records from 2 specified countries"""
    source_df_5x5_df.country_filter('country_2, country_5')
    chispa.assert_df_equality(source_df_5x5_df.dataframe, expected_df_5x2)


def test_country_filter_country_not_in_df(source_df_5x5_df: DF, expected_df_5x1_c: DataFrame):
    """Testing country_filter when one of the country expected to be filtered not exist in the dataframe."""
    source_df_5x5_df.country_filter('country_2, country_6')
    chispa.assert_df_equality(source_df_5x5_df.dataframe, expected_df_5x1_c)


def test_country_filter_empty_country_str(source_df_5x5_df: DF, source_df_5x5: DataFrame):
    """Testing country_filter with empty country list - no change expected."""
    chispa.assert_df_equality(source_df_5x5_df.dataframe, source_df_5x5)
    source_df_5x5_df.country_filter('')
    chispa.assert_df_equality(source_df_5x5_df.dataframe, source_df_5x5)
