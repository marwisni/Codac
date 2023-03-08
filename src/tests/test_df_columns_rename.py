"""Test module of data_joiner.df.rename() method."""
from logging import Logger
import chispa
from pytest import fixture
from pyspark.sql import SparkSession, DataFrame
from data_joiner.df import DF


@fixture(name="source_df_5x1_df")
def fixture_source_df_5x1_df(spark: SparkSession, logger: Logger):
    """Generating source dummy DataFrame with 5 columns and 1 row"""
    data = [('abc', 'abc', 'abc', 'abc', 'abc')]
    return DF(spark,
              'source_df_5x1_df',
              None,
              logger,
              dataframe = spark.createDataFrame(data, ['a', 'b', 'c', 'd', 'e']))


def test_column_rename_all_3_changes_should_be_done(source_df_5x1_df: DF, expected_df_5x1_3chg: DataFrame):
    """Testing column_rename when all 3 changes should been done."""    
    source_df_5x1_df.columns_rename({'a': 'f', 'c': 'g', 'e': 'd3'})
    chispa.assert_df_equality(source_df_5x1_df.dataframe, expected_df_5x1_3chg)


def test_column_rename_1_column_not_in_df(source_df_5x1_df: DF, expected_df_5x1_2chg: DataFrame):
    """Testing column_rename when column which should be changed is not in dataframe"""
    source_df_5x1_df.columns_rename({'z': 'f', 'c': 'g', 'e': 'd3'})
    chispa.assert_df_equality(source_df_5x1_df.dataframe, expected_df_5x1_2chg)


def test_column_rename_with_empty_changes_dict(source_df_5x1_df: DF, source_df_5x1: DataFrame):
    """Testing column_rename with empty changes dictionary - no change expected."""    
    chispa.assert_df_equality(source_df_5x1_df.dataframe, source_df_5x1)
    source_df_5x1_df.columns_rename({})
    chispa.assert_df_equality(source_df_5x1_df.dataframe, source_df_5x1)
