"""Test configuration file."""
import logging
from pyspark.sql import SparkSession
from pytest import fixture

__docformat__ = 'restructuredtext'

@fixture(name="spark", scope="session")
def fixture_spark() -> SparkSession:
    """Initialization of test spark session"""
    return SparkSession.builder.appName('test_codac').getOrCreate()


@fixture(name="logger", scope="session")
def fixture_logger() -> logging.Logger:
    """Initialization of test logger"""
    return logging.getLogger()


@fixture(name="warn")
def fixture_warn() -> int:
    """Genarating logging.WARN constant for testing log_level_parser function"""
    return logging.WARN


@fixture(name="notset")
def fixture_notset() -> int:
    """Genarating logging.NOSET constant for testing log_level_parser function"""
    return logging.NOTSET
