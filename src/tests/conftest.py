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
