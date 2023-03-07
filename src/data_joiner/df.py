"""Module containing DF class"""
from logging import Logger
from pyspark.sql import SparkSession


class DF:
    """
    A class to represent a dataframe.

    ...

    Attributes
    ----------
    dataframe : DataFrame
        DataFrame object stored in DF instance.
    name : str
        Name of DataFrame. 
    logger : Logger
        Logger used to logging transformation processed with the DataFrame. 
    
    Methods
    -------
    columns_select(select: list(str)) -> None:
        Selects particular columns from the dataframe.
    """
    def __init__(self, spark_session: SparkSession, name: str, path: str, logger: Logger, header: bool = True) -> None:
        self.dataframe = spark_session.read.csv(path, header=header)
        self.name = name
        self.logger = logger
        logger.info(f"Created DF object '{self.name}' with data imported from {path}.")


def columns_select(self, select: list(str)) -> None:
    """Selecting particular columns from the dataframe.

    Args:        
        select (list(str)): List of columns names that should be selected.        
    """
    self.dataframe = self.dataframe.select(*select)
    self.logger.info(f"Only columns: {', '.join(select)} have been selected from dataframe '{self.name}'.")


def columns_drop(self, drop: list(str)) -> None:
    """Removing particular columns from the dataframe.

    Args:
       drop (list(str)): List of columns names that should be removed.
    """
    self.dataframe = self.dataframe.drop(*drop)
    self.logger.info(f"Removed {', '.join(drop)} columns from the dataframe '{self.name}'.")


def columns_rename(self, rename: dict) -> None:
    """Rename particular column names.
    
    Args:    
        rename (dict): Dictionary of changes that should happen in format "old_name": "new_name".        
    """
    changes_list = []
    for column in self.dataframe.columns:
        if column in rename:
            changes_list.append(f"{column} as {rename[column]}")
        else:
            changes_list.append(column)
    self.dataframe = self.dataframe.selectExpr(changes_list)
    self.logger.info(f"Columns' names {*list(rename.keys()),} have been changed in dataframe '{self.name}'.")


def country_filter(self, countries_str: str) -> None:
    """Filter data from dataframe including only particular countries.

    Args:        
        countries_str (str): Countries as comma separated string which should be included after filtering.        
    """
    if countries_str == '':
        self.logger.info('Data has not been filtered by country because empty string has been provided as parameter.')
    else:
        countries_list = [country.strip() for country in countries_str.split(',')]
        self.dataframe.filter(self.dataframe.country.isin(countries_list))
        self.logger.info(f"Data in the '{self.name}' dataframe has been filtered by country/countries: ({countries_str}).")


def dataframe_join(self, other : DF, on: list(str)) -> None:
    """Joining another dataframes.

    Args:
        other (Dataframe): Second dataframe which should be joined with current one.
        join (list(str)): List of columns names that dataframes should be joined on.        
    """
    self.dataframe = self.dataframe.join(other, on)
    self.logger.info(f"Dataframe '{other.name}' has been joined to dataframe '{self.name}' according to '{on}' column(s).")


def dataframe_save(self, path: str, header: bool = True) -> None:
    """Saving results to .csv file.

    Args:
        path (str): Path to the location where data should be saved.
        header (bool): Information if data should be saved with headers or not.        
    """
    self.dataframe.write.csv(path, header=header, mode='overwrite')
    self.logger.info(f"Output file has been saved successfully to {path} directory.")
    return
