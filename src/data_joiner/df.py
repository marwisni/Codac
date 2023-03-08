"""Module containing DF class"""
from logging import Logger
from typing import List, Dict
from pyspark.sql import SparkSession, DataFrame


class DF:
    """
    A class to represent a dataframe.
    
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

    def __init__(self,
                 spark_session: SparkSession,
                 name: str,
                 path: str,
                 logger: Logger,
                 header: bool = True,
                 dataframe: DataFrame = None) -> None:
        if path:
            self.dataframe = spark_session.read.csv(path, header=header)
            self.name = name
            self.logger = logger
            logger.info(
                f"Created DF object '{self.name}' with data imported from {path}.")
        else:
            self.dataframe = dataframe
            self.name = name
            self.logger = logger
            logger.info(f"Created DF object '{self.name}'"
                        " with provided DataFrame object because path to .csv file has not been provided.")


    def columns_select(self, select: List[str]) -> None:
        """Selecting particular columns from the dataframe.

        Args:
        -----
            select (list[str]): List of columns names that should be selected.        
        """
        self.dataframe = self.dataframe.select(*select)
        self.logger.info(
            f"Only columns: {', '.join(select)} have been selected from dataframe '{self.name}'.")


    def columns_drop(self, drop: List[str]) -> None:
        """Removing particular columns from the dataframe.

        Args:
        -----
        drop (list[str]): List of columns names that should be removed.
        """
        self.dataframe = self.dataframe.drop(*drop)
        self.logger.info(
            f"Removed {', '.join(drop)} columns from the dataframe '{self.name}'.")


    def columns_rename(self, rename: Dict[str, str]) -> None:
        """Rename particular column names.

        Args:
        -----
            rename (dict): Dictionary of changes that should happen in format "old_name": "new_name".        
        """
        changes_list = []
        for column in self.dataframe.columns:
            if column in rename:
                changes_list.append(f"{column} as {rename[column]}")
            else:
                changes_list.append(column)
        self.dataframe = self.dataframe.selectExpr(changes_list)
        self.logger.info(
            f"Columns' names {*list(rename.keys()),} have been changed in dataframe '{self.name}'.")


    def country_filter(self, countries_str: str) -> None:
        """Filter data from dataframe including only particular countries.

        Args:
        -----
            countries_str (str): Countries which should be included after filtering as comma separated string.        
        """
        if countries_str == '':
            self.logger.info(
                'Data has not been filtered by country because empty string has been provided as parameter.')
        else:
            countries_list = [country.strip()
                            for country in countries_str.split(',')]
            self.dataframe = self.dataframe.filter(self.dataframe.country.isin(countries_list))
            self.logger.info(
                f"Data in the '{self.name}' dataframe has been filtered by country/countries: ({countries_str}).")


    def join(self, other: 'DF', join_on: List[str]) -> None:
        """Joining another dataframes.

        Args:
        -----
            other (Dataframe): Second dataframe which should be joined with current one.
            on (list[str]): List of columns names that dataframes should be joined on.        
        """
        self.dataframe = self.dataframe.join(other.dataframe, join_on)
        self.logger.info(
            f"Dataframe '{other.name}' has been joined to dataframe '{self.name}' according to '{join_on}' column(s).")


    def save(self, path: str, header: bool = True) -> None:
        """Saving results to .csv file.

        Args:
        -----
            path (str): Path to the location where data should be saved.
            header (bool): Information if data should be saved with headers or not.        
        """
        self.dataframe.write.csv(path, header=header, mode='overwrite')
        self.logger.info(
            f"Output file has been saved successfully to {path} directory.")
        return
