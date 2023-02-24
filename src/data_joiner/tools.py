"""
Module including auxiliary functions used in main module.

Functions:
    - column_rename(DataFrame, dict) -> DataFrame
    - country_filter(DataFrame, str) -> DataFrame
"""

__docformat__ = 'restructuredtext'

def column_rename(dataframe, change: dict):
    """Rename particular column names.
    
    Args:    
        - dataframe (DataFrame): DataFrame for which columns names should be changed.        
        - change (dict): Dictionary of changes that should happen in format "old_name": "new_name".

    Returns:
        DataFrame: Dataframe with renamed columns.
    """
    changes_list = []
    for column in dataframe.columns:
        if column in change.keys():
            changes_list.append(f"{column} as {change[column]}")
        else:
            changes_list.append(column)
    return dataframe.selectExpr(changes_list)


def country_filter(dataframe, countries_str: str):
    """Filter data from dataframe including only particular countries.

    Args:
        dataframe (DataFrame): Input DataFrame which should be filtered by country.
        countries_str (str): Countries as comma separated string which should be included after filtering.

    Returns:
        DataFrame: Dataframe with data only from countries included in the countries_str.
    """
    if countries_str == '':
        return dataframe
    countries = [country.strip() for country in countries_str.split(',')]
    return dataframe.filter(dataframe.country.isin(countries))
