def column_rename(dataframe, change: dict):
    # TODO Maybe should iterate by change not by colmns?
    changes_list = []
    for column in dataframe.columns:
        if column in change.keys():
            changes_list.append(f"{column} as {change[column]}")
        else:
            changes_list.append(column)
    return dataframe.selectExpr(changes_list)


def country_filter(dataframe, countries_str: str):
    if countries_str == '':
        return dataframe
    else:
        countries = [country.strip() for country in countries_str.split(',')]
        return dataframe.filter(dataframe.country.isin(countries))
    