"""Pandas functions used across different functions"""

import pandas


def get_column_datatypes(dtypes: pandas.DataFrame):
    """helper for generating column type for dashboard API"""
    column_dict = {}
    for column in dtypes.index:
        if column.endswith("year"):
            column_dict[column] = "year"
        elif column.endswith("month"):
            column_dict[column] = "month"
        elif column.endswith("week"):
            column_dict[column] = "week"
        elif column.endswith("day") or str(dtypes[column]) == "datetime64":
            column_dict[column] = "day"
        elif column.startswith("cnt") or str(dtypes[column]) in (
            "Int8",
            "Int16",
            "Int32",
            "Int64",
            "UInt8",
            "UInt16",
            "UInt32",
            "UInt64",
        ):
            column_dict[column] = "integer"
        elif str(dtypes[column]) in ("Float32", "Float64"):
            column_dict[column] = "float"
        elif str(dtypes[column]) == "boolean":
            column_dict[column] = "boolean"
        else:
            column_dict[column] = "string"
    return column_dict
