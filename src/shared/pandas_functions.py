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
        elif column.endswith("day") or str(dtypes[column]).lower() == "datetime64":
            column_dict[column] = "day"
        elif column.startswith("cnt") or str(dtypes[column]).lower() in (
            "int8",
            "int16",
            "int32",
            "int64",
            "uint8",
            "uint16",
            "uint32",
            "uint64",
        ):
            column_dict[column] = "integer"
        elif str(dtypes[column]).lower() in ("float32", "float64"):
            column_dict[column] = "float"
        elif str(dtypes[column]) == "boolean":
            column_dict[column] = "boolean"
        elif column in ["median", "average", "std_dev", "percentage"]:
            column_dict[column] = "double"
        else:
            column_dict[column] = "string"
    return column_dict
