"""Pandas functions used across different functions"""

import pandas


def get_column_datatypes(df: pandas.DataFrame):
    """helper for generating column type for dashboard API"""
    column_dict = {}
    for column in df.dtypes.index:
        column_dict[column] = {}
        if column.endswith("year"):
            column_dict[column]["type"] = "year"
        elif column.endswith("month"):
            column_dict[column]["type"] = "month"
        elif column.endswith("week"):
            column_dict[column]["type"] = "week"
        elif column.endswith("day") or str(df.dtypes[column]).lower() == "datetime64":
            column_dict[column]["type"] = "day"
        elif column.startswith("cnt") or str(df.dtypes[column]).lower() in (
            "int8",
            "int16",
            "int32",
            "int64",
            "uint8",
            "uint16",
            "uint32",
            "uint64",
        ):
            column_dict[column]["type"] = "integer"
        elif str(df.dtypes[column]).lower() in ("float32", "float64"):
            column_dict[column]["type"] = "float"
        elif str(df.dtypes[column]) == "boolean":
            column_dict[column]["type"] = "boolean"
        elif column in ["median", "average", "std_dev", "percentage"]:
            column_dict[column]["type"] = "double"
        else:
            column_dict[column]["type"] = "string"
        if not column.startswith("cnt"):
            column_dict[column]["distinct_values_count"] = df[column].nunique()
    return column_dict
