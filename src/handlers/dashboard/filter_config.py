"""Reimplementation of dashboard's filtering logic for SQL construction"""
# ['age:lte:100', 'age:gte:100'],
# ['age:lte:100,age:gte:100']
_FILTER_MAP_TWO_PARAM = {
    # Text
    "strEq": "'%s' LIKE '%s'",
    "strContains": "'%s' LIKE '%%%s%%'",
    "strStartsWith": "'%s' LIKE '%s%%'",
    "strEndsWith": "'%s' LIKE '%%%s'",
    "matches": "'%s' ~ '%s'",
    "strEqCI": "'%s' ILIKE '%s'",
    "strContainsCI": "'%s' ILIKE '%%%s%%'",
    "strStartsWithCI": "'%s' ILIKE '%s%%'",
    "strEndsWithCI": "'%s' ILIKE '%%%s'",
    "matchesCI": "'%s' ~* '%s'",
    "strNotEq": "'%s' NOT LIKE '%s'",
    "strNotContains": "'%s' NOT LIKE '%%%s%%'",
    "strNotStartsWith": "'%s' NOT LIKE '%s%%'",
    "strNotEndsWith": "'%s' NOT LIKE '%%%s'",
    "notMatches": "'%s' !~ '%s'",
    "strNotEqCI": "'%s' NOT ILIKE '%s'",
    "strNotContainsCI": "'%s' NOT ILIKE '%%%s%%'",
    "strNotStartsWithCI": "'%s' NOT ILIKE '%s%%'",
    "strNotEndsWithCI": "'%s' NOT ILIKE '%%%s'",
    "notMatchesCI": "'%s' !~* '%s'",
    # Any
    "eq": "%s = %s",
    "ne": "%s != %s",
    "gt": "%s > %s",
    "gte": "%s >= %s",
    "lt": "%s < %s",
    "lte": "%s <= %s",
}

_FILTER_MAP_ONE_PARAM = {
    # Booleans
    "isTrue": "%s IS TRUE",
    "isNotTrue": "%s IS NOT TRUE",
    "isFalse": "%s IS FALSE",
    "isNotFalse": "%s IS NOT FALSE",
    # Any
    "isNull": "%s IS NULL",
    "isNotNull": "%s IS NOT NULL",
}


def _parse_filter_req(filter_req):
    if "," in filter_req:
        return " AND ".join(_parse_filter_req(x) for x in filter_req.split(","))
    filter_req_split = filter_req.split(":")
    if filter_req_split[1] in _FILTER_MAP_ONE_PARAM.keys():
        return _FILTER_MAP_ONE_PARAM[filter_req_split[1]] % filter_req_split[0]
    return _FILTER_MAP_TWO_PARAM[filter_req_split[1]] % (
        filter_req_split[0],
        filter_req_split[2],
    )


def get_filter_string(filter_array):
    filter_string = ""
    if len(filter_array) > 1:
        return " OR ".join(_parse_filter_req(x) for x in filter_array)
    elif len(filter_array) == 1:
        return _parse_filter_req(filter_array[0])
    return ""
