"""Reimplementation of dashboard's filtering logic for SQL construction"""

_FILTER_MAP_TWO_PARAM = {
    # Text
    "strEq": "%s LIKE '%s'",
    "strContains": "%s LIKE '%%%s%%'",
    "strStartsWith": "%s LIKE '%s%%'",
    "strEndsWith": "%s LIKE '%%%s'",
    "matches": "%s ~ '%s'",
    "strEqCI": "%s ILIKE '%s'",
    "strContainsCI": "%s ILIKE '%%%s%%'",
    "strStartsWithCI": "%s ILIKE '%s%%'",
    "strEndsWithCI": "%s ILIKE '%%%s'",
    "matchesCI": "%s ~* '%s'",
    "strNotEq": "%s NOT LIKE '%s'",
    "strNotContains": "%s NOT LIKE '%%%s%%'",
    "strNotStartsWith": "%s NOT LIKE '%s%%'",
    "strNotEndsWith": "%s NOT LIKE '%%%s'",
    "notMatches": "%s !~ '%s'",
    "strNotEqCI": "%s NOT ILIKE '%s'",
    "strNotContainsCI": "%s NOT ILIKE '%%%s%%'",
    "strNotStartsWithCI": "%s NOT ILIKE '%s%%'",
    "strNotEndsWithCI": "%s NOT ILIKE '%%%s'",
    "notMatchesCI": "%s !~* '%s'",
    # numeric
    "eq": "%s = %s",
    "ne": "%s != %s",
    "gt": "%s > %s",
    "gte": "%s >= %s",
    "lt": "%s < %s",
    "lte": "%s <= %s",
    # dates
    "sameDay": "from_iso8601_timestamp(%s) = date_trunc('day',from_iso8601_timestamp('%s'))",
    "sameWeek": "date_trunc('week',from_iso8601_timestamp(%s)) = "
    "date_trunc('week',from_iso8601_timestamp('%s'))",
    "sameMonth": "date_trunc('month',from_iso8601_timestamp(%s)) = "
    "date_trunc('month',from_iso8601_timestamp('%s'))",
    "sameYear": "date_trunc('year',from_iso8601_timestamp(%s)) = "
    "date_trunc('year',from_iso8601_timestamp('%s'))",
    "sameDayOrBefore": "from_iso8601_timestamp(%s) <= "
    "date_trunc('day',from_iso8601_timestamp('%s'))",
    "sameWeekOrBefore": "date_trunc('week',from_iso8601_timestamp(%s)) <= "
    "date_trunc('week',from_iso8601_timestamp('%s'))",
    "sameMonthOrBefore": (
        "date_trunc('month',from_iso8601_timestamp(%s)) <= "
        "date_trunc('month',from_iso8601_timestamp('%s'))"
    ),
    "sameYearOrBefore": "date_trunc('year',from_iso8601_timestamp(%s)) <= "
    "date_trunc('year',from_iso8601_timestamp('%s'))",
    "sameDayOrAfter": "from_iso8601_timestamp(%s) >= "
    "date_trunc('day',from_iso8601_timestamp('%s'))",
    "sameWeekOrAfter": (
        "date_trunc('week',from_iso8601_timestamp(%s)) "
        ">= date_trunc('week',from_iso8601_timestamp('%s'))"
    ),
    "sameMonthOrAfter": (
        "date_trunc('month',from_iso8601_timestamp(%s)) >= "
        "date_trunc('month',from_iso8601_timestamp('%s'))"
    ),
    "sameYearOrAfter": "date_trunc('year',from_iso8601_timestamp(%s)) >= "
    "date_trunc('year',from_iso8601_timestamp('%s'))",
    "beforeDay": "from_iso8601_timestamp(%s) < " "date_trunc('day',from_iso8601_timestamp('%s'))",
    "beforeWeek": "date_trunc('week',from_iso8601_timestamp(%s)) < "
    "date_trunc('week',from_iso8601_timestamp('%s'))",
    "beforeMonth": "date_trunc('month',from_iso8601_timestamp(%s)) < "
    "date_trunc('month',from_iso8601_timestamp('%s'))",
    "beforeYear": "date_trunc('year',from_iso8601_timestamp(%s)) < "
    "date_trunc('year',from_iso8601_timestamp('%s'))",
    "afterDay": "from_iso8601_timestamp(%s) > " "date_trunc('day',from_iso8601_timestamp('%s'))",
    "afterWeek": "date_trunc('week',from_iso8601_timestamp(%s)) > "
    "date_trunc('week',from_iso8601_timestamp('%s'))",
    "afterMonth": "date_trunc('month',from_iso8601_timestamp(%s)) > "
    "date_trunc('month',from_iso8601_timestamp('%s'))",
    "afterYear": "date_trunc('year',from_iso8601_timestamp(%s)) > "
    "date_trunc('year',from_iso8601_timestamp('%s'))",
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
    if filter_req_split[1] in _FILTER_MAP_ONE_PARAM:
        return _FILTER_MAP_ONE_PARAM[filter_req_split[1]] % filter_req_split[0]
    return _FILTER_MAP_TWO_PARAM[filter_req_split[1]] % (
        filter_req_split[0],
        filter_req_split[2],
    )


def get_filter_string(filter_array):
    if len(filter_array) > 1:
        return " OR ".join(_parse_filter_req(x) for x in filter_array)
    elif len(filter_array) == 1:
        return _parse_filter_req(filter_array[0])
    return ""
