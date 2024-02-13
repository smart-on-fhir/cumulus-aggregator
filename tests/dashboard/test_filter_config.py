import pytest

from src.handlers.dashboard.filter_config import get_filter_string


@pytest.mark.parametrize(
    "input_str,output_str",
    [
        # Checking individual conversions
        (["col:strEq:str"], "col LIKE 'str'"),
        (["col:strContains:str"], "col LIKE '%str%'"),
        (["col:strStartsWith:str"], "col LIKE 'str%'"),
        (["col:strEndsWith:str"], "col LIKE '%str'"),
        (["col:matches:str"], "col ~ 'str'"),
        (["col:strEqCI:str"], "col ILIKE 'str'"),
        (["col:strContainsCI:str"], "col ILIKE '%str%'"),
        (["col:strStartsWithCI:str"], "col ILIKE 'str%'"),
        (["col:strEndsWithCI:str"], "col ILIKE '%str'"),
        (["col:matchesCI:str"], "col ~* 'str'"),
        (["col:strNotEq:str"], "col NOT LIKE 'str'"),
        (["col:strNotContains:str"], "col NOT LIKE '%str%'"),
        (["col:strNotStartsWith:str"], "col NOT LIKE 'str%'"),
        (["col:strNotEndsWith:str"], "col NOT LIKE '%str'"),
        (["col:notMatches:str"], "col !~ 'str'"),
        (["col:strNotEqCI:str"], "col NOT ILIKE 'str'"),
        (["col:strNotContainsCI:str"], "col NOT ILIKE '%str%'"),
        (["col:strNotStartsWithCI:str"], "col NOT ILIKE 'str%'"),
        (["col:strNotEndsWithCI:str"], "col NOT ILIKE '%str'"),
        (["col:notMatchesCI:str"], "col !~* 'str'"),
        (["col:eq:10.2"], "col = 10.2"),
        (["col:ne:10.2"], "col != 10.2"),
        (["col:gt:10.2"], "col > 10.2"),
        (["col:gte:10.2"], "col >= 10.2"),
        (["col:lt:10.2"], "col < 10.2"),
        (["col:lte:10.2"], "col <= 10.2"),
        (["col:isTrue"], "col IS TRUE"),
        (["col:isNotTrue"], "col IS NOT TRUE"),
        (["col:isFalse"], "col IS FALSE"),
        (["col:isNotFalse"], "col IS NOT FALSE"),
        (["col:isNull"], "col IS NULL"),
        (["col:isNotNull"], "col IS NOT NULL"),
        (["col:strEq:str"], "col LIKE 'str'"),
        (
            ["column:sameDay:1900-01-01"],
            "DATE(column) = date_trunc('day',DATE('1900-01-01'))",
        ),
        (
            ["column:sameWeek:1900-01-01"],
            "date_trunc('week',DATE(column)) = date_trunc('week',DATE('1900-01-01'))",
        ),
        (
            ["column:sameMonth:1900-01-01"],
            "date_trunc('month',DATE(column)) = date_trunc('month',DATE('1900-01-01'))",
        ),
        (
            ["column:sameYear:1900-01-01"],
            "date_trunc('year',DATE(column)) = date_trunc('year',DATE('1900-01-01'))",
        ),
        (
            ["column:sameDayOrBefore:1900-01-01"],
            "DATE(column) <= date_trunc('day',DATE('1900-01-01'))",
        ),
        (
            ["column:sameWeekOrBefore:1900-01-01"],
            "date_trunc('week',DATE(column)) <= date_trunc('week',DATE('1900-01-01'))",
        ),
        (
            ["column:sameMonthOrBefore:1900-01-01"],
            (
                "date_trunc('month',DATE(column)) <= "
                "date_trunc('month',DATE('1900-01-01'))"
            ),
        ),
        (
            ["column:sameYearOrBefore:1900-01-01"],
            "date_trunc('year',DATE(column)) <= date_trunc('year',DATE('1900-01-01'))",
        ),
        (
            ["column:sameDayOrAfter:1900-01-01"],
            "DATE(column) >= date_trunc('day',DATE('1900-01-01'))",
        ),
        (
            ["column:sameWeekOrAfter:1900-01-01"],
            "date_trunc('week',DATE(column)) >= date_trunc('week',DATE('1900-01-01'))",
        ),
        (
            ["column:sameMonthOrAfter:1900-01-01"],
            (
                "date_trunc('month',DATE(column)) >= "
                "date_trunc('month',DATE('1900-01-01'))"
            ),
        ),
        (
            ["column:sameYearOrAfter:1900-01-01"],
            "date_trunc('year',DATE(column)) >= date_trunc('year',DATE('1900-01-01'))",
        ),
        (
            ["column:beforeDay:1900-01-01"],
            "DATE(column) < date_trunc('day',DATE('1900-01-01'))",
        ),
        (
            ["column:beforeWeek:1900-01-01"],
            "date_trunc('week',DATE(column)) < date_trunc('week',DATE('1900-01-01'))",
        ),
        (
            ["column:beforeMonth:1900-01-01"],
            "date_trunc('month',DATE(column)) < date_trunc('month',DATE('1900-01-01'))",
        ),
        (
            ["column:beforeYear:1900-01-01"],
            "date_trunc('year',DATE(column)) < date_trunc('year',DATE('1900-01-01'))",
        ),
        (
            ["column:afterDay:1900-01-01"],
            "DATE(column) > date_trunc('day',DATE('1900-01-01'))",
        ),
        (
            ["column:afterWeek:1900-01-01"],
            "date_trunc('week',DATE(column)) > date_trunc('week',DATE('1900-01-01'))",
        ),
        (
            ["column:afterMonth:1900-01-01"],
            "date_trunc('month',DATE(column)) > date_trunc('month',DATE('1900-01-01'))",
        ),
        (
            ["column:afterYear:1900-01-01"],
            "date_trunc('year',DATE(column)) > date_trunc('year',DATE('1900-01-01'))",
        ),
        # Checking compound statements
        (
            ["col:strEq:str", "col:strEqCI:str"],
            "col LIKE 'str' OR col ILIKE 'str'",
        ),
        (
            ["col:strEq:str,col:strEqCI:str"],
            "col LIKE 'str' AND col ILIKE 'str'",
        ),
        (
            ["col:strEq:str", "col:strEq:str,col:strEqCI:str"],
            "col LIKE 'str' OR col LIKE 'str' AND col ILIKE 'str'",
        ),
    ],
)
def test_filter_string(input_str, output_str):
    assert get_filter_string(input_str) == output_str
