import pathlib

import jinja2
import pytest


@pytest.mark.parametrize(
    "filter_configs,expected",
    [
        # Checking individual conversions
        (
            ["col:strEq:str"],
            """
        (
            CAST("col" AS VARCHAR) LIKE 'str'
                    )""",
        ),
        (
            ["col:strContains:str"],
            """
        (
            CAST("col" AS VARCHAR) LIKE '%str%'
                    )""",
        ),
        (
            ["col:strStartsWith:str"],
            """
        (
            CAST("col" AS VARCHAR) LIKE 'str%'
                    )""",
        ),
        (
            ["col:strEndsWith:str"],
            """
        (
            CAST("col" AS VARCHAR) LIKE '%str'
                    )""",
        ),
        (
            ["col:matches:str"],
            """
        (
            regexp_like(CAST("col" AS VARCHAR), 'str')
                    )""",
        ),
        (
            ["col:strEqCI:str"],
            """
        (
            regexp_like(CAST("col" AS VARCHAR), '(?i)^str$')
                    )""",
        ),
        (
            ["col:strContainsCI:str"],
            """
        (
            regexp_like(CAST("col" AS VARCHAR), '(?i)str')
                    )""",
        ),
        (
            ["col:strStartsWithCI:str"],
            """
        (
            regexp_like(CAST("col" AS VARCHAR), '(?i)^str')
                    )""",
        ),
        (
            ["col:strEndsWithCI:str"],
            """
        (
            regexp_like(CAST("col" AS VARCHAR), '(?i)str$')
                    )""",
        ),
        (
            ["col:matchesCI:str"],
            """
        (
            regexp_like(CAST("col" AS VARCHAR), '(?i)str')
                    )""",
        ),
        (
            ["col:strNotEq:str"],
            """
        (
            CAST("col" AS VARCHAR) NOT LIKE 'str'
                    )""",
        ),
        (
            ["col:strNotContains:str"],
            """
        (
            CAST("col" AS VARCHAR) NOT LIKE '%str%'
                    )""",
        ),
        (
            ["col:strNotStartsWith:str"],
            """
        (
            CAST("col" AS VARCHAR) NOT LIKE 'str%'
                    )""",
        ),
        (
            ["col:strNotEndsWith:str"],
            """
        (
            CAST("col" AS VARCHAR) NOT LIKE '%str'
                    )""",
        ),
        (
            ["col:notMatches:str"],
            """
        (
            NOT regexp_like(CAST("col" AS VARCHAR), 'str')
                    )""",
        ),
        (
            ["col:strNotEqCI:str"],
            """
        (
            NOT regexp_like(CAST("col" AS VARCHAR), '(?i)^str$')
                    )""",
        ),
        (
            ["col:strNotContainsCI:str"],
            """
        (
            NOT regexp_like(CAST("col" AS VARCHAR), '(?i)str')
                    )""",
        ),
        (
            ["col:strNotStartsWithCI:str"],
            """
        (
            NOT regexp_like(CAST("col" AS VARCHAR), '(?i)^str')
                    )""",
        ),
        (
            ["col:strNotEndsWithCI:str"],
            """
        (
            NOT regexp_like(CAST("col" AS VARCHAR), '(?i)str$')
                    )""",
        ),
        (
            ["col:notMatchesCI:str"],
            """
        (
            NOT regexp_like(CAST("col" AS VARCHAR), '(?i)str')
                    )""",
        ),
        (
            ["col:eq:10.2"],
            """
        (
            "col" = 10.2
                    )""",
        ),
        (
            ["col:ne:10.2"],
            """
        (
            "col" != 10.2
                    )""",
        ),
        (
            ["col:gt:10.2"],
            """
        (
            "col" > 10.2
                    )""",
        ),
        (
            ["col:gte:10.2"],
            """
        (
            "col" >= 10.2
                    )""",
        ),
        (
            ["col:lt:10.2"],
            """
        (
            "col" < 10.2
                    )""",
        ),
        (
            ["col:lte:10.2"],
            """
        (
            "col" <= 10.2
                    )""",
        ),
        (
            ["col:isTrue"],
            """
        (
            "col" IS TRUE
                    )""",
        ),
        (
            ["col:isNotTrue"],
            """
        (
            "col" IS NOT TRUE
                    )""",
        ),
        (
            ["col:isFalse"],
            """
        (
            "col" IS FALSE
                    )""",
        ),
        (
            ["col:isNotFalse"],
            """
        (
            "col" IS NOT FALSE
                    )""",
        ),
        (
            ["col:isNull"],
            """
        (
            "col" IS NULL
                    )""",
        ),
        (
            ["col:isNotNull"],
            """
        (
            "col" IS NOT NULL
                    )""",
        ),
        (
            ["column:sameDay:1900-01-01"],
            """
        (
            from_iso8601_timestamp("column") = """
            """date_trunc('day',from_iso8601_timestamp('1900-01-01'))
                    )""",
        ),
        (
            ["column:sameWeek:1900-01-01"],
            """
        (
            date_trunc('week',from_iso8601_timestamp("column")) = """
            """date_trunc('week',from_iso8601_timestamp('1900-01-01'))
                    )""",
        ),
        (
            ["column:sameMonth:1900-01-01"],
            """
        (
            date_trunc('month',from_iso8601_timestamp("column")) = """
            """date_trunc('month',from_iso8601_timestamp('1900-01-01'))
                    )""",
        ),
        (
            ["column:sameYear:1900-01-01"],
            """
        (
            date_trunc('year',from_iso8601_timestamp("column")) = """
            """date_trunc('year',from_iso8601_timestamp('1900-01-01'))
                    )""",
        ),
        (
            ["column:sameDayOrBefore:1900-01-01"],
            """
        (
            from_iso8601_timestamp("column") <= """
            """date_trunc('day',from_iso8601_timestamp('1900-01-01'))
                    )""",
        ),
        (
            ["column:sameWeekOrBefore:1900-01-01"],
            """
        (
            date_trunc('week',from_iso8601_timestamp("column")) <= """
            """date_trunc('week',from_iso8601_timestamp('1900-01-01'))
                    )""",
        ),
        (
            ["column:sameMonthOrBefore:1900-01-01"],
            (
                """
        (
            date_trunc('month',from_iso8601_timestamp("column")) <= """
                """date_trunc('month',from_iso8601_timestamp('1900-01-01'))
                    )"""
            ),
        ),
        (
            ["column:sameYearOrBefore:1900-01-01"],
            """
        (
            date_trunc('year',from_iso8601_timestamp("column")) <= """
            """date_trunc('year',from_iso8601_timestamp('1900-01-01'))
                    )""",
        ),
        (
            ["column:sameDayOrAfter:1900-01-01"],
            """
        (
            from_iso8601_timestamp("column") >= """
            """date_trunc('day',from_iso8601_timestamp('1900-01-01'))
                    )""",
        ),
        (
            ["column:sameWeekOrAfter:1900-01-01"],
            """
        (
            date_trunc('week',from_iso8601_timestamp("column")) >= """
            """date_trunc('week',from_iso8601_timestamp('1900-01-01'))
                    )""",
        ),
        (
            ["column:sameMonthOrAfter:1900-01-01"],
            (
                """
        (
            date_trunc('month',from_iso8601_timestamp("column")) >= """
                """date_trunc('month',from_iso8601_timestamp('1900-01-01'))
                    )"""
            ),
        ),
        (
            ["column:sameYearOrAfter:1900-01-01"],
            """
        (
            date_trunc('year',from_iso8601_timestamp("column")) >= """
            """date_trunc('year',from_iso8601_timestamp('1900-01-01'))
                    )""",
        ),
        (
            ["column:beforeDay:1900-01-01"],
            """
        (
            from_iso8601_timestamp("column") < """
            """date_trunc('day',from_iso8601_timestamp('1900-01-01'))
                    )""",
        ),
        (
            ["column:beforeWeek:1900-01-01"],
            """
        (
            date_trunc('week',from_iso8601_timestamp("column")) < """
            """date_trunc('week',from_iso8601_timestamp('1900-01-01'))
                    )""",
        ),
        (
            ["column:beforeMonth:1900-01-01"],
            """
        (
            date_trunc('month',from_iso8601_timestamp("column")) < """
            """date_trunc('month',from_iso8601_timestamp('1900-01-01'))
                    )""",
        ),
        (
            ["column:beforeYear:1900-01-01"],
            """
        (
            date_trunc('year',from_iso8601_timestamp("column")) < """
            """date_trunc('year',from_iso8601_timestamp('1900-01-01'))
                    )""",
        ),
        (
            ["column:afterDay:1900-01-01"],
            """
        (
            from_iso8601_timestamp("column") > """
            """date_trunc('day',from_iso8601_timestamp('1900-01-01'))
                    )""",
        ),
        (
            ["column:afterWeek:1900-01-01"],
            """
        (
            date_trunc('week',from_iso8601_timestamp("column")) > """
            """date_trunc('week',from_iso8601_timestamp('1900-01-01'))
                    )""",
        ),
        (
            ["column:afterMonth:1900-01-01"],
            """
        (
            date_trunc('month',from_iso8601_timestamp("column")) > """
            """date_trunc('month',from_iso8601_timestamp('1900-01-01'))
                    )""",
        ),
        (
            ["column:afterYear:1900-01-01"],
            """
        (
            date_trunc('year',from_iso8601_timestamp("column")) > """
            """date_trunc('year',from_iso8601_timestamp('1900-01-01'))
                    )""",
        ),
        # Checking compound statements
        (
            ["col:strEq:str", "col:strEqCI:str"],
            """
        (
            CAST("col" AS VARCHAR) LIKE 'str'
                    )
        OR (
            regexp_like(CAST("col" AS VARCHAR), '(?i)^str$')
                    )""",
        ),
        (
            ["col:strEq:str,col:strEqCI:str"],
            """
        (
            CAST("col" AS VARCHAR) LIKE 'str'
            AND regexp_like(CAST("col" AS VARCHAR), '(?i)^str$')
                    )""",
        ),
        (
            ["col:strEq:str", "col:strEq:str,col:strEqCI:str"],
            """
        (
            CAST("col" AS VARCHAR) LIKE 'str'
                    )
        OR (
            CAST("col" AS VARCHAR) LIKE 'str'
            AND regexp_like(CAST("col" AS VARCHAR), '(?i)^str$')
                    )""",
        ),
    ],
)
def test_filter_string(filter_configs, expected):
    inline_configs = []
    for filter_config in filter_configs:
        subconfigs = filter_config.split(",")
        inline_config = []
        for subconfig in subconfigs:
            subconfig = subconfig.split(":")
            config = {"data": subconfig[0], "filter_type": subconfig[1]}
            if len(subconfig) == 3:
                config["bound"] = subconfig[2]
            inline_config.append(config)
        inline_configs.append(inline_config)
    with open(pathlib.Path(__file__).parent / "test_filter_inline.sql.jinja") as file:
        template = file.read()
        loader = jinja2.FileSystemLoader(
            pathlib.Path(__file__).parent / "../../src/dashboard/get_chart_data/templates/"
        )
        env = jinja2.Environment(loader=loader).from_string(template)
        query = env.render(
            inline_configs=inline_configs,
        )
    assert query == expected
