import pytest

from src.handlers.dashboard.filter_config import get_filter_string


@pytest.mark.parametrize(
    "input,output",
    [
        # Checking individual conversions
        (["str1:strEq:str2"], "'str1' LIKE 'str2'"),
        (["str1:strContains:str2"], "'str1' LIKE '%str2%'"),
        (["str1:strStartsWith:str2"], "'str1' LIKE 'str2%'"),
        (["str1:strEndsWith:str2"], "'str1' LIKE '%str2'"),
        (["str1:matches:str2"], "'str1' ~ 'str2'"),
        (["str1:strEqCI:str2"], "'str1' ILIKE 'str2'"),
        (["str1:strContainsCI:str2"], "'str1' ILIKE '%str2%'"),
        (["str1:strStartsWithCI:str2"], "'str1' ILIKE 'str2%'"),
        (["str1:strEndsWithCI:str2"], "'str1' ILIKE '%str2'"),
        (["str1:matchesCI:str2"], "'str1' ~* 'str2'"),
        (["str1:strNotEq:str2"], "'str1' NOT LIKE 'str2'"),
        (["str1:strNotContains:str2"], "'str1' NOT LIKE '%str2%'"),
        (["str1:strNotStartsWith:str2"], "'str1' NOT LIKE 'str2%'"),
        (["str1:strNotEndsWith:str2"], "'str1' NOT LIKE '%str2'"),
        (["str1:notMatches:str2"], "'str1' !~ 'str2'"),
        (["str1:strNotEqCI:str2"], "'str1' NOT ILIKE 'str2'"),
        (["str1:strNotContainsCI:str2"], "'str1' NOT ILIKE '%str2%'"),
        (["str1:strNotStartsWithCI:str2"], "'str1' NOT ILIKE 'str2%'"),
        (["str1:strNotEndsWithCI:str2"], "'str1' NOT ILIKE '%str2'"),
        (["str1:notMatchesCI:str2"], "'str1' !~* 'str2'"),
        (["10.1:eq:10.2"], "10.1 = 10.2"),
        (["10.1:ne:10.2"], "10.1 != 10.2"),
        (["10.1:gt:10.2"], "10.1 > 10.2"),
        (["10.1:gte:10.2"], "10.1 >= 10.2"),
        (["10.1:lt:10.2"], "10.1 < 10.2"),
        (["10.1:lte:10.2"], "10.1 <= 10.2"),
        (["str:isTrue"], "str IS TRUE"),
        (["str:isNotTrue"], "str IS NOT TRUE"),
        (["str:isFalse"], "str IS FALSE"),
        (["str:isNotFalse"], "str IS NOT FALSE"),
        (["str:isNull"], "str IS NULL"),
        (["str:isNotNull"], "str IS NOT NULL"),
        (["str1:strEq:str2"], "'str1' LIKE 'str2'"),
        # Checking compound statements
        (
            ["str1:strEq:str2", "str1:strEqCI:str2"],
            "'str1' LIKE 'str2' OR 'str1' ILIKE 'str2'",
        ),
        (
            ["str1:strEq:str2,str1:strEqCI:str2"],
            "'str1' LIKE 'str2' AND 'str1' ILIKE 'str2'",
        ),
        (
            ["str1:strEq:str2", "str1:strEq:str2,str1:strEqCI:str2"],
            "'str1' LIKE 'str2' OR 'str1' LIKE 'str2' AND 'str1' ILIKE 'str2'",
        ),
    ],
)
def test_filter_string(input, output):
    assert get_filter_string(input) == output
