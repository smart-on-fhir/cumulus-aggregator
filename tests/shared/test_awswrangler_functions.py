from contextlib import nullcontext as does_not_raise

import pytest

from src.shared import awswrangler_functions, enums
from tests import mock_utils

AGG_PATH = (
    "s3://cumulus-aggregator-site-counts-test/aggregates/study/study__encounter/"
    "study__encounter__099/study__encounter__aggregate.parquet"
)
FLAT_PATH = (
    "s3://cumulus-aggregator-site-counts-test/flat/study/princeton_plainsboro_teaching_hospital/"
    "study__encounter__princeton_plainsboro_teaching_hospital__099/study__encounter__flat.parquet"
)
STUDY_META_PATH = (
    "s3://cumulus-aggregator-site-counts-test/study_metadata/study/study__encounter/"
    "princeton_plainsboro_teaching_hospital/099/study__meta_date.parquet"
)


@pytest.mark.parametrize(
    "root,extension,version,site,expects,raises",
    [
        (enums.BucketPath.AGGREGATE.value, "parquet", None, None, [AGG_PATH], does_not_raise()),
        (
            enums.BucketPath.AGGREGATE.value,
            "parquet",
            f"{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}__{mock_utils.EXISTING_VERSION}",
            None,
            [AGG_PATH],
            does_not_raise(),
        ),
        (
            enums.BucketPath.AGGREGATE.value,
            "parquet",
            "missing_version",
            None,
            [],
            does_not_raise(),
        ),
        (enums.BucketPath.FLAT.value, ".parquet", None, None, [FLAT_PATH], does_not_raise()),
        (
            enums.BucketPath.FLAT.value,
            ".parquet",
            None,
            mock_utils.EXISTING_SITE,
            [FLAT_PATH],
            does_not_raise(),
        ),
        (enums.BucketPath.FLAT.value, ".parquet", None, "missing_site", [], does_not_raise()),
        (
            enums.BucketPath.FLAT.value,
            ".parquet",
            (
                f"{mock_utils.EXISTING_STUDY}__{mock_utils.EXISTING_DATA_P}__"
                f"{mock_utils.EXISTING_SITE}__{mock_utils.EXISTING_VERSION}"
            ),
            mock_utils.EXISTING_SITE,
            [FLAT_PATH],
            does_not_raise(),
        ),
        (
            enums.BucketPath.FLAT.value,
            ".parquet",
            "missing_version",
            mock_utils.EXISTING_SITE,
            [],
            does_not_raise(),
        ),
        (enums.BucketPath.AGGREGATE.value, ".doc", None, None, [], does_not_raise()),
        (
            "athena",
            ".parquet",
            None,
            None,
            [],
            # test path hotmapping for symlinks makes catching the narrow exception fussy
            pytest.raises(Exception),
        ),
    ],
)
def test_get_package_list(mock_bucket, root, extension, version, site, expects, raises):
    with raises:
        res = awswrangler_functions.get_s3_data_package_list(
            root,
            mock_utils.TEST_BUCKET,
            mock_utils.EXISTING_STUDY,
            mock_utils.EXISTING_DATA_P,
            extension=extension,
            version=version,
            site=site,
        )
        assert res == expects


def test_get_s3_study_meta(mock_bucket):
    res = awswrangler_functions.get_s3_study_meta_list(
        mock_utils.TEST_BUCKET,
        mock_utils.EXISTING_STUDY,
        mock_utils.EXISTING_DATA_P,
        mock_utils.EXISTING_SITE,
        mock_utils.EXISTING_VERSION,
    )
    assert res == [STUDY_META_PATH]
