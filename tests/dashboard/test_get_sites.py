import json

from src.dashboard.get_sites import get_sites
from tests import mock_utils


def test_get_metadata(mock_bucket):
    res = get_sites.sites_handler({}, {})
    assert json.loads(res["body"]) == {
        mock_utils.EXISTING_SITE: "Princeton Plainsboro Teaching Hospital",
        mock_utils.OTHER_SITE: "Saint Elsewhere",
        mock_utils.NEW_SITE: "Chicago Hope",
    }
