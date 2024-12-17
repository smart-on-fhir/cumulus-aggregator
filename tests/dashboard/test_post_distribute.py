import json
import os
from unittest import mock

import pytest
import responses
from freezegun import freeze_time

from src.dashboard.post_distribute import post_distribute


@mock.patch.dict(
    os.environ, {"TOPIC_STUDY_PAYLOAD_ARN": "test-payload", "AWS_REGION": "us-east-1"}, clear=True
)
@pytest.mark.parametrize(
    "name,url,expected_status",
    [
        (
            "test_study",
            "https://github.com/smart-on-fhir/test_study/",
            200,
        ),
        ("invalid_study", "https://github.com/smart-on-fhir/invalid_study", 500),
        ("non_cumulus_repo", "https://github.com/user/non_cumulus_repo", 500),
    ],
)
@responses.activate
@freeze_time("2020-01-01")
def test_process_github(mock_notification, tmp_path, name, url, expected_status, monkeypatch):
    responses.add(
        responses.GET,
        "https://api.github.com/repos/smart-on-fhir/test_study/git/trees/main?recursive=1",
        json={
            "tree": [
                {
                    "path": ".github",
                    "type": "tree",
                },
                {
                    "path": "test.sql",
                    "type": "blob",
                },
                {
                    "path": "manifest.toml",
                    "type": "blob",
                },
            ]
        },
    )
    responses.add(
        responses.GET,
        "https://api.github.com/repos/smart-on-fhir/invalid_study/git/trees/main?recursive=1",
        status=404,
    )
    responses.add(
        responses.GET,
        "https://raw.githubusercontent.com/smart-on-fhir/test_study/main/test.sql",
        body="""CREATE TABLE test_study__table AS
SELECT * from core__patient""",
    )
    responses.add(
        responses.GET,
        "https://raw.githubusercontent.com/smart-on-fhir/test_study/main/manifest.toml",
        body="""study_prefix="test_study"
[file_config]
file_names=[
    "test.sql"
]""",
    )
    monkeypatch.setattr(post_distribute, "BASE_DIR", tmp_path)
    mock_sns = mock.MagicMock()
    monkeypatch.setattr(post_distribute, "sns_client", mock_sns)
    res = post_distribute.distribute_handler(
        {"body": json.dumps({"github": url, "study_name": name})}, {}
    )
    assert res["statusCode"] == expected_status
    if expected_status == 200:
        assert mock_sns.publish.is_called()
        expected_message = {
            "TopicArn": "test-payload",
            "MessageGroupId": "test_study",
            "Subject": "test_study",
        }
        for k, v in mock_sns.publish.call_args[1].items():
            if k == "Message":
                # zipping these files is not 100% stochastic due to the tmpdir name, so
                # we'll just check for the expected file in the zip binary string
                assert "0000.test.00.create_table_test_study__table" in v
            else:
                assert expected_message[k] == v


def test_invalid_key():
    res = post_distribute.distribute_handler({"body": json.dumps({"bad_para,": "foo"})}, {})
    assert res["statusCode"] == 500
