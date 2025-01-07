"""Validates that we've received a usable request for a study to distribute"""

import json
import os
from unittest import mock

import pytest
import responses
from freezegun import freeze_time

from src.dashboard.post_distribute import post_distribute


@mock.patch.dict(
    os.environ, {"TOPIC_QUEUE_API_ARN": "test-payload", "AWS_REGION": "us-east-1"}, clear=True
)
@pytest.mark.parametrize(
    "name,url,tag,expected_status",
    [
        (
            "test_study",
            "https://github.com/smart-on-fhir/test_study",
            None,
            200,
        ),
        (
            "test_study",
            "https://github.com/smart-on-fhir/test_study",
            "tag",
            200,
        ),
        (
            "invalid_study",
            "https://github.com/smart-on-fhir/invalid_study",
            None,
            500,
        ),
        (
            "invalid_tag",
            "https://github.com/smart-on-fhir/invalid_tag",
            "tag",
            500,
        ),
        ("non_cumulus_repo", "https://github.com/user/non_cumulus_repo", None, 500),
    ],
)
@responses.activate
@freeze_time("2020-01-01")
def test_process_github(mock_notification, tmp_path, name, url, tag, expected_status, monkeypatch):
    responses.add(responses.GET, "https://github.com/smart-on-fhir/test_study", status=200)
    responses.add(responses.GET, "https://github.com/smart-on-fhir/test_study/tree/tag", status=200)
    responses.add(
        responses.GET,
        "https://github.com/smart-on-fhir/invalid_study",
        status=404,
    )
    responses.add(
        responses.GET,
        "https://github.com/smart-on-fhir/invalid_tag",
        status=200,
    )
    responses.add(
        responses.GET,
        "https://github.com/smart-on-fhir/invalid_tag/tree/tag",
        status=404,
    )

    mock_sns = mock.MagicMock()
    monkeypatch.setattr(post_distribute, "sns_client", mock_sns)
    res = post_distribute.distribute_handler(
        {"body": json.dumps({"github": {"url": url, "tag": tag}, "study_name": name})}, {}
    )
    assert res["statusCode"] == expected_status
    if expected_status == 200:
        assert mock_sns.publish.is_called()
        expected_message = {
            "TopicArn": "test-payload",
            "MessageGroupId": "test_study",
            "Subject": "test_study",
            "Message": json.dumps({"github": {"url": url, "tag": tag}, "study_name": name}),
        }
        for k, v in mock_sns.publish.call_args[1].items():
            assert expected_message[k] == v


def test_invalid_key():
    res = post_distribute.distribute_handler({"body": json.dumps({"bad_para,": "foo"})}, {})
    assert res["statusCode"] == 500
