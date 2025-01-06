import json
import os
import pathlib
import shutil
import subprocess
from unittest import mock

import pytest
import responses
from freezegun import freeze_time

from src.dashboard.queue_distribute import queue_distribute


def error_callback(process):
    process.returncode = 1
    raise subprocess.CalledProcessError(1, "Error cloning")


@mock.patch.dict(
    os.environ, {"TOPIC_STUDY_PAYLOAD_ARN": "test-payload", "AWS_REGION": "us-east-1"}, clear=True
)
@pytest.mark.parametrize(
    "name,url,tag,expected_status",
    [
        (
            "test_study",
            "https://github.com/smart-on-fhir/cumulus-aggregator-test-study/",
            None,
            200,
        ),
        (
            "test_study",
            "https://github.com/smart-on-fhir/cumulus-aggregator-test-study/",
            "tag",
            200,
        ),
        ("invalid_study", "https://github.com/smart-on-fhir/invalid_study", None, 500),
    ],
)
@responses.activate
def test_process_github(
    mock_notification, tmp_path, name, url, tag, expected_status, monkeypatch, fp
):
    fp.allow_unregistered(True)  # fp is provided by pytest-subprocess

    args = ["--depth", "1", url, f"{tmp_path}/studies"]
    if tag:
        args = ["--branch", tag, *args]
    if name == "invalid_study":
        fp.register(["/usr/bin/git", "clone", *args], callback=error_callback)
    else:
        fp.register(["/usr/bin/git", "clone", *args])
        (tmp_path / "studies").mkdir()
        study_dir = tmp_path / f"studies/{name}"
        shutil.copytree(
            pathlib.Path.cwd() / "./tests/test_data/mock_payloads/cumulus-aggregator-test-study",
            study_dir,
        )
        if tag:
            # if we're checking out a tag, make sure we've set up an actual git repo
            subprocess.run(["git", "checkout", "tag"], cwd=study_dir)

    monkeypatch.setattr(queue_distribute, "BASE_DIR", tmp_path)
    mock_sns = mock.MagicMock()
    monkeypatch.setattr(queue_distribute, "sns_client", mock_sns)
    with freeze_time("2020-01-01"):  # Don't use as a fixture for this test; collides with fp mock
        res = queue_distribute.queue_handler(
            {
                "Records": [
                    {
                        "Sns": {
                            "Message": json.dumps(
                                {"github": {"url": url, "tag": tag}, "study_name": name}
                            )
                        }
                    }
                ]
            },
            {},
        )
    assert res["statusCode"] == expected_status
    if expected_status == 200:
        assert mock_sns.publish.is_called()
        expected_message = {
            "TopicArn": "test-payload",
            "MessageGroupId": "test_study",
            "Message": '{"study": "test_study"}',
            "Subject": "test_study",
        }
        for k, v in mock_sns.publish.call_args[1].items():
            if k == "MessageAttributes":
                assert name in str(v["study"]["BinaryValue"])
            else:
                assert expected_message[k] == v
    if tag == "tag":
        files = [p for p in (tmp_path / f"studies/{name}").iterdir() if p.is_file()]
        files = [file.stem for file in files]
        print(type(files[0]))
        assert "tag" not in files
