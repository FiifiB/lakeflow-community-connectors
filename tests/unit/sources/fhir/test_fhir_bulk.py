"""Unit tests for BulkExportClient — all HTTP calls are mocked."""

import json
from unittest.mock import MagicMock, patch

import pytest

from databricks.labs.community_connector.sources.fhir.fhir_bulk import BulkExportClient
from databricks.labs.community_connector.sources.fhir.fhir_utils import SmartAuthClient


def _make_auth(auth_type="none"):
    return SmartAuthClient(token_url="", client_id="", auth_type=auth_type)


def _make_client(scope="system", group_id=None, poll_interval=1, timeout=5):
    return BulkExportClient(
        base_url="https://fhir.example.com/r4",
        auth_client=_make_auth(),
        scope=scope,
        group_id=group_id,
        poll_interval=poll_interval,
        timeout=timeout,
    )


# ── Constructor tests ─────────────────────────────────────────────────────────


def test_system_scope_url():
    c = _make_client(scope="system")
    assert c._export_url() == "https://fhir.example.com/r4/$export"


def test_patient_scope_url():
    c = _make_client(scope="patient")
    assert c._export_url() == "https://fhir.example.com/r4/Patient/$export"


def test_group_scope_url():
    c = _make_client(scope="group", group_id="g123")
    assert c._export_url() == "https://fhir.example.com/r4/Group/g123/$export"


def test_group_scope_requires_group_id():
    with pytest.raises(ValueError, match="bulk_export_group_id is required"):
        _make_client(scope="group", group_id=None)


def test_invalid_scope_raises():
    with pytest.raises(ValueError, match="Invalid bulk_export_scope"):
        _make_client(scope="invalid")


# ── Kick-off tests ────────────────────────────────────────────────────────────


def test_kick_off_returns_status_url():
    c = _make_client()
    resp = MagicMock()
    resp.status_code = 202
    resp.headers = {"Content-Location": "https://fhir.example.com/status/123"}
    c._session = MagicMock()
    c._session.get.return_value = resp

    url = c._kick_off(["Patient"], None, "since")
    assert url == "https://fhir.example.com/status/123"


def test_kick_off_sends_correct_headers():
    c = _make_client()
    resp = MagicMock()
    resp.status_code = 202
    resp.headers = {"Content-Location": "https://fhir.example.com/status/1"}
    c._session = MagicMock()
    c._session.get.return_value = resp

    c._kick_off(["Patient"], None, "since")
    call_kwargs = c._session.get.call_args
    headers = call_kwargs.kwargs.get("headers") or call_kwargs[1].get("headers")
    assert headers["Prefer"] == "respond-async"
    assert "application/fhir+json" in headers["Accept"]


def test_kick_off_since_mode_adds_since_param():
    c = _make_client()
    resp = MagicMock()
    resp.status_code = 202
    resp.headers = {"Content-Location": "https://fhir.example.com/status/1"}
    c._session = MagicMock()
    c._session.get.return_value = resp

    c._kick_off(["Patient"], "2024-01-01T00:00:00Z", "since")
    call_kwargs = c._session.get.call_args
    params = call_kwargs.kwargs.get("params") or call_kwargs[1].get("params")
    assert params["_since"] == "2024-01-01T00:00:00Z"
    assert "_typeFilter" not in params


def test_kick_off_typefilter_mode_builds_filter():
    c = _make_client()
    resp = MagicMock()
    resp.status_code = 202
    resp.headers = {"Content-Location": "https://fhir.example.com/status/1"}
    c._session = MagicMock()
    c._session.get.return_value = resp

    c._kick_off(["Patient", "Observation"], "2024-01-01T00:00:00Z", "typefilter")
    call_kwargs = c._session.get.call_args
    params = call_kwargs.kwargs.get("params") or call_kwargs[1].get("params")
    assert "_since" not in params
    assert "Patient?_lastUpdated=gt2024-01-01T00:00:00Z" in params["_typeFilter"]
    assert "Observation?_lastUpdated=gt2024-01-01T00:00:00Z" in params["_typeFilter"]


def test_kick_off_full_mode_no_since():
    c = _make_client()
    resp = MagicMock()
    resp.status_code = 202
    resp.headers = {"Content-Location": "https://fhir.example.com/status/1"}
    c._session = MagicMock()
    c._session.get.return_value = resp

    c._kick_off(["Patient"], "2024-01-01T00:00:00Z", "full")
    call_kwargs = c._session.get.call_args
    params = call_kwargs.kwargs.get("params") or call_kwargs[1].get("params")
    assert "_since" not in params
    assert "_typeFilter" not in params


def test_kick_off_raises_on_non_202():
    c = _make_client()
    resp = MagicMock()
    resp.status_code = 400
    resp.text = "Bad Request"
    resp.headers = {}
    c._session = MagicMock()
    c._session.get.return_value = resp

    with pytest.raises(RuntimeError, match="kick-off failed"):
        c._kick_off(["Patient"], None, "since")


# ── Poll tests ────────────────────────────────────────────────────────────────


def test_poll_returns_manifest_on_200():
    c = _make_client()
    manifest = {
        "transactionTime": "2024-01-15T00:00:00Z",
        "requiresAccessToken": True,
        "output": [{"type": "Patient", "url": "https://fhir.example.com/file1.ndjson"}],
        "error": [],
    }
    resp = MagicMock()
    resp.status_code = 200
    resp.json.return_value = manifest
    c._session = MagicMock()
    c._session.get.return_value = resp

    result = c._poll_until_complete("https://fhir.example.com/status/1")
    assert result == manifest


@patch("databricks.labs.community_connector.sources.fhir.fhir_bulk.time.sleep")
def test_poll_waits_on_202_then_returns(mock_sleep):
    c = _make_client(poll_interval=1, timeout=30)

    resp_202 = MagicMock()
    resp_202.status_code = 202
    resp_202.headers = {"Retry-After": "2", "X-Progress": "50%"}

    manifest = {"transactionTime": "2024-01-15T00:00:00Z", "output": [], "error": []}
    resp_200 = MagicMock()
    resp_200.status_code = 200
    resp_200.json.return_value = manifest

    c._session = MagicMock()
    c._session.get.side_effect = [resp_202, resp_200]

    result = c._poll_until_complete("https://fhir.example.com/status/1")
    assert result == manifest
    mock_sleep.assert_called_once_with(2.0)


def test_poll_raises_on_timeout():
    c = _make_client(timeout=0)
    resp = MagicMock()
    resp.status_code = 202
    resp.headers = {}
    c._session = MagicMock()
    c._session.get.return_value = resp

    with pytest.raises(RuntimeError, match="timed out"):
        c._poll_until_complete("https://fhir.example.com/status/1")


def test_poll_raises_on_non_retriable_error():
    c = _make_client()
    resp = MagicMock()
    resp.status_code = 400
    resp.text = "Bad Request"
    c._session = MagicMock()
    c._session.get.return_value = resp

    with pytest.raises(RuntimeError, match="status check failed"):
        c._poll_until_complete("https://fhir.example.com/status/1")


@patch("databricks.labs.community_connector.sources.fhir.fhir_bulk.time.sleep")
def test_poll_retries_on_transient_500_then_succeeds(mock_sleep):
    c = _make_client(poll_interval=1, timeout=30)

    resp_500 = MagicMock()
    resp_500.status_code = 500
    resp_500.text = "Internal Server Error"
    resp_500.headers = {}

    manifest = {"transactionTime": "2024-01-15T00:00:00Z", "output": [], "error": []}
    resp_200 = MagicMock()
    resp_200.status_code = 200
    resp_200.json.return_value = manifest

    c._session = MagicMock()
    c._session.get.side_effect = [resp_500, resp_200]

    result = c._poll_until_complete("https://fhir.example.com/status/1")
    assert result == manifest
    assert mock_sleep.call_count == 1


# ── Download tests ────────────────────────────────────────────────────────────


def test_download_ndjson_parses_lines():
    c = _make_client()
    ndjson = [
        json.dumps({"resourceType": "Patient", "id": "p1"}),
        "",
        json.dumps({"resourceType": "Patient", "id": "p2"}),
    ]
    resp = MagicMock()
    resp.status_code = 200
    resp.iter_lines.return_value = iter(ndjson)
    resp.close = MagicMock()
    c._session = MagicMock()
    c._session.get.return_value = resp

    records = list(c._download_ndjson("https://fhir.example.com/file.ndjson", True))
    assert len(records) == 2
    assert records[0]["id"] == "p1"
    assert records[1]["id"] == "p2"


def test_download_ndjson_no_auth_when_not_required():
    c = _make_client()
    resp = MagicMock()
    resp.status_code = 200
    resp.iter_lines.return_value = iter([])
    resp.close = MagicMock()
    c._session = MagicMock()
    c._session.get.return_value = resp

    list(c._download_ndjson("https://presigned.s3.amazonaws.com/file.ndjson", False))
    call_kwargs = c._session.get.call_args
    headers = call_kwargs.kwargs.get("headers") or call_kwargs[1].get("headers")
    assert "Authorization" not in headers


def test_download_ndjson_handles_307_redirect():
    c = _make_client()
    redirect_resp = MagicMock()
    redirect_resp.status_code = 307
    redirect_resp.headers = {"Location": "https://s3.amazonaws.com/presigned-file"}
    redirect_resp.close = MagicMock()

    ndjson_resp = MagicMock()
    ndjson_resp.status_code = 200
    ndjson_resp.iter_lines.return_value = iter(
        [
            json.dumps({"resourceType": "Patient", "id": "p1"}),
        ]
    )
    ndjson_resp.close = MagicMock()

    c._session = MagicMock()
    c._session.get.side_effect = [redirect_resp, ndjson_resp]

    records = list(c._download_ndjson("https://cerner.com/bulk/files/123", True))
    assert len(records) == 1

    second_call = c._session.get.call_args_list[1]
    headers = second_call.kwargs.get("headers") or second_call[1].get("headers")
    assert "Authorization" not in headers


# ── download_and_parse tests ──────────────────────────────────────────────────


def test_download_and_parse_groups_by_type():
    c = _make_client()
    manifest = {
        "requiresAccessToken": True,
        "output": [
            {"type": "Patient", "url": "https://fhir.example.com/patient.ndjson"},
            {"type": "Observation", "url": "https://fhir.example.com/obs.ndjson"},
        ],
        "error": [],
    }

    def fake_download(url, requires_token):
        if "patient" in url:
            return iter([{"resourceType": "Patient", "id": "p1"}])
        return iter(
            [
                {"resourceType": "Observation", "id": "o1"},
                {"resourceType": "Observation", "id": "o2"},
            ]
        )

    c._download_ndjson = fake_download
    result = c._download_and_parse(manifest)
    assert len(result["Patient"]) == 1
    assert len(result["Observation"]) == 2


def test_download_and_parse_empty_output():
    c = _make_client()
    manifest = {"requiresAccessToken": True, "output": [], "error": []}
    c._download_ndjson = MagicMock()
    result = c._download_and_parse(manifest)
    assert result == {}


# ── export() end-to-end (mocked) ─────────────────────────────────────────────


def test_export_returns_transaction_time_and_records():
    c = _make_client()
    manifest = {
        "transactionTime": "2024-06-01T12:00:00Z",
        "requiresAccessToken": True,
        "output": [{"type": "Patient", "url": "https://fhir.example.com/p.ndjson"}],
        "error": [],
    }
    c._kick_off = MagicMock(return_value="https://fhir.example.com/status/1")
    c._poll_until_complete = MagicMock(return_value=manifest)
    c._download_ndjson = lambda url, tok: iter([{"resourceType": "Patient", "id": "p1"}])
    c._cleanup = MagicMock()

    tx_time, resources = c.export(["Patient"], since=None, incremental_mode="since")
    assert tx_time == "2024-06-01T12:00:00Z"
    assert len(resources["Patient"]) == 1
    c._cleanup.assert_called_once()


def test_export_invalid_incremental_mode_raises():
    c = _make_client()
    with pytest.raises(ValueError, match="Invalid bulk_incremental_mode"):
        c.export(["Patient"], incremental_mode="invalid")


# ── Cleanup tests ─────────────────────────────────────────────────────────────


def test_cleanup_does_not_raise_on_error():
    c = _make_client()
    c._session = MagicMock()
    c._session.delete.side_effect = ConnectionError("network error")
    c._cleanup("https://fhir.example.com/status/1")
