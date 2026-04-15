"""Integration tests for the bulk export path in FhirLakeflowConnect."""

from unittest.mock import MagicMock, patch

from databricks.labs.community_connector.sources.fhir.fhir import FhirLakeflowConnect


def _make_options(**overrides):
    opts = {
        "base_url": "https://fhir.example.com/r4",
        "auth_type": "none",
    }
    opts.update(overrides)
    return opts


def _make_connector(**overrides):
    return FhirLakeflowConnect(_make_options(**overrides))


# ── Bulk path routing ─────────────────────────────────────────────────────────


def test_bulk_path_not_used_when_disabled():
    c = _make_connector()
    assert not c._bulk_enabled


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_bulk_path_used_when_enabled(MockBulkClient):
    mock_instance = MagicMock()
    mock_instance.export.return_value = (
        "2024-06-01T00:00:00Z",
        {
            "Patient": [
                {
                    "resourceType": "Patient",
                    "id": "p1",
                    "meta": {"lastUpdated": "2024-06-01T00:00:00Z"},
                },
            ]
        },
    )
    MockBulkClient.return_value = mock_instance

    c = _make_connector(bulk_export_enabled="true")
    assert c._bulk_enabled

    records, offset = c.read_table("Patient", None, {"profile": "uk_core"})
    record_list = list(records)
    assert len(record_list) == 1
    assert record_list[0]["id"] == "p1"
    mock_instance.export.assert_called_once()


# ── Cursor / offset behavior ─────────────────────────────────────────────────


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_bulk_uses_transaction_time_as_cursor(MockBulkClient):
    mock_instance = MagicMock()
    mock_instance.export.return_value = (
        "2024-06-15T12:00:00Z",
        {
            "Patient": [
                {
                    "resourceType": "Patient",
                    "id": "p1",
                    "meta": {"lastUpdated": "2024-06-01T00:00:00Z"},
                },
            ]
        },
    )
    MockBulkClient.return_value = mock_instance

    c = _make_connector(bulk_export_enabled="true")
    _, offset = c.read_table("Patient", None, {})
    assert offset == {"cursor": "2024-06-15T12:00:00Z"}


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_bulk_short_circuits_when_cursor_ge_init_ts(MockBulkClient):
    mock_instance = MagicMock()
    MockBulkClient.return_value = mock_instance

    c = _make_connector(bulk_export_enabled="true")
    # Set a cursor far in the future (past init_ts)
    start_offset = {"cursor": "2099-01-01T00:00:00+00:00"}
    records, offset = c.read_table("Patient", start_offset, {})
    assert list(records) == []
    assert offset == start_offset
    mock_instance.export.assert_not_called()


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_bulk_returns_empty_when_no_records(MockBulkClient):
    mock_instance = MagicMock()
    mock_instance.export.return_value = ("2024-06-01T00:00:00Z", {})
    MockBulkClient.return_value = mock_instance

    c = _make_connector(bulk_export_enabled="true")
    records, offset = c.read_table("Patient", None, {})
    assert list(records) == []
    assert offset == {}


# ── Incremental mode passthrough ──────────────────────────────────────────────


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_bulk_passes_incremental_mode(MockBulkClient):
    mock_instance = MagicMock()
    mock_instance.export.return_value = (
        "2024-06-01T00:00:00Z",
        {
            "Patient": [
                {
                    "resourceType": "Patient",
                    "id": "p1",
                    "meta": {"lastUpdated": "2024-06-01T00:00:00Z"},
                },
            ]
        },
    )
    MockBulkClient.return_value = mock_instance

    c = _make_connector(bulk_export_enabled="true", bulk_incremental_mode="typefilter")
    c.read_table("Patient", {"cursor": "2024-01-01T00:00:00Z"}, {})

    call_kwargs = mock_instance.export.call_args
    assert (
        call_kwargs.kwargs.get("incremental_mode") == "typefilter"
        or (call_kwargs.args and len(call_kwargs.args) > 2 and call_kwargs.args[2] == "typefilter")
        or call_kwargs[1].get("incremental_mode") == "typefilter"
    )


# ── Group scope initialization ────────────────────────────────────────────────


@patch("databricks.labs.community_connector.sources.fhir.fhir.BulkExportClient")
def test_bulk_group_scope_passes_group_id(MockBulkClient):
    MockBulkClient.return_value = MagicMock()
    _make_connector(
        bulk_export_enabled="true",
        bulk_export_scope="group",
        bulk_export_group_id="g-abc123",
    )
    call_kwargs = MockBulkClient.call_args
    assert call_kwargs.kwargs.get("scope") == "group" or call_kwargs[1].get("scope") == "group"
    assert (
        call_kwargs.kwargs.get("group_id") == "g-abc123"
        or call_kwargs[1].get("group_id") == "g-abc123"
    )
