"""FHIR Bulk Data Export client (HL7 Bulk Data Access IG).

Implements the async $export workflow:
  1. Kick-off:  GET [base]/$export  ->  202 + Content-Location
  2. Poll:      GET [status-url]    ->  202 (in-progress) | 200 (manifest)
  3. Download:  GET [file-url]      ->  NDJSON stream
  4. Cleanup:   DELETE [status-url]  ->  202 (best-effort)

Supports all three export scopes (system, patient, group) and handles
server-specific download patterns: bearer-token auth, presigned URLs,
and 307 redirects to presigned S3 (Cerner).
"""

import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import quote

import requests

from databricks.labs.community_connector.sources.fhir.fhir_constants import (
    BULK_EXPORT_TIMEOUT,
    BULK_NDJSON_TIMEOUT,
    BULK_POLL_INTERVAL,
    BULK_VALID_INCREMENTAL_MODES,
    BULK_VALID_SCOPES,
    HTTP_TIMEOUT,
    INITIAL_BACKOFF,
    MAX_RETRIES,
    RETRIABLE_STATUS_CODES,
)
from databricks.labs.community_connector.sources.fhir.fhir_utils import SmartAuthClient

logger = logging.getLogger(__name__)


class BulkExportClient:
    """Client for the FHIR Bulk Data Access IG async export workflow."""

    def __init__(
        self,
        base_url: str,
        auth_client: SmartAuthClient,
        scope: str = "system",
        group_id: Optional[str] = None,
        poll_interval: int = BULK_POLL_INTERVAL,
        timeout: int = BULK_EXPORT_TIMEOUT,
    ) -> None:
        if scope not in BULK_VALID_SCOPES:
            raise ValueError(
                f"Invalid bulk_export_scope {scope!r}. Use one of: {sorted(BULK_VALID_SCOPES)}."
            )
        if scope == "group" and not group_id:
            raise ValueError("bulk_export_group_id is required when bulk_export_scope is 'group'.")
        self._base_url = base_url.rstrip("/")
        self._auth_client = auth_client
        self._scope = scope
        self._group_id = group_id
        self._poll_interval = poll_interval
        self._timeout = timeout
        self._session = requests.Session()

    def _export_url(self) -> str:
        if self._scope == "patient":
            return f"{self._base_url}/Patient/$export"
        if self._scope == "group":
            safe_id = quote(self._group_id, safe="")
            return f"{self._base_url}/Group/{safe_id}/$export"
        return f"{self._base_url}/$export"

    def _auth_headers(self) -> dict:
        token = self._auth_client.get_token()
        h = {"Accept": "application/fhir+json"}
        if token:
            h["Authorization"] = f"Bearer {token}"
        return h

    def _ndjson_auth_headers(self) -> dict:
        token = self._auth_client.get_token()
        h = {"Accept": "application/fhir+ndjson"}
        if token:
            h["Authorization"] = f"Bearer {token}"
        return h

    def export(
        self,
        resource_types: list,
        since: Optional[str] = None,
        incremental_mode: str = "since",
    ) -> tuple:
        """Run a full bulk export cycle: kick-off -> poll -> download -> cleanup.

        Returns (transaction_time, {resource_type: [resource_dicts]}).
        transaction_time is an ISO-8601 string suitable for use as a CDC cursor.
        For incremental_mode="full", transaction_time is None (caller should not
        advance the cursor).
        """
        if incremental_mode not in BULK_VALID_INCREMENTAL_MODES:
            raise ValueError(
                f"Invalid bulk_incremental_mode {incremental_mode!r}. "
                f"Use one of: {sorted(BULK_VALID_INCREMENTAL_MODES)}."
            )

        status_url = self._kick_off(resource_types, since, incremental_mode)
        try:
            manifest = self._poll_until_complete(status_url)
            resources = self._download_and_parse(manifest)
            if incremental_mode == "full":
                # Full mode re-exports all data every run — don't advance cursor.
                return None, resources
            transaction_time = manifest.get("transactionTime")
            if not transaction_time:
                logger.warning(
                    "Bulk export manifest missing transactionTime. "
                    "Falling back to current UTC time as cursor — this may "
                    "cause duplicate records on the next incremental run."
                )
                transaction_time = datetime.now(timezone.utc).isoformat()
            return transaction_time, resources
        finally:
            self._cleanup(status_url)

    def _kick_off(
        self,
        resource_types: list,
        since: Optional[str],
        incremental_mode: str,
    ) -> str:
        """Issue the async $export request. Returns the status polling URL."""
        url = self._export_url()
        params = {}

        if incremental_mode == "typefilter" and since and resource_types:
            # Per HL7 spec, _typeFilter encodes the resource type in each filter
            # string, so _type is redundant and some servers reject the combination.
            filters = [f"{rt}?_lastUpdated=gt{since}" for rt in resource_types]
            params["_typeFilter"] = ",".join(filters)
        else:
            if resource_types:
                params["_type"] = ",".join(resource_types)
            if since and incremental_mode == "since":
                params["_since"] = since

        headers = self._auth_headers()
        headers["Prefer"] = "respond-async"

        backoff = INITIAL_BACKOFF
        for attempt in range(MAX_RETRIES):
            resp = self._session.get(
                url,
                params=params,
                headers=headers,
                timeout=HTTP_TIMEOUT,
            )
            if resp.status_code == 202:
                status_url = resp.headers.get("Content-Location")
                if not status_url:
                    raise RuntimeError(
                        "Bulk export kick-off returned 202 but no Content-Location header. "
                        f"Response headers: {dict(resp.headers)}"
                    )
                logger.info("Bulk export started. Status URL: %s", status_url)
                return status_url
            if resp.status_code not in RETRIABLE_STATUS_CODES:
                raise RuntimeError(
                    f"Bulk export kick-off failed (HTTP {resp.status_code}): {resp.text[:500]}"
                )
            if attempt < MAX_RETRIES - 1:
                sleep_seconds = _retry_after(resp, backoff)
                logger.warning(
                    "Bulk export kick-off got %d, retrying in %.0fs (attempt %d/%d)",
                    resp.status_code,
                    sleep_seconds,
                    attempt + 1,
                    MAX_RETRIES,
                )
                time.sleep(sleep_seconds)
                backoff *= 2

        raise RuntimeError(
            f"Bulk export kick-off failed after {MAX_RETRIES} retries "
            f"(last HTTP {resp.status_code}): {resp.text[:500]}"
        )

    def _poll_until_complete(self, status_url: str) -> dict:
        """Poll the status URL until the export completes or times out.

        Retries on transient errors (429, 500, 502, 503) with backoff.
        """
        deadline = time.time() + self._timeout
        transient_backoff = INITIAL_BACKOFF
        while time.time() < deadline:
            resp = self._session.get(
                status_url,
                headers=self._auth_headers(),
                timeout=HTTP_TIMEOUT,
            )
            if resp.status_code == 200:
                return resp.json()

            if resp.status_code == 202:
                progress = resp.headers.get("X-Progress", "")
                if progress:
                    logger.info("Bulk export progress: %s", progress)
                sleep_seconds = _retry_after(resp, self._poll_interval)
                transient_backoff = INITIAL_BACKOFF  # reset on successful poll
                time.sleep(sleep_seconds)
                continue

            if resp.status_code in RETRIABLE_STATUS_CODES:
                sleep_seconds = _retry_after(resp, transient_backoff)
                logger.warning(
                    "Bulk export poll got %d, retrying in %.0fs",
                    resp.status_code,
                    sleep_seconds,
                )
                time.sleep(sleep_seconds)
                transient_backoff = min(transient_backoff * 2, 120)
                continue

            raise RuntimeError(
                f"Bulk export status check failed (HTTP {resp.status_code}): {resp.text[:500]}"
            )

        raise RuntimeError(
            f"Bulk export timed out after {self._timeout}s. Status URL: {status_url}"
        )

    def _download_and_parse(self, manifest: dict) -> dict:
        """Download NDJSON files from the manifest and group records by type."""
        requires_token = manifest.get("requiresAccessToken", True)
        resources: dict = {}

        for entry in manifest.get("output", []):
            rtype = entry.get("type", "Unknown")
            url = entry.get("url")
            if not url:
                continue
            records = list(self._download_ndjson(url, requires_token))
            resources.setdefault(rtype, []).extend(records)
            logger.info(
                "Downloaded %d %s resources from %s",
                len(records),
                rtype,
                url,
            )

        for entry in manifest.get("error", []):
            url = entry.get("url")
            if url:
                try:
                    errors = list(self._download_ndjson(url, requires_token))
                    for err in errors:
                        logger.warning("Bulk export OperationOutcome: %s", json.dumps(err)[:300])
                except Exception as exc:
                    logger.warning("Failed to download error file %s: %s", url, exc)

        return resources

    def _download_ndjson(self, url: str, requires_access_token: bool = True):
        """Download and parse an NDJSON file, yielding one dict per line.

        Handles three download patterns:
        1. Direct HTTP + bearer token (Epic, HAPI, InterSystems, etc.)
        2. No auth / presigned URL (IBM, SMART test server)
        3. 307 redirect to presigned S3 (Cerner) — strips auth on redirect
        """
        headers = (
            self._ndjson_auth_headers()
            if requires_access_token
            else {"Accept": "application/fhir+ndjson"}
        )
        resp = self._session.get(
            url,
            headers=headers,
            stream=True,
            allow_redirects=False,
            timeout=BULK_NDJSON_TIMEOUT,
        )

        if resp.status_code in (301, 302, 303, 307, 308):
            redirect_url = resp.headers.get("Location")
            if not redirect_url:
                resp.close()
                raise RuntimeError(f"NDJSON download got {resp.status_code} but no Location header")
            resp.close()
            resp = self._session.get(
                redirect_url,
                headers={"Accept": "application/fhir+ndjson"},
                stream=True,
                timeout=BULK_NDJSON_TIMEOUT,
            )

        try:
            if resp.status_code != 200:
                raise RuntimeError(
                    f"NDJSON download failed (HTTP {resp.status_code}): {resp.text[:500]}"
                )
            for line in resp.iter_lines(decode_unicode=True):
                if line:
                    yield json.loads(line)
        finally:
            resp.close()

    def _cleanup(self, status_url: str) -> None:
        """Best-effort DELETE of the export job. Never raises."""
        try:
            self._session.delete(
                status_url,
                headers=self._auth_headers(),
                timeout=HTTP_TIMEOUT,
            )
        except Exception as exc:
            logger.warning("Bulk export cleanup failed for %s: %s", status_url, exc)


def _retry_after(resp: requests.Response, default: float) -> float:
    """Extract Retry-After header value in seconds, or return default."""
    retry_after = resp.headers.get("Retry-After")
    if retry_after:
        try:
            return float(retry_after)
        except ValueError:
            pass
    return default
