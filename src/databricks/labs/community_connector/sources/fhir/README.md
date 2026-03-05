# Lakeflow FHIR R4 Community Connector

This documentation describes how to configure and use the **FHIR R4** Lakeflow community connector to ingest clinical data from FHIR-compliant servers into Databricks.

## Overview

[FHIR (Fast Healthcare Interoperability Resources)](https://hl7.org/fhir/R4/) is the HL7 standard for exchanging healthcare information electronically. The FHIR R4 connector enables you to ingest clinical resources such as patients, observations, conditions, and encounters from any FHIR R4-compliant server into Delta tables in Databricks.

The connector supports incremental (CDC) ingestion using the `_lastUpdated` search parameter, so after the initial full load, only new and modified records are fetched on subsequent runs.

This connector has been tested against the following FHIR server implementations:

- **Epic** (SMART Backend Services)
- **Cerner / Oracle Health** (SMART Backend Services)
- **Azure Health Data Services** (Azure API for FHIR)
- **AWS HealthLake**
- **HAPI FHIR** (open-source reference server)

## FHIR Server Requirements

Your FHIR server **must** support the following capabilities for this connector to work correctly:

- **FHIR R4 (4.0.1)** — the server must expose FHIR R4-compliant endpoints.
- **Authentication** — the server must support one of:
  - SMART on FHIR Backend Services with JWT assertion (`jwt_assertion`)
  - OAuth2 client credentials with a symmetric secret (`client_secret`)
  - No authentication (`none`) — for open or development servers
- **`_lastUpdated` search parameter** — must be supported on all resource types you intend to ingest. This is how the connector performs incremental sync.
- **`_count` pagination parameter** — the server must honor the `_count` parameter to control page size.
- **Bundle searchset responses with pagination links** — the server must return FHIR Bundle resources of type `searchset` and provide `next` links for pagination.

> **Note:** The FHIR specification does not require servers to implement `_lastUpdated` (only `_id` is mandatory). If your server does not support `_lastUpdated`, the connector will detect this and raise an error with guidance.

## Prerequisites

- **FHIR server access**: You need network connectivity from your Databricks workspace to the FHIR server's base URL.
- **Credentials**: Depending on your authentication method, you will need either an RSA key pair, a client ID and secret, or no credentials at all (for open servers).
- **Lakeflow / Databricks environment**: A workspace where you can register a Lakeflow community connector and run ingestion pipelines.

## Authentication Setup

The connector supports three authentication methods. Choose the one that matches your FHIR server's capabilities.

### JWT Assertion (SMART Backend Services)

Use this method for production FHIR servers that support SMART Backend Services (e.g., Epic, Cerner).

1. **Generate an RSA key pair** (if you do not already have one):
   ```bash
   openssl genrsa -out private_key.pem 2048
   openssl rsa -in private_key.pem -pubout -out public_key.pem
   ```
2. **Register the public key** with your FHIR server. The exact process varies by server:
   - **Epic**: Register via the Epic App Orchard developer portal and upload the public key.
   - **Cerner**: Register via the Cerner Code developer portal.
   - After registration, you will receive a **client ID**.
3. **Locate the token endpoint URL**. This is typically available at:
   ```
   {fhir_base_url}/.well-known/smart-configuration
   ```
   Look for the `token_endpoint` field in the response.
4. **Determine the required scopes**. Common scopes include:
   - `system/*.read` (read all resource types)
   - `system/Patient.read system/Observation.read` (read specific resource types)
5. **Provide the following connection parameters**:
   - `auth_type`: `jwt_assertion`
   - `token_url`: the token endpoint URL from step 3
   - `client_id`: the client ID from step 2
   - `private_key_pem`: the contents of `private_key.pem` (include the `BEGIN` and `END` delimiters)
   - `scope`: the required scopes from step 4

### Client Secret

Use this method for servers that support OAuth2 client credentials with a symmetric secret (SMART v1 confidential clients).

1. **Obtain a client ID and client secret** from your FHIR server administrator.
2. **Locate the token endpoint URL** (same process as JWT assertion, step 3 above).
3. **Provide the following connection parameters**:
   - `auth_type`: `client_secret`
   - `token_url`: the token endpoint URL
   - `client_id`: the client ID
   - `client_secret`: the client secret
   - `scope` (optional): the required scopes, if your server requires them

### No Authentication

Use this method for open or development FHIR servers (e.g., the public HAPI FHIR test server at `https://hapi.fhir.org/baseR4`).

1. **Provide the following connection parameters**:
   - `auth_type`: `none`
   - `base_url`: the FHIR server URL (e.g., `https://hapi.fhir.org/baseR4`)

No additional credentials are required.

## Setup

### Connection Parameters

Provide the following **connection-level** options when configuring the connector.

| Name | Type | Required | Description |
|---|---|---|---|
| `base_url` | string | Yes | Base URL of the FHIR R4 server (e.g., `https://hapi.fhir.org/baseR4`). |
| `auth_type` | string | Yes | Authentication method: `jwt_assertion`, `client_secret`, or `none`. |
| `token_url` | string | Conditional | OAuth2 token endpoint URL. Required for `jwt_assertion` and `client_secret` auth. |
| `client_id` | string | Conditional | OAuth2 client ID. Required for `jwt_assertion` and `client_secret` auth. |
| `private_key_pem` | string | Conditional | RSA private key in PEM format. Required for `jwt_assertion` auth. |
| `client_secret` | string | Conditional | OAuth2 client secret. Required for `client_secret` auth. |
| `scope` | string | Conditional | OAuth2 scope(s), space-separated. Required for `jwt_assertion`; optional for `client_secret`. |
| `externalOptionsAllowList` | string | Yes | Comma-separated list of table-specific option names. Must be set to: `resource_types,page_size,max_records_per_batch` |

### Create a Unity Catalog Connection

A Unity Catalog connection for this connector can be created in two ways via the UI:

1. Follow the **Lakeflow Community Connector** UI flow from the **Add Data** page.
2. Select any existing Lakeflow Community Connector connection for this source or create a new one.
3. Set `externalOptionsAllowList` to `resource_types,page_size,max_records_per_batch`.

The connection can also be created using the standard Unity Catalog API.

## Supported Objects

The FHIR R4 connector ingests FHIR resources as tables. The following 14 resource types are supported by default:

| Resource Type | Primary Key | Ingestion Mode | Cursor Field |
|---|---|---|---|
| `Patient` | `id` | CDC | `lastUpdated` |
| `Observation` | `id` | CDC | `lastUpdated` |
| `Condition` | `id` | CDC | `lastUpdated` |
| `Encounter` | `id` | CDC | `lastUpdated` |
| `Procedure` | `id` | CDC | `lastUpdated` |
| `MedicationRequest` | `id` | CDC | `lastUpdated` |
| `DiagnosticReport` | `id` | CDC | `lastUpdated` |
| `AllergyIntolerance` | `id` | CDC | `lastUpdated` |
| `Immunization` | `id` | CDC | `lastUpdated` |
| `Coverage` | `id` | CDC | `lastUpdated` |
| `CarePlan` | `id` | CDC | `lastUpdated` |
| `Goal` | `id` | CDC | `lastUpdated` |
| `Device` | `id` | CDC | `lastUpdated` |
| `DocumentReference` | `id` | CDC | `lastUpdated` |

All resource types use **CDC (Change Data Capture)** ingestion. On the first run, all records are fetched. On subsequent runs, only records with a `lastUpdated` timestamp newer than the last cursor are retrieved.

Each table includes the full FHIR resource as a `raw_json` column alongside typed columns for commonly queried fields.

## Table Configurations

### Source & Destination

These are set directly under each `table` object in the pipeline spec:

| Option | Required | Description |
|---|---|---|
| `source_table` | Yes | FHIR resource type name (e.g., `Patient`, `Observation`). Must use exact casing as listed above. |
| `destination_catalog` | No | Target catalog (defaults to pipeline's default) |
| `destination_schema` | No | Target schema (defaults to pipeline's default) |
| `destination_table` | No | Target table name (defaults to `source_table`) |

### Common `table_configuration` options

These are set inside the `table_configuration` map alongside any source-specific options:

| Option | Required | Description |
|---|---|---|
| `scd_type` | No | `SCD_TYPE_1` (default) or `SCD_TYPE_2`. |
| `primary_keys` | No | List of columns to override the connector's default primary keys. |
| `sequence_by` | No | Column used to order records for SCD Type 2 change tracking. |

### Source-specific `table_configuration` options

| Option | Required | Default | Description |
|---|---|---|---|
| `resource_types` | No | All 14 default resources | Comma-separated list of FHIR resource types to ingest. Use this to limit ingestion to specific resource types. |
| `page_size` | No | `100` | Number of resources to request per page from the FHIR server (sent as the `_count` parameter). |
| `max_records_per_batch` | No | `1000` | Maximum number of records to fetch per pipeline trigger. Useful for controlling API usage and pipeline run duration. |

## Data Type Mapping

FHIR data types are mapped to Spark SQL types as follows:

| FHIR Type | Spark Type | Notes |
|---|---|---|
| `string`, `code`, `uri` | `StringType` | Includes identifiers, status codes, and reference URLs. |
| `instant`, `dateTime` | `TimestampType` | ISO 8601 timestamps with timezone (e.g., `2024-01-15T10:30:00+00:00`). |
| `date` | `StringType` | FHIR dates are partial (e.g., `2024-01-15` or `2024-01`) and do not map to a full timestamp. |
| `boolean` | `BooleanType` | Standard `true`/`false` values. |
| `decimal` | `DoubleType` | Numeric values such as observation quantities. |
| `integer`, `positiveInt` | `LongType` | Whole number values. |
| `Reference.reference` | `StringType` | FHIR reference strings (e.g., `Patient/123`). |

## How to Run

### Step 1: Clone/Copy the Source Connector Code

Follow the Lakeflow Community Connector UI, which will guide you through setting up a pipeline using the selected source connector code.

### Step 2: Configure Your Pipeline

Update the `pipeline_spec` in the main pipeline file (e.g., `ingest.py`). Here is an example configuration that ingests Patient and Observation resources:

```json
{
  "pipeline_spec": {
    "connection_name": "fhir_connection",
    "object": [
      {
        "table": {
          "source_table": "Patient",
          "table_configuration": {
            "page_size": "100",
            "max_records_per_batch": "1000"
          }
        }
      },
      {
        "table": {
          "source_table": "Observation",
          "table_configuration": {
            "page_size": "50",
            "max_records_per_batch": "500"
          }
        }
      },
      {
        "table": {
          "source_table": "Condition"
        }
      }
    ]
  }
}
```

To ingest only a subset of resource types, specify them via the `resource_types` table configuration option (comma-separated). If omitted, all 14 default resource types are available.

### Step 3: Run and Schedule the Pipeline

Run the pipeline using your standard Lakeflow / Databricks orchestration (e.g., a scheduled job or workflow).

- On the **first run**, all records for each configured resource type are fetched.
- On **subsequent runs**, only records with `lastUpdated` newer than the previous cursor are retrieved.

#### Best Practices

- **Start small**: Begin by ingesting one or two resource types (e.g., `Patient`, `Observation`) to validate your configuration and connectivity.
- **Use incremental sync**: The CDC ingestion mode minimizes API calls and data transfer on subsequent runs.
- **Set appropriate schedules**: Balance data freshness requirements with your FHIR server's rate limits and capacity.
- **Tune batch sizes for production EHRs**: Production FHIR servers (Epic, Cerner) may have rate limits. Use `page_size` and `max_records_per_batch` to control throughput.

#### Troubleshooting

**Server does not support `_lastUpdated`:**
If the FHIR server ignores the `_lastUpdated` filter, the connector will detect this and raise an error:
> "FHIR server appears to have ignored the '_lastUpdated' filter -- all returned records have lastUpdated <= the cursor."

This means the server does not support incremental sync via `_lastUpdated`. Contact your FHIR server administrator to confirm whether this search parameter is enabled. The `_lastUpdated` parameter is not mandatory in the FHIR specification, so some servers may not implement it.

**Authentication errors (401 / 403):**
- Verify that `auth_type` matches the authentication method your FHIR server supports.
- For `jwt_assertion`: confirm that the public key is registered with the server, the `client_id` is correct, and the `private_key_pem` includes the full PEM content with delimiters.
- For `client_secret`: confirm the `client_id` and `client_secret` are correct and not expired.
- Check that the `scope` parameter includes the necessary permissions for the resource types you are ingesting.
- Ensure the `token_url` is correct (check `/.well-known/smart-configuration` on your FHIR server).

**Empty results on incremental sync:**
This is expected behavior when no records have been created or modified since the last sync. The connector will return no new data and the cursor will remain unchanged.

**Rate limiting (429 responses):**
If the FHIR server returns HTTP 429 (Too Many Requests), the connector will retry with exponential backoff (up to 3 retries). If rate limiting persists:
- Reduce `page_size` to request fewer resources per page.
- Reduce `max_records_per_batch` to limit the total number of records per pipeline trigger.
- Increase the interval between scheduled pipeline runs.

**Connection timeouts:**
The connector uses a 60-second timeout for all FHIR API requests. If your server is slow to respond, check network connectivity between your Databricks workspace and the FHIR server.

**Resource type not found (404):**
Verify that the resource type name uses exact FHIR casing (e.g., `Patient`, not `patient` or `PATIENT`). Also confirm that the FHIR server supports the resource type you are trying to ingest.

## References

- FHIR R4 specification: https://hl7.org/fhir/R4/
- FHIR R4 Search: https://hl7.org/fhir/R4/search.html
- FHIR R4 Bundle: https://hl7.org/fhir/R4/bundle.html
- SMART Backend Services: https://www.hl7.org/fhir/smart-app-launch/backend-services.html
- HAPI FHIR public test server: https://hapi.fhir.org/baseR4
