# FHIR Connector Code Review

## Rules

1. **`profiles/__init__.py` over-exposes internal symbols.**
   - `_EXTRACTOR_REGISTRY`, `_SCHEMA_REGISTRY`, and `_chain` are never imported outside `fhir_profile_registry.py` — remove them from the `__init__.py` re-exports and `__all__`.
   - `_COMMON_FIELDS` is only used by `test_fhir_profiles_registry.py` — that test should import it directly from `fhir_profile_registry` instead of going through the package's public API.
   - The public API of `profiles/` should be limited to: `get_schema`, `extract`, `register`, `PROFILE_CHAIN`, `FALLBACK_SCHEMA`.
