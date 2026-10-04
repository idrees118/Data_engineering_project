# 0002. Keep payloads as raw JSON in bronze; type them in silver

**Status:** accepted

## Context
Producers evolve their schemas. If ingestion flattens payloads into typed columns, every schema
change forces an ingestion deploy, and a wrong interpretation means re-reading the stream.

## Decision
Ingestion validates against the event contract but stores `payload` as the original JSON text.
Typing happens in dbt staging models.

## Consequences
* A bug in parsing is fixed by changing SQL and rebuilding; bronze never needs to be re-ingested.
* New optional fields flow into bronze untouched and can be exposed later.
* Bronze is a little larger and slower to scan than a flattened table, which is acceptable
  because only silver reads it, once per run.
