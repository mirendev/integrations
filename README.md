# Miren integrations

Adapters for running existing tools on Miren's execution infrastructure.

- [Dagster](./dagster-miren/README.md): a Python `MirenRunLauncher` that launches
  each Dagster run on a finite Miren worker. Includes installation and instance
  configuration, unit tests, and a real local Dagster + Miren E2E test.

Each integration lives in its own directory with its package metadata and tests.
The Dagster adapter requires the orchestrator submission API in
[Miren Runtime PR #1328](https://github.com/mirendev/runtime/pull/1328); it is not yet published to PyPI.
