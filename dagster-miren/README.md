# Dagster on Miren

`dagster-miren` launches **one finite Miren Run per Dagster run**. Dagster's
queued coordinator admits runs; the run worker uses the job's existing executor
(normally multiprocess). Jobs, schedules and sensors do not need changes to
scale across runs. Miren executes commands, not HTTP-triggered simulated work.

This is an initial, unreleased integration, pinned to Dagster **1.12.21** because
it uses Dagster's internal launcher/command interfaces. It requires the Runtime
changes in [Miren Runtime PR #1328](https://github.com/mirendev/runtime/pull/1328)
(`Runs.submitRun` and REST bindings); older released
servers do not support it. It has not been published to PyPI.

## Installation and code-image mapping

From this integrations checkout, install into the daemon, webserver and worker image:

```sh
pip install ./dagster-miren
# Add the storage libraries used by your instance and IO manager, for example:
pip install dagster-postgres==0.28.21 'sqlalchemy<2.1'
```

You can build a wheel with `pip wheel --no-deps ./dagster-miren`
and install that same wheel into all three environments. Package installation
alone does not deploy the worker image.

Deploy a task-only Miren app containing your user code and its dependencies:

```toml
# .miren/app.toml in the worker image's deployment context
name = "analytics-workers"
web = false

[build]
dockerfile = "Dockerfile"

[tasks.dagster]
command = "true" # overridden by ExecuteRunArgs for each execution
max_concurrent = 20
```

`app` and `version` below are the explicit code-image mapping. `version` must be
the full immutable `app_version/<name>` entity ID reported by a deployment,
not an image tag, short ID or `latest`. The server validates that it belongs to
the configured app. Configure a new version only after validating its user
code against the code server. Already submitted work retains its original
version and exact command, even after redeployment.

This first launcher has **one app/version mapping per Dagster instance**. All
code locations served by that instance must be present in that image, or use
separate Dagster instances for separate worker images. Per-code-location
routing is not implemented. Use matching user-code images for code servers
and workers: Python executable paths, importable modules, working directory,
Dagster version and dependency versions must match. The launcher uses the
reconstructable origin supplied by Dagster without rewriting it.

## Instance configuration

```yaml
# $DAGSTER_HOME/dagster.yaml
storage:
  postgres:
    postgres_url:
      env: DAGSTER_POSTGRES_URL

run_coordinator:
  module: dagster._core.run_coordinator
  class: QueuedRunCoordinator
  config:
    max_concurrent_runs: 20

run_launcher:
  module: dagster_miren
  class: MirenRunLauncher
  config:
    endpoint: https://cluster.example.com:8443
    app: analytics-workers
    version: app_version/analytics-workers-vREPLACE_WITH_DEPLOYED_VERSION
    task: dagster
    token_env: MIREN_DAGSTER_TOKEN
    # For a private cluster CA:
    # ca_cert: /run/secrets/miren-ca.crt
    # Alternatively authenticate with an issued client certificate:
    # client_cert: /run/secrets/miren-client.crt
    # client_key: /run/secrets/miren-client.key
    cancellation_timeout: 60

run_monitoring:
  enabled: true
  max_resume_run_attempts: 0
  start_timeout_seconds: 600
```

Use the API/REST listener address, not the app's ingress URL. TLS certificate
verification is mandatory. `ca_cert`, `client_cert` and `client_key` accept
Dagster `StringSource` values (including environment references). `token_env`
names an environment variable containing a valid API bearer JWT, **not** the
sandbox token-endpoint secret. The client rereads that variable on each request;
it does not obtain or refresh JWTs itself. Supply/rotate credentials through
your deployment's credential provider. Client cert/key must be supplied
together. Grant the identity the `runs` interface actions `submitrun`, `getrun`,
`listruns` and `cancelrun`, scoped to the worker app where possible.

Never put credentials literally in YAML: Dagster serializes its instance
reference into the execution command. The launcher also persists that request
in `miren/submission`, then writes `miren/run_id`. Treat these tags as reserved
execution metadata. Their durable presence allows the same submission to be
retrieved after a lost response or daemon restart. Submissions require a declared
non-console task (the launcher defaults to `dagster`). Runtime's default history
retention keeps these Runs for at least **seven days after termination**;
deduplication is guaranteed only while the record exists. Do not recover/replay
submissions past that horizon unless you have extended Runtime's retention.
Deleting a Run manually also removes its deduplication record.

## Shared storage and health

Every worker needs network access to the same Dagster run/event database and
the same durable artifact store. Set the database environment variable in
both Dagster services and the Miren worker app. Use an object-store-backed IO
manager or other genuinely shared artifact storage; the workers do not share
local filesystems, and finite Miren Runs do not mount the app's Miren disks.
Configure a shared compute-log manager as well if you need worker stdout/stderr
in the Dagster UI. The E2E fixture uses Postgres for artifacts and Dagster events;
its local compute logs are deliberately not proof of shared compute-log support.

Submission queues at Miren task capacity, unlike interactive `miren app run`.
Set Dagster concurrency and startup timeout to account for image pulls and
Miren's capacity queue. Miren admits a Run before its sandbox is ready:
`worker_status=pending/not_ready` produces Dagster `UNKNOWN`, never `RUNNING`.
Only an observed running sandbox reports `RUNNING`. Lookup errors report
`UNKNOWN`, terminal failures report `FAILED`, and successful process exit
reports `SUCCESS`. Dagster owns the job's final result via its shared events.

Dagster 1.12.21's monitor treats `UNKNOWN` on an already started run as failure;
the `transient` result flag does not defer that decision. An API outage can
therefore fail monitoring of a still-live worker. No worker replacement is
attempted. Resume/replacement stays disabled until recovery is validated.
Cancellation records a request and waits up to `cancellation_timeout` for a
terminal Run plus stopped/dead/missing sandbox before acknowledging success.
A timeout returns false; cancellation is not undone and may finish later.

## Tests

Unit tests:

```sh
uv venv .venv
uv pip install --python .venv/bin/python -e './dagster-miren[test]'
.venv/bin/pytest dagster-miren/tests/test_launcher.py -q
```

For the real E2E test, use a separate [Miren Runtime checkout](https://github.com/mirendev/runtime)
containing the Runs API changes. Start that checkout's **local** iso dev
environment and server first (`make dev-start`, then `make dev-server-start`
outside an orb). The setup script defaults to the `repo-dev-repo-shell` and
`repo-dev-repo_postgres` container names; pass `--shell-container` and
`--postgres-container` if yours differ. Set `ISO_SESSION` if your Runtime dev
environment uses a non-default session. The script explicitly deploys to
`local`, not your default remote cluster.
The fixture creates the disposable `dagster-miren-test` app, a
`dagster_miren_test` database and a `dagster-miren-code` Docker code server.
It stages builds, certificates and instance config under the Runtime checkout's
gitignored `tmp/`, where iso can access the deployment context.

```sh
export MIREN_RUNTIME_DIR=/absolute/path/to/runtime
.venv/bin/python dagster-miren/tests/prepare_e2e.py --runtime-dir "$MIREN_RUNTIME_DIR"
source "$MIREN_RUNTIME_DIR/tmp/dagster-miren-e2e/env.sh"
# In a persistent service/terminal:
.venv/bin/dagster-daemon run -w "$DAGSTER_HOME/workspace.yaml"
# In another terminal with the same environment:
.venv/bin/pytest dagster-miren/tests/test_e2e.py -s
```

In an Amp orb use `amp orb service start` for the daemon/server; use absolute
paths and ensure the service user can access Docker. Stopping a Docker exec
client does not necessarily stop the Miren server process in its container:
stop the server there before launching a replacement.

The real test checks concurrent queued runs on distinct Miren sandboxes,
dependent-step results 38 and 80 in shared storage, recorded Dagster failure
events/nonzero worker exit, cancellation through sandbox teardown, terminal
replay without reexecution, conflicting-request rejection, eight-way fresh
submission deduplication, a deliberately dropped accepted HTTP response, and
pending health at a deterministic capacity boundary. Unit tests additionally
cover the admitted-but-not-started health boundary and request recovery before
the worker-ID tag was saved.

After stopping the daemon, remove the disposable code server with
`docker rm -f dagster-miren-code`; remove `$MIREN_RUNTIME_DIR/tmp/dagster-miren-e2e`
when you no longer need its local credentials/config. The local app/Run history and test
database can be retained for inspection or removed through their respective
local management tools.

## Scope

Cross-run scale-out is implemented and end-to-end tested. There is **no
`miren_executor` or per-step Miren delegation** here: dependent steps run within
each finite run worker using Dagster's multiprocess executor. No claim is made
about outputs/retries across separate step workers, daemon-crash recovery,
runner-loss recovery, automatic host provisioning or CPU/memory-aware host
placement. This fans out onto the existing Miren runner fleet; it does not
create machines.
