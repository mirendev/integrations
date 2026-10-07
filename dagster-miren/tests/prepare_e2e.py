"""Prepare an isolated local test against the repo's running iso dev cluster.

Run with the integration's Python environment and --runtime-dir pointing to
a Runtime checkout with a running local iso dev cluster.
Only local disposable apps/database/containers are created. No default remote
cluster is used: every CLI operation explicitly selects local.
"""

import argparse
import json
import shutil
import subprocess
from pathlib import Path

import psycopg2
import yaml

PACKAGE = Path(__file__).resolve().parents[1]


def run(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()


def ip(name):
    return run(
        "docker",
        "inspect",
        name,
        "--format",
        "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime-dir", type=Path, required=True)
    parser.add_argument("--shell-container", default="repo-dev-repo-shell")
    parser.add_argument("--postgres-container", default="repo-dev-repo_postgres")
    args = parser.parse_args()
    ROOT = args.runtime_dir.resolve()
    if not (ROOT / "hack/dev-exec").is_file():
        parser.error("--runtime-dir must point to a Miren Runtime checkout")
    STATE = ROOT / "tmp/dagster-miren-e2e"
    STATE.mkdir(parents=True, exist_ok=True)
    pg_host, runtime_host = ip(args.postgres_container), ip(args.shell_container)
    pg_url = f"postgresql://cloud:cloud@{pg_host}:5432/dagster_miren_test"
    with psycopg2.connect(f"postgresql://cloud:cloud@{pg_host}:5432/cloud") as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname='dagster_miren_test'")
            if cur.fetchone() is None:
                cur.execute("CREATE DATABASE dagster_miren_test")
    with psycopg2.connect(pg_url) as conn, conn.cursor() as cur:
        cur.execute(
            "CREATE TABLE IF NOT EXISTS miren_test_starts(run_id TEXT PRIMARY KEY, hostname TEXT NOT NULL, at TIMESTAMPTZ DEFAULT now())"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS miren_test_outputs(run_id TEXT, step TEXT, value INTEGER, hostname TEXT, PRIMARY KEY(run_id,step))"
        )
    build = STATE / "build"
    build.mkdir(exist_ok=True)
    for name in ("Dockerfile", "jobs.py", ".miren"):
        src, dst = PACKAGE / "tests/worker" / name, build / name
        if src.is_dir():
            shutil.copytree(src, dst, dirs_exist_ok=True)
        else:
            shutil.copy2(src, dst)
    shutil.copy2(PACKAGE / "pyproject.toml", build)
    shutil.copytree(
        PACKAGE / "dagster_miren",
        build / "dagster_miren",
        dirs_exist_ok=True,
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    summary = STATE / "deploy.json"
    subprocess.run(
        [
            "./hack/dev-exec",
            "m",
            "deploy",
            "-C",
            "local",
            "-d",
            "/src/" + str(build.relative_to(ROOT)),
            "-e",
            "DAGSTER_TEST_PG_URL=" + pg_url,
            "-f",
            "-q",
            "--summary-json",
            "/src/" + str(summary.relative_to(ROOT)),
        ],
        cwd=ROOT,
        check=True,
    )
    version = "app_version/" + json.loads(summary.read_text())["app_version"]
    config = yaml.safe_load(
        run(
            "docker",
            "exec",
            args.shell_container,
            "cat",
            "/home/dev/.config/miren/clientconfig.yaml",
        )
    )
    cluster = config["clusters"]["local"]
    for field, filename in (
        ("ca_cert", "ca.crt"),
        ("client_cert", "client.crt"),
        ("client_key", "client.key"),
    ):
        path = STATE / filename
        path.write_text(cluster[field])
        path.chmod(0o600)
    home = STATE / "home"
    home.mkdir(exist_ok=True)
    launcher = {
        "endpoint": f"https://{runtime_host}:8443",
        "app": "dagster-miren-test",
        "version": version,
        "ca_cert": str(STATE / "ca.crt"),
        "client_cert": str(STATE / "client.crt"),
        "client_key": str(STATE / "client.key"),
    }
    (home / "dagster.yaml").write_text(
        yaml.safe_dump(
            {
                "storage": {
                    "postgres": {"postgres_url": {"env": "DAGSTER_TEST_PG_URL"}}
                },
                "run_launcher": {
                    "module": "dagster_miren",
                    "class": "MirenRunLauncher",
                    "config": launcher,
                },
                "run_coordinator": {
                    "module": "dagster._core.run_coordinator",
                    "class": "QueuedRunCoordinator",
                    "config": {"max_concurrent_runs": 4},
                },
                "run_monitoring": {
                    "enabled": True,
                    "max_resume_run_attempts": 0,
                    "poll_interval_seconds": 2,
                },
                "telemetry": {"enabled": False},
            }
        )
    )
    subprocess.run(
        ["docker", "build", "-q", "-t", "dagster-miren-code:test", str(build)],
        check=True,
    )
    # Code server is a persistent local Docker service, independent of Amp's
    # process cgroup, and uses the same interpreter/module paths as workers.
    existing = run(
        "docker",
        "ps",
        "-a",
        "--filter",
        "name=^dagster-miren-code$",
        "--format",
        "{{.Names}}",
    )
    if existing:
        run("docker", "rm", "-f", "dagster-miren-code")
    run(
        "docker",
        "run",
        "-d",
        "--name",
        "dagster-miren-code",
        "--restart",
        "unless-stopped",
        "-e",
        "DAGSTER_TEST_PG_URL=" + pg_url,
        "dagster-miren-code:test",
    )
    (home / "workspace.yaml").write_text(
        yaml.safe_dump(
            {
                "load_from": [
                    {
                        "grpc_server": {
                            "host": ip("dagster-miren-code"),
                            "port": 4000,
                            "location_name": "miren-test",
                        }
                    }
                ]
            }
        )
    )
    (STATE / "env.sh").write_text(
        f"export DAGSTER_HOME={home}\nexport DAGSTER_TEST_PG_URL={pg_url}\nexport DAGSTER_MIREN_E2E=1\n"
    )
    print(
        f"Prepared local E2E. Source {STATE}/env.sh, start dagster-daemon run -w {home}/workspace.yaml, then pytest tests/test_e2e.py -s."
    )
