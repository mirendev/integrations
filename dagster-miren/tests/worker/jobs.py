import os
import socket
import time

import psycopg2
from dagster import Definitions, IOManager, io_manager, job, op


class PostgresTestIOManager(IOManager):
    """Test-only artifact store, deliberately separate from worker filesystems."""

    def handle_output(self, context, obj):
        with (
            psycopg2.connect(os.environ["DAGSTER_TEST_PG_URL"]) as conn,
            conn.cursor() as cur,
        ):
            cur.execute(
                "INSERT INTO miren_test_outputs(run_id, step, value, hostname) VALUES (%s,%s,%s,%s)",
                (context.run_id, context.step_key, obj, socket.gethostname()),
            )

    def load_input(self, context):
        output = context.upstream_output
        with (
            psycopg2.connect(os.environ["DAGSTER_TEST_PG_URL"]) as conn,
            conn.cursor() as cur,
        ):
            cur.execute(
                "SELECT value FROM miren_test_outputs WHERE run_id=%s AND step=%s",
                (output.run_id, output.step_key),
            )
            return cur.fetchone()[0]


@io_manager
def shared_io(_):
    return PostgresTestIOManager()


@op(config_schema={"value": int, "delay": int, "fail": bool})
def compute(context):
    with (
        psycopg2.connect(os.environ["DAGSTER_TEST_PG_URL"]) as conn,
        conn.cursor() as cur,
    ):
        cur.execute(
            "INSERT INTO miren_test_starts(run_id, hostname) VALUES (%s,%s)",
            (context.run_id, socket.gethostname()),
        )
    time.sleep(context.op_config["delay"])
    if context.op_config["fail"]:
        raise RuntimeError("intentional integration test failure")
    return context.op_config["value"] * 7


@op
def dependent(value: int):
    return value + 3


@job(resource_defs={"io_manager": shared_io})
def arithmetic():
    dependent(compute())


defs = Definitions(jobs=[arithmetic])
