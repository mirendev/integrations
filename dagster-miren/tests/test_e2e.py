"""Opt-in real Dagster daemon + Miren finite worker test (see README)."""

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor

import psycopg2
import pytest
import requests
from dagster import DagsterInstance
from dagster._core.launcher.base import WorkerStatus
from dagster._core.storage.dagster_run import DagsterRunStatus
from dagster._core.workspace.context import WorkspaceProcessContext
from dagster._core.workspace.load_target import WorkspaceFileTarget
from dagster_miren.launcher import RUN_ID_TAG, SUBMISSION_TAG

pytestmark = pytest.mark.skipif(
    os.environ.get("DAGSTER_MIREN_E2E") != "1",
    reason="requires local E2E setup and daemon",
)


def wait_for(predicate, timeout=180):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = predicate()
        if result:
            return result
        time.sleep(0.5)
    raise AssertionError("Timed out waiting for local integration state")


def query(sql, params=()):
    with (
        psycopg2.connect(os.environ["DAGSTER_TEST_PG_URL"]) as conn,
        conn.cursor() as cur,
    ):
        cur.execute(sql, params)
        return cur.fetchall()


def test_real_concurrent_runs_failure_cancellation_and_replay(monkeypatch):
    with (
        DagsterInstance.get() as instance,
        WorkspaceProcessContext(
            instance,
            WorkspaceFileTarget(
                [os.path.join(os.environ["DAGSTER_HOME"], "workspace.yaml")]
            ),
        ) as workspace,
    ):
        request = workspace.create_request_context()
        location = request.get_code_location("miren-test")
        repo = next(iter(location.get_repositories().values()))
        remote_job = repo.get_full_job("arithmetic")

        def submit(value, delay, fail=False):
            run = instance.create_run(
                job_name="arithmetic",
                run_config={
                    "ops": {
                        "compute": {
                            "config": {"value": value, "delay": delay, "fail": fail}
                        }
                    }
                },
                job_snapshot=remote_job.job_snapshot,
                remote_job_origin=remote_job.get_remote_origin(),
                job_code_origin=remote_job.get_python_origin(),
                run_id=None,
                status=DagsterRunStatus.NOT_STARTED,
                tags={},
                root_run_id=None,
                parent_run_id=None,
                step_keys_to_execute=None,
                execution_plan_snapshot=None,
                parent_job_snapshot=None,
                asset_selection=None,
                asset_check_selection=None,
                resolved_op_selection=None,
                op_selection=None,
                asset_graph=None,
            )
            instance.submit_run(run.run_id, request)
            return run.run_id

        first, second = submit(5, 20), submit(11, 20)
        wait_for(
            lambda: (
                len(
                    query(
                        "SELECT run_id FROM miren_test_starts WHERE run_id IN (%s,%s)",
                        (first, second),
                    )
                )
                == 2
            )
        )
        launcher = instance.run_launcher
        workers = [
            launcher.client.get(instance.get_run_by_id(r).tags[RUN_ID_TAG])
            for r in (first, second)
        ]
        assert all(
            w["status"] == "running" and w["worker_status"] == "running"
            for w in workers
        )
        assert workers[0]["sandbox"] != workers[1]["sandbox"]
        wait_for(
            lambda: all(instance.get_run_by_id(r).is_finished for r in (first, second))
        )
        assert all(
            instance.get_run_by_id(r).status == DagsterRunStatus.SUCCESS
            for r in (first, second)
        )
        assert query(
            "SELECT step,value FROM miren_test_outputs WHERE run_id=%s ORDER BY step",
            (first,),
        ) == [("compute", 35), ("dependent", 38)]
        assert query(
            "SELECT step,value FROM miren_test_outputs WHERE run_id=%s ORDER BY step",
            (second,),
        ) == [("compute", 77), ("dependent", 80)]

        # Discard a real accepted response, then replay it concurrently after
        # terminal completion. This must not relaunch or reset the worker.
        payload = json.loads(instance.get_run_by_id(first).tags[SUBMISSION_TAG])
        wait_for(
            lambda: (
                launcher.client.get(instance.get_run_by_id(first).tags[RUN_ID_TAG])[
                    "status"
                ]
                == "succeeded"
            )
        )
        launcher.client.submit(payload)
        with ThreadPoolExecutor(max_workers=8) as pool:
            ids = list(pool.map(lambda _: launcher.client.submit(payload), range(8)))
        assert len(set(ids)) == 1
        assert ids[0] == instance.get_run_by_id(first).tags[RUN_ID_TAG]
        assert query(
            "SELECT count(*) FROM miren_test_starts WHERE run_id=%s", (first,)
        ) == [(1,)]
        assert launcher.client.get(ids[0])["status"] == "succeeded"
        with pytest.raises(requests.HTTPError):
            launcher.client.submit({**payload, "command": ["false"]})

        failed = submit(13, 0, fail=True)
        wait_for(lambda: instance.get_run_by_id(failed).is_finished)
        assert instance.get_run_by_id(failed).status == DagsterRunStatus.FAILURE
        failed_id = instance.get_run_by_id(failed).tags[RUN_ID_TAG]
        wait_for(lambda: launcher.client.get(failed_id)["status"] == "failed")
        assert launcher.client.get(failed_id)["exit_code"] != 0
        assert any(
            e.dagster_event and e.dagster_event.is_step_failure
            for e in instance.all_logs(failed)
        )

        canceled = submit(17, 120)
        wait_for(
            lambda: query(
                "SELECT run_id FROM miren_test_starts WHERE run_id=%s", (canceled,)
            )
        )
        assert launcher.terminate(canceled)
        assert instance.get_run_by_id(canceled).status == DagsterRunStatus.CANCELED
        cancel_info = launcher.client.get(
            instance.get_run_by_id(canceled).tags[RUN_ID_TAG]
        )
        assert cancel_info["status"] == "canceled"
        assert cancel_info["worker_status"] in {"stopped", "dead", "missing"}
        assert (
            query("SELECT value FROM miren_test_outputs WHERE run_id=%s", (canceled,))
            == []
        )

        # Force a capacity boundary so pending is deterministic, not a race
        # against a fast local image pull. Drop the first accepted HTTP response.
        probe_payload = {
            "app": launcher.app,
            "version": launcher.version,
            "task": "startup_probe",
            "command": ["sleep", "120"],
            "request_id": "probe-" + first,
        }
        real_request = requests.request
        lost = []

        def drop_accepted_response(*args, **kwargs):
            response = real_request(*args, **kwargs)
            if (kwargs.get("json") or {}).get("request_id") == probe_payload[
                "request_id"
            ] and not lost:
                response.raise_for_status()
                lost.append(response.json()["id"])
                raise requests.ConnectionError("test discarded accepted response")
            return response

        monkeypatch.setattr(requests, "request", drop_accepted_response)
        probe = launcher.client.submit(probe_payload)
        assert lost == [probe]
        pending = None
        try:
            wait_for(lambda: launcher.client.get(probe)["worker_status"] == "running")
            with ThreadPoolExecutor(max_workers=8) as pool:
                pending_ids = list(
                    pool.map(
                        lambda _: launcher.client.submit(
                            {**probe_payload, "request_id": "pending-" + first}
                        ),
                        range(8),
                    )
                )
            assert len(set(pending_ids)) == 1
            pending = pending_ids[0]
            info = launcher.client.get(pending)
            assert info["status"] == "pending"
            assert info["worker_status"] == "pending"
            assert not info.get("sandbox")
            health_run = instance.get_run_by_id(first).with_tags({RUN_ID_TAG: pending})
            assert (
                launcher.check_run_worker_health(health_run).status
                == WorkerStatus.UNKNOWN
            )
            launcher.client.cancel(pending)
            wait_for(lambda: launcher.client.get(pending)["status"] == "canceled")
            assert launcher.client.get(pending)["worker_status"] == "none"
        finally:
            launcher.client.cancel(probe)
            if pending:
                launcher.client.cancel(pending)
            wait_for(
                lambda: (
                    launcher.client.get(probe)["worker_status"]
                    in {"dead", "stopped", "missing"}
                )
            )
        print(
            json.dumps(
                {
                    "concurrent_runs": [first, second],
                    "sandboxes": [w["sandbox"] for w in workers],
                    "results": [38, 80],
                    "failure": failed,
                    "cancellation": canceled,
                    "duplicate_responses": len(ids),
                },
                indent=2,
            )
        )
