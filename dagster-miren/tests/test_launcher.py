import json
from unittest.mock import Mock

import pytest
import requests
from dagster import DagsterInstance, job, op
from dagster._core.code_pointer import ModuleCodePointer
from dagster._core.launcher.base import LaunchRunContext, WorkerStatus
from dagster._core.origin import RepositoryPythonOrigin
from dagster._grpc.types import ExecuteRunArgs
from dagster._serdes import deserialize_value
from dagster_miren import MirenRunLauncher
from dagster_miren.client import MirenClient
from dagster_miren.launcher import RUN_ID_TAG, SUBMISSION_TAG


@op
def noop():
    pass


@job
def sample_job():
    noop()


@pytest.fixture
def launched(tmp_path):
    with DagsterInstance.local_temp(str(tmp_path)) as instance:
        launcher = MirenRunLauncher("https://cluster", "workers", "app_version/v1")
        launcher.register_instance(instance)
        launcher.client = Mock()
        launcher.client.submit.return_value = "run/finite-worker"
        origin = RepositoryPythonOrigin(
            "/usr/local/bin/python", ModuleCodePointer("jobs", "defs", "/app")
        )
        run = instance.create_run_for_job(
            sample_job, job_code_origin=origin.get_job_origin("sample_job")
        )
        yield instance, launcher, run


def test_command_and_lost_response_recovery_preserve_version(launched):
    instance, launcher, run = launched
    requests_seen = []

    def lose_response(payload):
        assert (
            json.loads(instance.get_run_by_id(run.run_id).tags[SUBMISSION_TAG])
            == payload
        )
        requests_seen.append(payload)
        raise requests.Timeout("accepted but lost response")

    launcher.client.submit.side_effect = lose_response
    with pytest.raises(requests.Timeout):
        launcher.launch_run(LaunchRunContext(run, None))
    payload = requests_seen[0]
    args = deserialize_value(payload["command"][-1], ExecuteRunArgs)
    assert args.run_id == run.run_id
    assert args.set_exit_code_on_failure is True
    assert args.instance_ref is not None
    assert args.job_origin == run.job_code_origin
    recovered = MirenRunLauncher(
        "https://cluster", "workers", "app_version/new-deployment"
    )
    recovered.register_instance(instance)
    recovered.client = Mock()
    recovered.client.submit.return_value = "run/finite-worker"
    launcher = recovered
    launcher.launch_run(LaunchRunContext(instance.get_run_by_id(run.run_id), None))
    assert launcher.client.submit.call_args.args[0] == payload
    assert instance.get_run_by_id(run.run_id).tags[RUN_ID_TAG] == "run/finite-worker"
    assert not launcher.supports_resume_run


@pytest.mark.parametrize(
    "run_status,worker_status,expected",
    [
        ("pending", "pending", WorkerStatus.UNKNOWN),
        ("running", "pending", WorkerStatus.UNKNOWN),
        ("running", "not_ready", WorkerStatus.UNKNOWN),
        ("running", "running", WorkerStatus.RUNNING),
        ("running", "missing", WorkerStatus.NOT_FOUND),
        ("failed", "running", WorkerStatus.FAILED),
        ("succeeded", "dead", WorkerStatus.SUCCESS),
    ],
)
def test_health_startup_boundary(launched, run_status, worker_status, expected):
    instance, launcher, run = launched
    instance.add_run_tags(run.run_id, {RUN_ID_TAG: "run/finite-worker"})
    launcher.client.get.return_value = {
        "status": run_status,
        "worker_status": worker_status,
    }
    assert (
        launcher.check_run_worker_health(instance.get_run_by_id(run.run_id)).status
        == expected
    )


def test_cancellation_waits_for_teardown(launched):
    instance, launcher, run = launched
    instance.add_run_tags(run.run_id, {RUN_ID_TAG: "run/finite-worker"})
    launcher.client.get.side_effect = [
        {"status": "canceled", "worker_status": "running", "sandbox": "sandbox/one"},
        {"status": "canceled", "worker_status": "stopped", "sandbox": "sandbox/one"},
    ]
    assert launcher.terminate(run.run_id)
    assert launcher.client.get.call_count == 2
    assert instance.get_run_by_id(run.run_id).is_finished
    assert not launcher.terminate(run.run_id)


def test_cancellation_timeout_does_not_claim_teardown(launched):
    instance, launcher, run = launched
    instance.add_run_tags(run.run_id, {RUN_ID_TAG: "run/finite-worker"})
    launcher.cancellation_timeout = 1
    launcher.client.get.return_value = {
        "status": "canceled",
        "worker_status": "running",
        "sandbox": "sandbox/one",
    }
    assert not launcher.terminate(run.run_id)
    assert not instance.get_run_by_id(run.run_id).is_finished


def test_cancellation_before_start_has_no_worker(launched):
    instance, launcher, run = launched
    instance.add_run_tags(run.run_id, {RUN_ID_TAG: "run/finite-worker"})
    launcher.client.get.return_value = {"status": "canceled", "worker_status": "none"}
    assert launcher.terminate(run.run_id)
    assert instance.get_run_by_id(run.run_id).is_finished


def test_http_retry_has_identical_request_and_keeps_tls_verification(monkeypatch):
    monkeypatch.setenv("TEST_MIREN_TOKEN", "test-only-token")
    response = Mock()
    response.json.return_value = {"id": "run/one"}
    request = Mock(side_effect=[requests.ConnectionError("response lost"), response])
    monkeypatch.setattr(requests, "request", request)
    client = MirenClient("https://cluster", token_env="TEST_MIREN_TOKEN")
    payload = {
        "app": "workers",
        "request_id": "one",
        "version": "app_version/v1",
        "command": ["true"],
    }
    assert client.submit(payload) == "run/one"
    assert request.call_args_list[0] == request.call_args_list[1]
    assert request.call_args.kwargs["verify"] is True
    with pytest.raises(ValueError):
        MirenClient("http://cluster")
