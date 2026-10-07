import json
import time

from dagster import Field, IntSource, StringSource
from dagster._core.launcher.base import CheckRunHealthResult, RunLauncher, WorkerStatus
from dagster._core.storage.dagster_run import DagsterRunStatus
from dagster._grpc.types import ExecuteRunArgs
from dagster._serdes import ConfigurableClass

from .client import MirenClient

RUN_ID_TAG = "miren/run_id"
SUBMISSION_TAG = "miren/submission"


class MirenRunLauncher(RunLauncher, ConfigurableClass):
    def __init__(
        self,
        endpoint,
        app,
        version,
        task="dagster",
        token_env=None,
        ca_cert=None,
        client_cert=None,
        client_key=None,
        cancellation_timeout=60,
        inst_data=None,
    ):
        self._inst_data = inst_data
        self.app, self.version, self.task = app, version, task
        self.cancellation_timeout = cancellation_timeout
        self.client = MirenClient(endpoint, token_env, ca_cert, client_cert, client_key)
        super().__init__()

    @property
    def inst_data(self):
        return self._inst_data

    @classmethod
    def config_type(cls):
        return {
            "endpoint": StringSource,
            "app": StringSource,
            "version": StringSource,
            "task": Field(StringSource, default_value="dagster", is_required=False),
            "token_env": Field(str, is_required=False),
            "ca_cert": Field(StringSource, is_required=False),
            "client_cert": Field(StringSource, is_required=False),
            "client_key": Field(StringSource, is_required=False),
            "cancellation_timeout": Field(
                IntSource, default_value=60, is_required=False
            ),
        }

    @classmethod
    def from_config_value(cls, inst_data, config_value):
        return cls(inst_data=inst_data, **config_value)

    def launch_run(self, context):
        run = self._instance.get_run_by_id(context.dagster_run.run_id)
        if run.is_finished or run.status == DagsterRunStatus.CANCELING:
            return
        if SUBMISSION_TAG in run.tags:
            payload = json.loads(run.tags[SUBMISSION_TAG])
        else:
            if context.job_code_origin is None:
                raise ValueError("Miren requires a reconstructable job code origin")
            payload = {
                "app": self.app,
                "version": self.version,
                "task": self.task,
                "request_id": "dagster-run-" + run.run_id,
                "command": list(
                    ExecuteRunArgs(
                        job_origin=context.job_code_origin,
                        run_id=run.run_id,
                        instance_ref=self._instance.get_ref(),
                        set_exit_code_on_failure=True,
                    ).get_command_args()
                ),
            }
            # Persist the exact request before sending. A restarted daemon can
            # recover the identity even if the response (or next tag write) is lost.
            self._instance.add_run_tags(
                run.run_id, {SUBMISSION_TAG: json.dumps(payload)}
            )
        worker_id = self.client.submit(payload)
        self._instance.add_run_tags(run.run_id, {RUN_ID_TAG: worker_id})
        self._instance.report_engine_event(
            f"Submitted finite Miren worker {worker_id}", run
        )
        if (
            self._instance.get_run_by_id(run.run_id).status
            == DagsterRunStatus.CANCELING
        ):
            self.client.cancel(worker_id)

    def _worker_id(self, run):
        if RUN_ID_TAG in run.tags:
            return run.tags[RUN_ID_TAG]
        if SUBMISSION_TAG in run.tags:
            worker_id = self.client.submit(json.loads(run.tags[SUBMISSION_TAG]))
            self._instance.add_run_tags(run.run_id, {RUN_ID_TAG: worker_id})
            return worker_id
        return None

    def terminate(self, run_id):
        run = self._instance.get_run_by_id(run_id)
        if run is None or run.is_finished:
            return False
        self._instance.report_run_canceling(run)
        worker_id = self._worker_id(run)
        if worker_id is None:
            return False
        self.client.cancel(worker_id)
        deadline = time.monotonic() + self.cancellation_timeout
        while time.monotonic() < deadline:
            info = self.client.get(worker_id)
            if info["status"] in {
                "canceled",
                "failed",
                "succeeded",
                "timed_out",
                "skipped",
            } and info["worker_status"] in {"dead", "stopped", "missing", "none"}:
                current = self._instance.get_run_by_id(run_id)
                if not current.is_finished:
                    self._instance.report_run_canceled(current)
                return True
            time.sleep(0.5)
        return False

    @property
    def supports_check_run_worker_health(self):
        return True

    def check_run_worker_health(self, run):
        try:
            worker_id = self._worker_id(run)
            if worker_id is None:
                return CheckRunHealthResult(
                    WorkerStatus.NOT_FOUND, "No Miren submission recorded"
                )
            info = self.client.get(worker_id)
        except Exception as exc:
            return CheckRunHealthResult(
                WorkerStatus.UNKNOWN,
                f"Miren lookup unavailable: {type(exc).__name__}",
                transient=True,
            )
        status = info["status"]
        if status == "succeeded":
            health = WorkerStatus.SUCCESS
        elif status in {"failed", "canceled", "timed_out", "skipped"}:
            health = WorkerStatus.FAILED
        elif info["worker_status"] == "running":
            health = WorkerStatus.RUNNING
        elif info["worker_status"] in {"dead", "stopped", "missing"}:
            health = WorkerStatus.NOT_FOUND
        else:
            health = WorkerStatus.UNKNOWN
        return CheckRunHealthResult(
            health,
            f"Miren run={status}, worker={info['worker_status']}",
            transient=health == WorkerStatus.UNKNOWN,
            run_worker_id=worker_id,
        )
