"""
Unit tests for airflow/dags/main_orchestration_dag.py
"""

import sys
import types
import unittest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Stub every heavy import before the module loads
# ---------------------------------------------------------------------------

def _make_stub(name):
    mod = types.ModuleType(name)
    sys.modules[name] = mod
    return mod


def _stub_airflow():
    dag_mod              = _make_stub("airflow")
    dag_cls              = MagicMock()
    dag_cls.__enter__    = MagicMock(return_value=MagicMock())
    dag_cls.__exit__     = MagicMock(return_value=False)
    dag_mod.DAG          = dag_cls
    sys.modules["airflow"] = dag_mod

    ops_python  = _make_stub("airflow.operators.python")
    ops_python.PythonOperator = MagicMock()

    spark_op = _make_stub("airflow.providers.apache.spark.operators.spark_submit")
    spark_op.SparkSubmitOperator = MagicMock()

    email_op = _make_stub("airflow.operators.email")
    email_op.EmailOperator = MagicMock()

    trigger = _make_stub("airflow.utils.trigger_rule")
    trigger.TriggerRule = MagicMock()


def _stub_third_party():
    urllib3_stub                  = _make_stub("urllib3")
    urllib3_stub.disable_warnings = lambda *a, **kw: None

    urllib3_exc                        = _make_stub("urllib3.exceptions")
    urllib3_exc.InsecureRequestWarning = Warning
    sys.modules["urllib3.exceptions"]  = urllib3_exc
    urllib3_stub.exceptions            = urllib3_exc

    nipyapi            = _make_stub("nipyapi")
    nipyapi.config     = MagicMock()
    nipyapi.security   = MagicMock()
    nipyapi.canvas     = MagicMock()
    nipyapi.nifi       = MagicMock()
    sys.modules["nipyapi"] = nipyapi

    docker_mod              = _make_stub("docker")
    docker_mod.from_env     = MagicMock()
    docker_mod.errors       = MagicMock()
    docker_mod.errors.NotFound = type("NotFound", (Exception,), {})
    sys.modules["docker"] = docker_mod

    requests_stub     = _make_stub("requests")
    requests_stub.get = MagicMock()
    sys.modules["requests"] = requests_stub


_stub_airflow()
_stub_third_party()

import importlib.util
import pathlib
import os

DAG_PATH = pathlib.Path(__file__).parent.parent / "airflow" / "dags" / "main_orchestration_dag.py"

with patch.dict(os.environ, {
    "NIFI_API_URL":          "http://nifi:8443/nifi-api",
    "NIFI_USERNAME":         "admin",
    "NIFI_PASSWORD":         "secret",
    "NIFI_PROCESSOR_GROUP":  "INGESTION-GROUP",
    "ALERT_EMAIL":           "team@example.com",
    "HDFS_NAMENODE":         "hdfs://namenode:9000",
    "NIFI_WARMUP":           "0",
    "NIFI_POLL_INTERVAL":    "1",
    "NIFI_POLL_TIMEOUT":     "10",
    "NIFI_STABLE_CHECKS":    "2",
}):
    spec   = importlib.util.spec_from_file_location("main_orchestration_dag", DAG_PATH)
    bcp    = importlib.util.module_from_spec(spec)
    sys.modules["main_orchestration_dag"] = bcp
    spec.loader.exec_module(bcp)

import nipyapi as _nipyapi
import docker   as _docker
import requests as _requests


# ===========================================================================
# _login_nifi
# ===========================================================================

class TestLoginNifi(unittest.TestCase):

    @patch.object(_nipyapi.security, "service_login")
    def test_sets_host_and_calls_login(self, mock_login):
        bcp._login_nifi()
        self.assertEqual(_nipyapi.config.nifi_config.host, bcp.NIFI_API_URL)
        mock_login.assert_called_once_with(
            service="nifi",
            username=bcp.NIFI_USERNAME,
            password=REDACTED_CREDENTIAL
        )

    @patch.object(_nipyapi.security, "service_login", side_effect=RuntimeError("auth failed"))
    def test_propagates_login_error(self, _):
        with self.assertRaises(RuntimeError):
            bcp._login_nifi()


# ===========================================================================
# kick_off_nifi
# ===========================================================================

class TestKickOffNifi(unittest.TestCase):

    def _make_pg(self, pg_id="pg-001"):
        pg    = MagicMock()
        pg.id = pg_id
        return pg

    @patch.object(_nipyapi.canvas, "schedule_process_group")
    @patch.object(_nipyapi.canvas, "get_process_group")
    @patch.object(bcp, "_login_nifi")
    def test_starts_group_by_env_name(self, mock_login, mock_get, mock_schedule):
        pg = self._make_pg()
        mock_get.return_value = pg
        bcp.kick_off_nifi()
        mock_get.assert_called_once_with(bcp.NIFI_GROUP, identifier_type="name")
        mock_schedule.assert_called_once_with(pg.id, True)

    @patch.object(_nipyapi.canvas, "schedule_process_group")
    @patch.object(_nipyapi.canvas, "get_process_group")
    @patch.object(bcp, "_login_nifi")
    def test_uses_ctx_group_name_override(self, mock_login, mock_get, mock_schedule):
        pg = self._make_pg("pg-override")
        mock_get.return_value = pg
        bcp.kick_off_nifi(group_name="custom-group")
        mock_get.assert_called_once_with("custom-group", identifier_type="name")


# ===========================================================================
# stop_nifi
# ===========================================================================

class TestStopNifi(unittest.TestCase):

    @patch.object(_nipyapi.canvas, "schedule_process_group")
    @patch.object(_nipyapi.canvas, "get_process_group")
    @patch.object(bcp, "_login_nifi")
    def test_stops_group(self, mock_login, mock_get, mock_schedule):
        pg    = MagicMock()
        pg.id = "pg-stop"
        mock_get.return_value = pg
        bcp.stop_nifi()
        mock_schedule.assert_called_once_with("pg-stop", False)


# ===========================================================================
# wait_for_nifi
# ===========================================================================

class TestWaitForNifi(unittest.TestCase):

    def _make_status(self, queued, threads):
        snap                     = MagicMock()
        snap.queued_count        = str(queued)
        snap.active_thread_count = threads
        agg                      = MagicMock()
        agg.aggregate_snapshot   = snap
        status                   = MagicMock()
        status.process_group_status = agg
        return status

    @patch("main_orchestration_dag.time.sleep", return_value=None)
    @patch.object(_nipyapi.canvas, "get_process_group")
    @patch.object(bcp, "_login_nifi")
    def test_returns_after_stable_idle_rounds(self, mock_login, mock_get, _sleep):
        pg    = MagicMock()
        pg.id = "pg-1"
        mock_get.return_value = pg

        statuses = (
            [self._make_status(10, 2)]
            + [self._make_status(0, 0)] * 2   # STABLE_CHECKS = 2
        )
        _nipyapi.nifi.FlowApi.return_value.get_process_group_status.side_effect = statuses

        bcp.wait_for_nifi()  # must not raise

    @patch("main_orchestration_dag.time.time")
    @patch("main_orchestration_dag.time.sleep", return_value=None)
    @patch.object(_nipyapi.canvas, "get_process_group")
    @patch.object(bcp, "_login_nifi")
    def test_raises_timeout_when_exceeded(self, mock_login, mock_get, _sleep, mock_time):
        pg    = MagicMock()
        pg.id = "pg-timeout"
        mock_get.return_value = pg
        mock_time.side_effect = [0, 0, 999]

        _nipyapi.nifi.FlowApi.return_value.get_process_group_status.return_value = (
            self._make_status(5, 0)
        )

        with self.assertRaises(TimeoutError):
            bcp.wait_for_nifi()

    @patch("main_orchestration_dag.time.sleep", return_value=None)
    @patch.object(_nipyapi.canvas, "get_process_group")
    @patch.object(bcp, "_login_nifi")
    def test_waits_for_initial_activity_before_counting_idle(self, mock_login, mock_get, _sleep):
        pg    = MagicMock()
        pg.id = "pg-2"
        mock_get.return_value = pg

        statuses = (
            [self._make_status(0, 0)] * 3    # idle before activity — should NOT count
            + [self._make_status(1, 0)]       # activity starts
            + [self._make_status(0, 0)] * 2  # STABLE_CHECKS idle rounds
        )
        _nipyapi.nifi.FlowApi.return_value.get_process_group_status.side_effect = statuses

        bcp.wait_for_nifi()  # must not raise


# ===========================================================================
# verify_hdfs
# ===========================================================================

class TestVerifyHdfs(unittest.TestCase):

    def _mock_response(self, status_code, body):
        resp                      = MagicMock()
        resp.status_code          = status_code
        resp.json.return_value    = body
        resp.raise_for_status     = MagicMock()
        if status_code >= 400:
            resp.raise_for_status.side_effect = Exception("HTTP error")
        return resp

    @patch.object(_requests, "get")
    def test_passes_when_enough_files(self, mock_get):
        files = [{"pathSuffix": f"f{i}.parquet"} for i in range(5)]
        mock_get.return_value = self._mock_response(200, {
            "FileStatuses": {"FileStatus": files}
        })
        bcp.verify_hdfs("/warehouse/bronze", min_files=5)

    @patch.object(_requests, "get")
    def test_raises_when_too_few_files(self, mock_get):
        mock_get.return_value = self._mock_response(200, {
            "FileStatuses": {"FileStatus": [{"pathSuffix": "one.parquet"}]}
        })
        with self.assertRaises(RuntimeError) as ctx:
            bcp.verify_hdfs("/warehouse/bronze", min_files=3)
        self.assertIn("Expected", str(ctx.exception))

    @patch.object(_requests, "get")
    def test_raises_on_404(self, mock_get):
        mock_get.return_value = self._mock_response(404, {})
        with self.assertRaises(RuntimeError) as ctx:
            bcp.verify_hdfs("/warehouse/missing")
        self.assertIn("missing", str(ctx.exception))

    @patch.object(_requests, "get")
    def test_strips_hdfs_scheme_from_path(self, mock_get):
        mock_get.return_value = self._mock_response(200, {
            "FileStatuses": {"FileStatus": [{"pathSuffix": "f.parquet"}]}
        })
        bcp.verify_hdfs("hdfs://namenode:9000/warehouse/bronze", min_files=1)
        url_called = mock_get.call_args[0][0]
        self.assertIn("/warehouse/bronze", url_called)
        self.assertNotIn("hdfs://", url_called)

    @patch.object(_requests, "get")
    def test_raises_on_http_error(self, mock_get):
        resp                  = MagicMock()
        resp.status_code      = 500
        resp.raise_for_status.side_effect = Exception("server error")
        mock_get.return_value = resp
        with self.assertRaises(Exception):
            bcp.verify_hdfs("/warehouse/bronze")


# ===========================================================================
# ensure_container_running
# ===========================================================================

class TestEnsureContainerRunning(unittest.TestCase):

    def _make_client(self, initial_status, reaches_running=True):
        container        = MagicMock()
        container.status = initial_status

        if reaches_running:
            container.reload.side_effect = (
                lambda: setattr(container, "status", "running")
            )
        else:
            container.reload.return_value = None

        client                           = MagicMock()
        client.containers.get.return_value = container
        return client, container

    @patch.object(bcp, "_docker_client")
    def test_skips_when_already_running(self, mock_dc):
        client, container = self._make_client("running")
        mock_dc.return_value = client
        bcp.ensure_container_running("my-container", timeout=5)
        container.start.assert_not_called()

    @patch("main_orchestration_dag.time.sleep", return_value=None)
    @patch.object(bcp, "_docker_client")
    def test_starts_stopped_container(self, mock_dc, _sleep):
        client, container = self._make_client("exited", reaches_running=True)
        mock_dc.return_value = client
        bcp.ensure_container_running("my-container", timeout=5)
        container.start.assert_called_once()

    @patch("main_orchestration_dag.time.sleep", return_value=None)
    @patch.object(bcp, "_docker_client")
    def test_raises_timeout_if_container_never_runs(self, mock_dc, _sleep):
        container        = MagicMock()
        container.status = "exited"
        container.reload.return_value = None

        client                           = MagicMock()
        client.containers.get.return_value = container
        mock_dc.return_value               = client

        with self.assertRaises(TimeoutError):
            bcp.ensure_container_running("my-container", timeout=2)

    @patch.object(bcp, "_docker_client")
    def test_silently_skips_missing_container(self, mock_dc):
        client                           = MagicMock()
        client.containers.get.side_effect = _docker.errors.NotFound("gone")
        mock_dc.return_value              = client
        bcp.ensure_container_running("ghost-container", timeout=5)


# ===========================================================================
# stop_container
# ===========================================================================

class TestStopContainer(unittest.TestCase):

    @patch.object(bcp, "_docker_client")
    def test_stops_running_container(self, mock_dc):
        container        = MagicMock()
        container.status = "running"

        client                           = MagicMock()
        client.containers.get.return_value = container
        mock_dc.return_value               = client

        bcp.stop_container("my-container", timeout=10)
        container.stop.assert_called_once_with(timeout=10)

    @patch.object(bcp, "_docker_client")
    def test_skips_already_stopped_container(self, mock_dc):
        container        = MagicMock()
        container.status = "exited"

        client                           = MagicMock()
        client.containers.get.return_value = container
        mock_dc.return_value               = client

        bcp.stop_container("my-container")
        container.stop.assert_not_called()

    @patch.object(bcp, "_docker_client")
    def test_silently_skips_missing_container(self, mock_dc):
        client                           = MagicMock()
        client.containers.get.side_effect = _docker.errors.NotFound("gone")
        mock_dc.return_value              = client
        bcp.stop_container("ghost-container")


# ===========================================================================
# ensure_spark_worker
# ===========================================================================

class TestEnsureSparkWorker(unittest.TestCase):

    @patch("main_orchestration_dag.time.sleep", return_value=None)
    @patch.object(_requests, "get")
    @patch.object(bcp, "ensure_container_running")
    def test_returns_when_alive_worker_found(self, mock_ecr, mock_get, _sleep):
        resp               = MagicMock()
        resp.json.return_value = {"workers": [{"state": "ALIVE"}]}
        mock_get.return_value  = resp

        bcp.ensure_spark_worker(timeout=30)
        mock_ecr.assert_called_once_with("spark-worker", timeout=30)

    @patch("main_orchestration_dag.time.time")
    @patch("main_orchestration_dag.time.sleep", return_value=None)
    @patch.object(_requests, "get", side_effect=ConnectionError("refused"))
    @patch.object(bcp, "ensure_container_running")
    def test_raises_timeout_when_no_workers(self, mock_ecr, _get, _sleep, mock_time):
        mock_time.side_effect = [0, 0, 999]
        with self.assertRaises(TimeoutError):
            bcp.ensure_spark_worker(timeout=10)

    @patch("main_orchestration_dag.time.sleep", return_value=None)
    @patch.object(_requests, "get")
    @patch.object(bcp, "ensure_container_running")
    def test_ignores_dead_workers_waits_for_alive(self, mock_ecr, mock_get, _sleep):
        dead              = MagicMock()
        dead.json.return_value  = {"workers": [{"state": "DEAD"}]}
        alive             = MagicMock()
        alive.json.return_value = {"workers": [{"state": "ALIVE"}]}
        mock_get.side_effect    = [dead, alive]

        bcp.ensure_spark_worker(timeout=60)


# ===========================================================================
# ensure_hive_ready
# ===========================================================================

class TestEnsureHiveReady(unittest.TestCase):

    @patch("main_orchestration_dag.time.sleep", return_value=None)
    @patch("main_orchestration_dag.socket.create_connection")
    @patch.object(bcp, "ensure_container_running")
    def test_returns_when_thrift_port_reachable(self, mock_ecr, mock_conn, _sleep):
        mock_conn.return_value.__enter__ = MagicMock(return_value=MagicMock())
        mock_conn.return_value.__exit__  = MagicMock(return_value=False)

        bcp.ensure_hive_ready(timeout=30)

        mock_ecr.assert_any_call("hive-metastore", timeout=120)
        mock_ecr.assert_any_call("hive-server",    timeout=120)

    @patch("main_orchestration_dag.time.time")
    @patch("main_orchestration_dag.time.sleep", return_value=None)
    @patch("main_orchestration_dag.socket.create_connection", side_effect=OSError("refused"))
    @patch.object(bcp, "ensure_container_running")
    def test_raises_timeout_when_port_never_opens(self, mock_ecr, _conn, _sleep, mock_time):
        mock_time.side_effect = [0, 0, 999]
        with self.assertRaises(TimeoutError) as ctx:
            bcp.ensure_hive_ready(timeout=10)
        self.assertIn("9083", str(ctx.exception))

    @patch("main_orchestration_dag.time.sleep", return_value=None)
    @patch("main_orchestration_dag.socket.create_connection")
    @patch.object(bcp, "ensure_container_running")
    def test_retries_until_port_opens(self, mock_ecr, mock_conn, _sleep):
        ok_ctx               = MagicMock()
        ok_ctx.__enter__     = MagicMock(return_value=MagicMock())
        ok_ctx.__exit__      = MagicMock(return_value=False)
        mock_conn.side_effect = [OSError("not yet"), ok_ctx]

        bcp.ensure_hive_ready(timeout=60)


if __name__ == "__main__":
    unittest.main()
