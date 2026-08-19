import os
import socket
import time
import logging
from datetime import datetime, timedelta
from urllib.parse import urlparse

import urllib3
import requests
import nipyapi
import docker as docker_sdk
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
log = logging.getLogger(__name__)

NIFI_API_URL  = os.environ.get("NIFI_API_URL")
NIFI_USERNAME = os.environ.get("NIFI_USERNAME")
NIFI_PASSWORD = os.environ.get("NIFI_PASSWORD")
NIFI_GROUP    = os.environ.get("NIFI_PROCESSOR_GROUP")
ALERT_EMAIL   = os.environ.get("ALERT_EMAIL")
HDFS_URL      = os.environ.get("HDFS_NAMENODE")
HIVE_URI      = os.environ.get("HIVE_METASTORE_URI", "thrift://hive-metastore:9083")

NIFI_WARMUP   = int(os.environ.get("NIFI_WARMUP", "60"))
POLL_INTERVAL = int(os.environ.get("NIFI_POLL_INTERVAL", "120"))
POLL_TIMEOUT  = int(os.environ.get("NIFI_POLL_TIMEOUT", "10800"))
STABLE_CHECKS = int(os.environ.get("NIFI_STABLE_CHECKS", "3")) 


def _login_nifi():
    nipyapi.config.nifi_config.host       = NIFI_API_URL
    nipyapi.config.nifi_config.verify_ssl = False
    nipyapi.security.service_login(
        service="nifi", username=NIFI_USERNAME, password=REDACTED_CREDENTIAL
    )


def kick_off_nifi(**ctx):
    name = ctx.get("group_name", NIFI_GROUP)
    _login_nifi()
    pg = nipyapi.canvas.get_process_group(name, identifier_type="name")
    nipyapi.canvas.schedule_process_group(pg.id, True)
    log.info(f"NiFi group '{name}' started (id={pg.id})")


def wait_for_nifi(**ctx):
    """Poll NiFi until ingestion is idle for STABLE_CHECKS consecutive rounds."""
    name = ctx.get("group_name", NIFI_GROUP)
    _login_nifi()
    pg = nipyapi.canvas.get_process_group(name, identifier_type="name")

    t0 = time.time()
    seen_activity = False
    empty_rounds  = 0

    log.info(f"Warming up {NIFI_WARMUP}s before first poll (poll={POLL_INTERVAL}s, timeout={POLL_TIMEOUT}s)")
    time.sleep(NIFI_WARMUP)

    while True:
        elapsed = time.time() - t0
        if elapsed > POLL_TIMEOUT:
            raise TimeoutError(f"NiFi didn't finish within {POLL_TIMEOUT // 3600}h")

        status  = nipyapi.nifi.FlowApi().get_process_group_status(pg.id)
        snap    = status.process_group_status.aggregate_snapshot
        queued  = int(snap.queued_count.replace(",", ""))
        threads = snap.active_thread_count or 0

        log.info(f"[{int(elapsed)}s] queued={queued}, threads={threads}, stable={empty_rounds}/{STABLE_CHECKS}")

        if queued > 0 or threads > 0:
            seen_activity = True
            empty_rounds  = 0
        else:
            if seen_activity:
                empty_rounds += 1
                if empty_rounds >= STABLE_CHECKS:
                    log.info(f"Ingestion done after {int(elapsed // 60)} min")
                    return
            else:
                log.info("NiFi idle – waiting for activity to begin")

        time.sleep(POLL_INTERVAL)


def stop_nifi(**ctx):
    name = ctx.get("group_name", NIFI_GROUP)
    _login_nifi()
    pg = nipyapi.canvas.get_process_group(name, identifier_type="name")
    nipyapi.canvas.schedule_process_group(pg.id, False)
    log.info(f"NiFi group '{name}' stopped (id={pg.id})")


def verify_hdfs(path: str, min_files: int = 1, **ctx):
    if path.startswith("hdfs://"):
        path = urlparse(path).path

    res = requests.get(f"http://namenode:9870/webhdfs/v1{path}?op=LISTSTATUS")
    if res.status_code == 404:
        raise RuntimeError(f"HDFS path missing: {path}")
    res.raise_for_status()

    files = res.json().get("FileStatuses", {}).get("FileStatus", [])
    if len(files) < min_files:
        raise RuntimeError(f"Expected >={min_files} files in {path}, got {len(files)}")
    log.info(f"{path} – {len(files)} file(s) OK")


# Container lifecycle helpers — start/stop Docker containers to manage RAM

def _docker_client():
    """Return a Docker client connected via the mounted socket."""
    return docker_sdk.from_env()


def ensure_container_running(container_name: str, timeout: int = 60, **ctx):
    """Start a stopped container and wait up to `timeout` seconds for it to be running."""
    client = _docker_client()
    try:
        container = client.containers.get(container_name)
        if container.status == "running":
            log.info(f"[{container_name}] Already running — nothing to do")
            return
        log.info(f"[{container_name}] Status={container.status}, starting...")
        container.start()
        # Wait for running state
        for _ in range(timeout):
            container.reload()
            if container.status == "running":
                log.info(f"[{container_name}] Started successfully")
                return
            time.sleep(1)
        raise TimeoutError(f"{container_name} did not reach 'running' within {timeout}s")
    except docker_sdk.errors.NotFound:
        log.warning(f"[{container_name}] Container not found — skipping start")


def stop_container(container_name: str, timeout: int = 30, **ctx):
    """Gracefully stop a running container to free RAM. Non-fatal if already stopped."""
    client = _docker_client()
    try:
        container = client.containers.get(container_name)
        if container.status != "running":
            log.info(f"[{container_name}] Already stopped (status={container.status})")
            return
        log.info(f"[{container_name}] Stopping to free RAM...")
        container.stop(timeout=timeout)
        log.info(f"[{container_name}] Stopped")
    except docker_sdk.errors.NotFound:
        log.warning(f"[{container_name}] Container not found — skipping stop")


def ensure_spark_worker(timeout: int = 120, **ctx):
    """Ensure spark-worker is running and registered with the master before submitting jobs."""
    ensure_container_running("spark-worker", timeout=timeout)

    t0 = time.time()
    log.info(f"Polling Spark master for alive worker (timeout={timeout}s)...")
    while True:
        elapsed = time.time() - t0
        if elapsed > timeout:
            raise TimeoutError(
                f"No Spark worker registered with master after {int(timeout)}s. "
                "Check spark-worker container logs."
            )
        try:
            resp    = requests.get("http://spark-master:8080/json/", timeout=3)
            workers = resp.json().get("workers", [])
            alive   = [w for w in workers if w.get("state") == "ALIVE"]
            if alive:
                log.info(
                    f"spark-worker registered after {int(elapsed)}s "
                    f"(alive workers: {len(alive)})"
                )
                return
        except Exception as poll_err:
            log.debug(f"[{int(elapsed)}s] Master poll failed: {poll_err}")
        log.info(f"[{int(elapsed)}s] No alive workers yet — retrying in 5s")
        time.sleep(5)


def ensure_hive_ready(timeout: int = 300, **ctx):
    """Start Hive containers and wait for the Thrift port (9083) to be reachable."""
    for container_name in ("hive-metastore", "hive-server"):
        ensure_container_running(container_name, timeout=120)

    host, port = "hive-metastore", 9083
    t0 = time.time()
    log.info(f"Polling {host}:{port} until Thrift is ready (timeout={timeout}s)...")
    while True:
        elapsed = time.time() - t0
        if elapsed > timeout:
            raise TimeoutError(f"Hive Metastore port {port} not ready after {timeout}s")
        try:
            with socket.create_connection((host, port), timeout=3):
                log.info(f"Hive Metastore ready after {int(elapsed)}s")
                return
        except OSError:
            log.info(f"[{int(elapsed)}s] Hive Metastore not ready yet — retrying in 10s")
            time.sleep(10)


default_args = {
    "owner": "data-engineering-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry":   False,
}

with DAG(
    dag_id="lakehouse_pipeline",
    default_args=default_args,
    description="NiFi ingestion → Bronze → Bronze Hive → Silver Spark → Gold Spark → alerts",
    start_date=datetime(2026, 5, 11),
    schedule="@daily",
    catchup=False,
    tags=["lakehouse"],
) as dag:

    # Ensure NiFi container is running before we talk to it
    ensure_nifi = PythonOperator(
        task_id="ensure_nifi_running",
        python_callable=ensure_container_running,
        op_kwargs={"container_name": "nifi", "timeout": 120},
    )

    start_nifi = PythonOperator(
        task_id="start_nifi",
        python_callable=kick_off_nifi,
    )

    poll_nifi = PythonOperator(
        task_id="wait_for_nifi",
        python_callable=wait_for_nifi,
        execution_timeout=timedelta(hours=4),
    )

    stop_nifi_task = PythonOperator(
        task_id="stop_nifi_flow",
        python_callable=stop_nifi,
    )

    # Stop NiFi container once ingestion is confirmed done to free RAM
    shutdown_nifi = PythonOperator(
        task_id="shutdown_nifi_container",
        python_callable=stop_container,
        op_kwargs={"container_name": "nifi", "timeout": 30},
        trigger_rule=TriggerRule.ALL_DONE,   # run even if stop_nifi_flow failed
    )
    ensure_hive = PythonOperator(
        task_id="ensure_hive_ready",
        python_callable=ensure_hive_ready,
        op_kwargs={"timeout": 300},
    )

    check_bronze = PythonOperator(
        task_id="check_bronze",
        python_callable=verify_hdfs,
        op_kwargs={"path": "/warehouse/bronze", "min_files": 47},
    )

    # Restart spark-worker if it was stopped at the end of the last run
    ensure_spark = PythonOperator(
        task_id="ensure_spark_worker",
        python_callable=ensure_spark_worker,
        op_kwargs={"timeout": 120},
    )

    catalog_bronze = SparkSubmitOperator(
        task_id="catalog_bronze",
        conn_id="spark_default",
        application="/opt/airflow/spark/jobs/bronze_catalog.py",
        jars="/opt/spark/jars/iceberg/postgresql-42.7.3.jar",
        conf={
            "spark.executor.memory":             "512m",
            "spark.driver.memory":               "512m",
            "spark.executor.cores":              "1",
            "spark.sql.adaptive.enabled":        "true",
            "spark.sql.legacy.timeParserPolicy": "LEGACY",
            "hive.metastore.uris":               HIVE_URI,
        },
        env_vars={
            "BRONZE_BASE":        f"{HDFS_URL}/warehouse/bronze",
            "HIVE_METASTORE_URI": HIVE_URI,
        },
        verbose=True,
    )

    run_silver = SparkSubmitOperator(
        task_id="run_silver",
        conn_id="spark_default",
        application="/opt/airflow/spark/jobs/silver_job.py",
        py_files="/opt/airflow/spark/jobs/silver_config.py",
        # Postgres JDBC JAR — needed by Hive metastore client inside Spark
        jars="/opt/spark/jars/iceberg/postgresql-42.7.3.jar",
        conf={
            "spark.executor.memory":                         "1g",
            "spark.driver.memory":                           "512m",
            "spark.executor.cores":                          "1",
            "spark.sql.adaptive.enabled":                    "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.shuffle.partitions":                  "20",
            "spark.sql.legacy.timeParserPolicy":             "LEGACY",
            "hive.metastore.uris":                           HIVE_URI,
        },
        env_vars={
            "BRONZE_BASE":        f"{HDFS_URL}/warehouse/bronze",
            "SILVER_BASE":        f"{HDFS_URL}/warehouse/silver",
            "HIVE_METASTORE_URI": HIVE_URI,
        },
        verbose=True,
    )

    check_silver = PythonOperator(
        task_id="check_silver",
        python_callable=verify_hdfs,
        op_kwargs={"path": "/warehouse/silver", "min_files": 1},
    )

    run_gold = SparkSubmitOperator(
        task_id="run_gold",
        conn_id="spark_default",
        application="/opt/airflow/spark/jobs/gold_job.py",
        jars="/opt/spark/jars/iceberg/postgresql-42.7.3.jar",
        conf={
            "spark.executor.memory":                         "4g",
            "spark.driver.memory":                           "4g",
            "spark.executor.cores":                          "2",
            "spark.sql.adaptive.enabled":                    "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.shuffle.partitions":                  "50",
            "spark.sql.legacy.timeParserPolicy":             "LEGACY",
            "hive.metastore.uris":                           HIVE_URI,
        },
        env_vars={
            "SILVER_BASE":        f"{HDFS_URL}/warehouse/silver",
            "GOLD_BASE":          f"{HDFS_URL}/warehouse/gold",
            "HIVE_METASTORE_URI": HIVE_URI,
        },
        verbose=True,
    )

    check_gold = PythonOperator(
        task_id="check_gold",
        python_callable=verify_hdfs,
        op_kwargs={"path": "/warehouse/gold/master_table", "min_files": 1},
    )

    # Stop Spark worker once all jobs are done to free RAM
    shutdown_spark = PythonOperator(
        task_id="shutdown_spark_worker",
        python_callable=stop_container,
        op_kwargs={"container_name": "spark-worker", "timeout": 30},
        trigger_rule=TriggerRule.ALL_DONE,   # run even if silver failed
    )

    mail_ok = EmailOperator(
        task_id="mail_success",
        to=ALERT_EMAIL,
        subject="✅ Lakehouse Pipeline – {{ ds }} – SUCCESS",
        html_content="""
            <h2 style="color:#2e7d32">Pipeline finished successfully</h2>
            <table style="border-collapse:collapse;width:100%">
              <tr><td style="padding:8px;border:1px solid #ddd"><b>Date</b></td>
                  <td style="padding:8px;border:1px solid #ddd">{{ ds }}</td></tr>
              <tr><td style="padding:8px;border:1px solid #ddd"><b>Bronze</b></td>
                  <td style="padding:8px;border:1px solid #ddd">✅ 47 files — Hive tables registered</td></tr>
              <tr><td style="padding:8px;border:1px solid #ddd"><b>Silver</b></td>
                  <td style="padding:8px;border:1px solid #ddd">✅ Hive tables registered</td></tr>
              <tr><td style="padding:8px;border:1px solid #ddd"><b>Gold</b></td>
                  <td style="padding:8px;border:1px solid #ddd">✅ master_table written</td></tr>
            </table>
            <p style="color:#666;margin-top:16px"><a href="http://localhost:8081">Airflow UI</a>
            &nbsp;|&nbsp;<a href="http://localhost:10002">HiveServer2 UI</a></p>
        """,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    mail_fail = EmailOperator(
        task_id="mail_failure",
        to=ALERT_EMAIL,
        subject="🚨 Lakehouse Pipeline – {{ ds }} – FAILED",
        html_content="""
            <h2 style="color:#c62828">Pipeline failed</h2>
            <table style="border-collapse:collapse;width:100%">
              <tr><td style="padding:8px;border:1px solid #ddd"><b>Date</b></td>
                  <td style="padding:8px;border:1px solid #ddd">{{ ds }}</td></tr>
              <tr><td style="padding:8px;border:1px solid #ddd"><b>Status</b></td>
                  <td style="padding:8px;border:1px solid #ddd">❌ Check Airflow logs</td></tr>
            </table>
            <p style="color:#666;margin-top:16px"><a href="http://localhost:8081">Airflow UI</a></p>
        """,
        trigger_rule=TriggerRule.ONE_FAILED,
    )

    ensure_nifi >> start_nifi >> poll_nifi >> stop_nifi_task >> shutdown_nifi
    shutdown_nifi >> ensure_hive >> check_bronze >> ensure_spark >> catalog_bronze >> run_silver >> check_silver
    check_silver >> run_gold >> check_gold
    check_gold >> shutdown_spark
    check_gold >> mail_ok
    check_gold >> mail_fail
