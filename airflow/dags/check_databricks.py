"""Write a small file to the landing volume and read it back: proves Airflow can reach Databricks."""

from __future__ import annotations

import io
import os

import pendulum
from airflow.sdk import dag, task
from databricks.sdk import WorkspaceClient


@dag(schedule=None, start_date=pendulum.datetime(2026, 11, 1, tz="UTC"), catchup=False, tags=["setup"])
def check_databricks():

    @task
    def write_and_read() -> None:
        w = WorkspaceClient()                      # reads DATABRICKS_HOST / DATABRICKS_TOKEN
        path = f"{os.environ['LANDING_VOLUME']}/_smoke/hello.txt"
        w.files.upload(path, io.BytesIO(b"hello from airflow"), overwrite=True)
        print(w.files.download(path).contents.read().decode())

    write_and_read()


check_databricks()