"""List the prediction files in S3, with their size and last-modified time.

The first step toward incremental ingestion: seeing what's in the bucket and
how Airflow reaches it, through a connection that assumes a read-only role.
"""

from __future__ import annotations

import pendulum
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import Variable, dag, task


@dag(
    schedule=None,                                   # manual runs only, for now
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    tags=["predictions"],
)
def list_prediction_files():

    @task
    def list_files() -> list[dict]:
        bucket = Variable.get("predictions_bucket")
        s3 = S3Hook(aws_conn_id="aws_predictions").get_conn()   # boto3 client, role already assumed

        files = []
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix="daily_"):
            for obj in page.get("Contents", []):
                files.append(
                    {
                        "key": obj["Key"],
                        "size": obj["Size"],
                        "last_modified": obj["LastModified"].isoformat(),
                    }
                )

        for f in sorted(files, key=lambda f: f["last_modified"]):
            print(f"{f['last_modified']}  {f['size']:>9,} bytes  {f['key']}")
        return files

    list_files()


list_prediction_files()