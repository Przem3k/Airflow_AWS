

from airflow import DAG, task
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models.baseoperator import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.decorators import apply_defaults
from concurrent.futures import ThreadPoolExecutor, as_completed



from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
import boto3
import json
import pandas as pd
import time
import os
import logging
import io
import csv

import hashlib
from datetime import datetime

tables_profile = {
    "loads": {
        "Primary_key": "id",
        "schema": {"load_id": "String",
                   "load_at": "DateTime",
                   "name": "String",
                   "ulid": "String",
                   "license_id": "String",
                   "app_version": "String",
                   "batch_from": "DateTime",
                   "batch_to": "DateTime",
                   "airflow_dag_id": "String"
                   },
    },
    "blueprints": {
        "Primary_key": "id",
        "schema": {"account_id": "Integer",
                   "created_at": "DateTime",
                   "deleted_at": "DateTime",
                   "format": "Integer",
                   "id": "Integer",
                   "published_at": "DateTime",
                   "space_id": "Integer",
                   "state": "Integer",
                   "ulid": "String",
                   "updated_at": "DateTime",
                   }
    },
    "daily_worker_usages": {
        "Primary_key": "id",
        "schema": {"account_id": "Integer",
                   "date": "DateTime",
                   "id": "Integer",
                   "private_minutes": "Integer",
                   "public_minutes": "Integer"
                   },
    },
    "heartbeats": {
        "Primary_key": ['first_heartbeat_time', 'last_heartbeat_time', "worker_ulid"],
        "schema": {"account_id": "Integer",
                   "first_heartbeat_time": "DateTime",
                   "last_heartbeat_time": "DateTime",
                   "worker_id": "Integer",
                   "worker_pool_id": "Integer",
                   "worker_pool_ulid": "string",
                   "worker_ulid": "string",
                   },
    },
    "logins": {
        "Primary_key": ['first_heartbeat_time', 'last_heartbeat_time', "worker_ulid"],
        "schema": {"account_id": "Integer",
                   "created_at": "DateTime",
                   "id": "Integer",
                   "updated_at": "DateTime",
                   "username": "string",
                   },
    },
    "managed_entities": {
        "Primary_key": ['id'],
        "schema": {"account_id": "Integer",
                   "created_at": "DateTime",
                   "creator_id": "Integer",
                   "id": "Integer",
                   "stack_id": "Integer",
                   "type": "String",
                   "updated_at": "DateTime",
                   "updater_id": "Integer",
                   },
    },
    "policies": {
        "Primary_key": ['id'],
        "schema": {"account_id": "Integer",
                   "created_at": "DateTime",
                   "id": "Integer",
                   "policy_template_id": "Integer",
                   "space_id": "Integer",
                   "type": "Integer",
                   "ulid": "String",
                   "updated_at": "DateTime"
                   },
    },
    "policy_attachments": {
        "Primary_key": ['id'],
        "schema": {"id": "Integer",
                   "created_at": "DateTime",
                   },
    },
    "resource_counts": {
        "Primary_key": ['id'],
        "schema": {"account_id": "Integer",
                   "created_at": "DateTime",
                   "id": "Integer",
                   "resources_count": "Integer",
                   "stack_id": "Integer"
                   },
    },
    "run_policy_receipts": {
        "Primary_key": ['id'],
        "schema": {"created_at": "DateTime",
                   "id": "Integer",
                   "policy_type": "Integer",
                   "policy_ulid": "String",
                   "run_id": "String",
                   "updated_at": "DateTime"
                   },
    },
    "run_state_transitions": {
        "Primary_key": ['id'],
        "schema": {
            "created_at": "DateTime",
            "id": "Integer",
            "new_state": "Integer",
            "new_state_version": "Integer",
            "run_ulid": "String",
            "worker_notified": "String",
            "worker_ulid": "String"
        },
    },
    "runs": {
        "Primary_key": ['id'],
        "schema": {
            "account_id": "DateTime",
            "commit_author_login": "String",
            "created_at": "DateTime",
            "drift_detection": "String",
            "finished": "String",
            "id": "Integer",
            "minutes_used": "Integer",
            "scheduled_task_id": "Integer",
            "stack_id": "Integer",
            "state": "Integer",
            "triggered_by": "String",
            "type": "Integer",
            "ulid": "String",
            "updated_at": "DateTime"
        },
    },
    "stacks": {
        "Primary_key": ['id'],
        "schema": {
            "account_id": "String",
            "administrative": "String",
            "autodeploy": "String",
            "autoretry": "String",
            "blueprint_id": "Integer",
            "created_at": "String",
            "deleted_at": "DateTime",
            "github_action_deploy": "String",
            "has_managed_state": "String",
            "id": "Integer",
            "is_module": "String",
            "manages_state_file": "String",
            "project_root": "String",
            "provider": "Integer",
            "public": "String",
            "terraform_provider": "String",
            "terraform_version": "String",
            "terraform_workflow_tool": "Integer",
            "ulid": "String",
            "updated_at": "DateTime",
            "use_smart_sanitization": "String",
            "using_space_level_vcs_integration": "String",
            "vendor": "Integer",
            "worker_pool_id": "Integer"
        },
    },
    "worker_pools": {
        "Primary_key": ['id'],
        "schema": {
            "account_id": "DateTime",
            "created_at": "DateTime",
            "id": "Integer"
        }
    },
    "workers": {
        "Primary_key": ['id'],
        "schema": {
            "created_at": "DateTime",
            "deleted_at": "DateTime",
            "gone_at": "DateTime",
            "id": "Integer",
            "metadata": "String",
            "ulid": "String",
            "updated_at": "DateTime",
            "version": "Integer",
            "worker_pool_id": "Integer"
        }
    }
}
# Configuration
AWS_CONN_ID = "aws_default"
#INCOMING_BUCKET_NAME = 'dev-spacelift-self-hosted-data-incoming'
PROCESSING_BUCKET_NAME = 'dev-spacelift-self-hosted-data-processing'
ARCHIVE_BUCKET_NAME = 'dev-spacelift-self-hosted-data-archive'
ERROR_BUCKET_NAME = 'dev-spacelift-self-hosted-data-error'
LOAD_TIME = current_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
IAM_ROLE = 'arn:aws:iam::467135700742:role/ssm-analysis-role'
#defines tags to mark files as success or failed to load to Redshift
PROCESSED_TAG_KEY= 'data_processed'
PROCESSED_TAG_SUCCESS = 'success'
PROCESSED_TAG_FAIL = 'error'


class S3TagSensor(BaseSensorOperator):
    """
    Sensor that monitors an S3 bucket for new files without the 'processed' tag.
    Uses 'reschedule' mode to optimize resource usage.

    :param bucket_name: Name of the S3 bucket
    :param prefix: Folder prefix to monitor (e.g., "input/")
    :param aws_conn_id: AWS connection ID in Airflow (default: "aws_default")
    """

    template_fields = ("prefix",)

    @apply_defaults
    def __init__(self, bucket_name, prefix, aws_conn_id="aws_default", *args, **kwargs):
        super().__init__(*args, mode="reschedule", **kwargs)
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.aws_conn_id = aws_conn_id

    def poke(self, context):
        """ Checks S3 for new files without the 'processed' tag """
        # s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        # s3_client = s3_hook.get_client_type("s3")

        aws_connection = BaseHook.get_connection(self.aws_conn_id)
        aws_access_key = aws_connection.login
        aws_secret_key = aws_connection.password
        extra = json.loads(aws_connection.extra) if aws_connection.extra else {}
        region_name = extra.get("region_name", "eu-west-1")
        s3_client = boto3.client('s3',
                          aws_access_key_id=aws_access_key,
                          aws_secret_access_key=aws_secret_key,
                          region_name=region_name,
                          )

        self.log.info(f"Checking for new files in: s3://{self.bucket_name}/{self.prefix}")

        response = s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=self.prefix)


        if "Contents" not in response:
            self.log.info("No files found.")
            return False

        matching_files = []

        for obj in response["Contents"]:
            file_key = obj["Key"]
            last_modified = obj["LastModified"]

            # Skip folders
            if file_key.endswith("/"):
                continue

            try:
                # Retrieve file tags
                tags = s3_client.get_object_tagging(Bucket=self.bucket_name, Key=file_key)["TagSet"]
                tag_dict = {tag["Key"]: tag["Value"] for tag in tags}
                # Check conditions
                if tag_dict.get(PROCESSED_TAG_KEY) not in  [PROCESSED_TAG_FAIL, PROCESSED_TAG_SUCCESS]:
                    self.log.info(f"File {file_key} is selected (last modified: {last_modified}).")
                    matching_files.append((file_key, last_modified))

            except Exception as e:
                self.log.warning(f"Could not get tags for {file_key}. Assuming no tags. Error: {e}")
                matching_files.append((file_key, last_modified))  # Assume file is new if we can't get tags

        if matching_files:
            # Get the oldest file (sort by LastModified)
            oldest_file = sorted(matching_files, key=lambda x: x[1])[0][0]

            # Push to XCom
            context['ti'].xcom_push(key="file_to_process", value=oldest_file)
            context['ti'].xcom_push(key="source_filename", value=oldest_file.split("/")[-1])
            context['ti'].xcom_push(key="source_folder"  , value=oldest_file.rsplit("/", 1)[0])
            self.log.info(f"Oldest file selected: {oldest_file} (pushed to XCom).")

            return True  # A matching file was found

        self.log.info("All files are already processed. Waiting for new files...")
        return False  # No new or error files found

class MultiS3TagSensor(BaseSensorOperator):
    """
    Sensor that monitors multiple S3 buckets/prefixes in parallel for new files without the 'processed' end 'error' tags.
    """

    @apply_defaults
    def __init__(self, buckets_info, aws_conn_id="aws_default", *args, **kwargs):
        super().__init__(*args, mode="reschedule", **kwargs)
        self.buckets_info = buckets_info
        self.aws_conn_id = aws_conn_id

    def _check_single_bucket(self, bucket_name, prefix):
        aws_connection = BaseHook.get_connection(self.aws_conn_id)
        aws_access_key = aws_connection.login
        aws_secret_key = aws_connection.password
        extra = json.loads(aws_connection.extra) if aws_connection.extra else {}
        region_name = extra.get("region_name", "eu-west-1")

        s3_client = boto3.client("s3",
                                 aws_access_key_id=aws_access_key,
                                 aws_secret_access_key=aws_secret_key,
                                 region_name=region_name)

        self.log.info(f"Checking s3://{bucket_name}/{prefix}")
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

        if "Contents" not in response:
            return None

        matching_files = []
        for obj in response["Contents"]:
            file_key = obj["Key"]
            last_modified = obj["LastModified"]

            if file_key.endswith("/"):
                continue

            try:
                tags = s3_client.get_object_tagging(Bucket=bucket_name, Key=file_key)["TagSet"]
                tag_dict = {tag["Key"]: tag["Value"] for tag in tags}

                if tag_dict.get("data_processed") not in ["error", "success"]:
                    matching_files.append((bucket_name, file_key, last_modified))
            except Exception as e:
                self.log.warning(f"Could not get tags for {file_key}. Assuming no tags. Error: {e}")
                matching_files.append((bucket_name, file_key, last_modified))

        if matching_files:
            return sorted(matching_files, key=lambda x: x[2])[0]

        return None

    def poke(self, context):
        with ThreadPoolExecutor(max_workers=len(self.buckets_info)) as executor:
            futures = [
                executor.submit(self._check_single_bucket, info["bucket"], info["prefix"])
                for info in self.buckets_info
            ]

            results = [future.result() for future in as_completed(futures) if future.result()]

        if results:
            selected_file = sorted(results, key=lambda x: x[2])[0]
            logging.info(f"All files not processed {results}")
            bucket, key, _ = selected_file

            context['ti'].xcom_push(key="file_to_process", value=key)
            context['ti'].xcom_push(key="source_filename", value=key.split("/")[-1])
            context['ti'].xcom_push(key="source_folder", value=key.rsplit("/", 1)[0])
            context['ti'].xcom_push(key="source_bucket", value=bucket)

            self.log.info(f"Selected file: s3://{bucket}/{key}")
            return True

        self.log.info("No matching files found across all buckets. Waiting...")
        return False



def generate_files_identifier(**kwargs):
    # extrac DAG Id
    context = kwargs
    run_id = context['dag_run'].run_id
    load_id = hashlib.md5(run_id.encode('utf-8')).hexdigest()
    print(f"Run ID: {run_id}, file_id: {load_id}")
    return {"load_id": load_id,
            "airflow_DAG_id": run_id}


def get_s3_client(connection='aws_default'):
    # Initialize boto3 client
    aws_connection = BaseHook.get_connection(connection)
    aws_access_key = aws_connection.login
    aws_secret_key = aws_connection.password
    extra = json.loads(aws_connection.extra) if aws_connection.extra else {}
    region_name = extra.get("region_name", "eu-west-1")
    s3 = boto3.client('s3',
                      aws_access_key_id=aws_access_key,
                      aws_secret_access_key=aws_secret_key,
                      region_name=region_name,
                      )
    return s3


def extract_load_data(data, load_id, dag_id=None):
    account = data["account"]
    account['app_version'] = data["spacelift_version"]
    account['batch_from'] = data["time_range"]["start"]
    account['batch_to'] = data["time_range"]["end"]
    load_data = {
        'load_id': load_id,
        'load_at': LOAD_TIME,
    }
    load_data.update(account)
    if dag_id:
        load_data['airflow_dag_id'] = dag_id
    return load_data


def extracting_tables_from_json(file_bucket, file_key, load_id, **kwargs):
    s3 = get_s3_client()
    logging.info(f"load_id: {load_id}")
    # extract folder from file key
    file_folder = file_key.rsplit("/", 1)[0]
    # extract file name without extension
    raw_file_name = file_key.rsplit("/", 1)[-1].rsplit(".", 1)[0]

    logging.info(f"path: {file_key}")
    file = s3.get_object(Bucket=file_bucket, Key=file_key)

    # data = file['Body']
    data = json.load(file['Body'])

    # extrac DAG Id and load id
    context = kwargs
    dag_id = context['dag_run'].run_id


    load_data = extract_load_data(data=data,load_id=load_id, dag_id=dag_id)

    data_to_process = data["metrics"]
    data_to_process['loads'] = {0: [load_data]}

    # iterate and extract all metrics
    files_and_tables = []
    for table, table_days in data["metrics"].items():
        # data flattening
        df = pd.DataFrame([entry for date, entries in table_days.items() for entry in entries])
        # df = pd.DataFrame([entry |{"_partition_date": date} for date, entries in table_days.items() for entry in entries])

        logging.info(f"Extracted records from {table}: {df.size}")

        if df.size > 0:
            # Prepare data types
            integer_columns = [k for k, v in tables_profile[table]['schema'].items() if v == 'Integer']
            df[integer_columns] = df[integer_columns].apply(lambda x: x.astype(pd.Int64Dtype()))

            # add account identifier and load_id
            if table not in ['loads']:
                # excluding metadata table
                df['account_ulid'] = load_data['ulid']
                df['load_id'] = load_id
            # dump extracted tables files to S3
            csv_buffer = io.StringIO()
            df.to_csv(path_or_buf=csv_buffer,
                      index=False,
                      quoting=csv.QUOTE_ALL,
                      sep='|',
                      quotechar='`',
                      escapechar="\\"
                      , encoding='utf-8')
            filename = f"{file_folder}/{table}/{load_id}/{raw_file_name}.csv"
            s3_key = f"{PROCESSING_BUCKET_NAME}/{filename}"
            s3 = get_s3_client()
            s3.put_object(Body=csv_buffer.getvalue(), Bucket=PROCESSING_BUCKET_NAME, Key=filename)
            logging.info(f"Successfully transferred file {filename}")
            files_and_tables.append({"s3_key": s3_key, "table_name": table})
        context['ti'].xcom_push(key="files_and_tables", value=files_and_tables)
    return files_and_tables


def load_data_to_redshift(ti):
    """
    Loads data to Redshift. Strategy all or nothing. If any of the tables upload failed entire translation is rollback.
    """
    files_and_tables = ti.xcom_pull(task_ids='extracting_from_json')
    logging.info(f'files_and_tables:{files_and_tables}')

    try:
        # initiate redshift connection
        redshift_hook = PostgresHook(postgres_conn_id="redshift_default")
        conn = redshift_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("BEGIN;")

        for table_info in files_and_tables:
            table_name = table_info["table_name"]
            s3_key = table_info["s3_key"]
            # Prepare Copy query
            copy_query = f"""
                copy spacelift_sh.{table_name}
                from 's3://{s3_key}'
                credentials  'aws_iam_role={IAM_ROLE}'
                DELIMITER '|'
                IGNOREHEADER 1
                DATEFORMAT 'auto'
                TIMEFORMAT 'auto'
                NULL AS 'None'
                CSV QUOTE AS '`';
            """
            # Execute copy
            print(f"Loading data to table: {table_name}.")
            cursor.execute(copy_query)
            # redshift_hook.run(copy_query)
            print(f"Data successfully transferred to a table: {table_name}.")
            conn.commit()
            status = 'success'
    except Exception as e:
        conn.rollback()
        logging.error(f"Error executing statement {copy_query}")
        logging.error(f"Failed to load data into {table_name}: {e}")
        status = 'failed'
    finally:
        cursor.close()
        conn.close()
    return status


def branch_task(ti):
    """
    Base on previous task status executes error or archive branch.
    """
    validation_results = ti.xcom_pull(task_ids='load_data_to_redshift')

    if validation_results == 'success':
        return 'move_to_archive'
    else:
        return 'move_to_error'

class UpdateS3TagOperator(BaseOperator):
    """
    Updates a specific tag on an S3 file.

    :param bucket_name: Name of the S3 bucket.
    :param file_key: (Optional) Key (path) of the file in S3. If None, it will be pulled from XCom.
    :param xcom_task_id: (Optional) Task ID to pull the file key from XCom.
    :param xcom_key: (Optional) XCom key where the file name is stored (default: "file_to_process").
    :param tag_key: The tag key to update.
    :param tag_value: The value to set for the given tag.
    :param aws_conn_id: AWS connection ID in Airflow (default: "aws_default").
    """

    @apply_defaults
    def __init__(self, tag_key, tag_value, s3_client, xcom_task_id=None, xcom_key=None,  *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.xcom_task_id = xcom_task_id
        self.xcom_key = xcom_key
        self.tag_key = tag_key
        self.tag_value = tag_value
        self.s3_client = s3_client

    def execute(self, context):
        """Updates the specified tag on the given S3 file."""
        ti = context["ti"]
        bucket_name = ti.xcom_pull(task_ids='wait_for_new_file', key='source_bucket')
        # Get file key: either from input or from XCom
        file_key = ti.xcom_pull(task_ids=self.xcom_task_id, key=self.xcom_key)
        logging.info(F"Attempt to tag {bucket_name} file {file_key} file with {self.tag_key} : {self.tag_value}")
        if not file_key:
            self.log.warning("No file found to update tags. Skipping.")
            return

        s3_client = self.s3_client

        # Fetch existing tags
        try:
            existing_tags = s3_client.get_object_tagging(Bucket=bucket_name, Key=file_key)["TagSet"]
        except Exception as e:
            self.log.warning(f"Could not retrieve existing tags for {file_key}: {e}")
            existing_tags = []

        # Update or add the specified tag
        updated_tags = {tag["Key"]: tag["Value"] for tag in existing_tags}
        updated_tags[self.tag_key] = self.tag_value  # Set new value

        # Convert back to AWS format
        tag_set = [{"Key": k, "Value": v} for k, v in updated_tags.items()]

        # Apply new tags
        s3_client.put_object_tagging(Bucket=bucket_name, Key=file_key, Tagging={"TagSet": tag_set})

        self.log.info(f"Updated tag '{self.tag_key}' for {file_key}: {self.tag_value}")
class MoveS3FilesOperator(BaseOperator):
    """
    Moves a list of S3 files from source buckets to a destination bucket.

    :param s3_paths: List of full S3 paths (e.g., "source-bucket/path/to/file.csv").
    :param destination_bucket: Target S3 bucket.
    :param aws_conn_id: AWS connection ID in Airflow (default: "aws_default").
    :param delete_source: Whether to delete the source files after copying (default: True).
    """

    @apply_defaults
    def __init__(self, destination_bucket, xcom_task_id, xcom_key, aws_conn_id="aws_default", delete_source=True, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.destination_bucket = destination_bucket
        self.aws_conn_id = aws_conn_id
        self.delete_source = delete_source
        self.xcom_task_id = xcom_task_id
        self.xcom_key = xcom_key

    def execute(self, context):
        """Moves files between S3 buckets by copying and optionally deleting them."""

        ti = context["ti"]

        # Log the XCom fetch attempt
        self.log.info(f"Attempting to pull from XCom: task_id={self.xcom_task_id}, key={self.xcom_key}")

        # Get files dict from XCom
        files_and_tables = ti.xcom_pull(task_ids=self.xcom_task_id, key=self.xcom_key)

        self.log.info(f"Retrieved from XCom: {files_and_tables}")

        if not files_and_tables:
            raise ValueError(f"No data found in XCom for task_id={self.xcom_task_id}, key={self.xcom_key}")

        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)

        for table_info in files_and_tables:
            s3_path = table_info["s3_key"]
            parts = s3_path.split("/", 1)  # Split into bucket and key
            if len(parts) < 2:
                self.log.warning(f"Skipping invalid S3 path: {s3_path}")
                continue

            source_bucket, key = parts
            destination_key = key  # Keep the same path in the new bucket

            self.log.info(f"Copying {key} from {source_bucket} to {self.destination_bucket}")

            # Copy file to the new bucket
            s3_hook.copy_object(
                source_bucket_name=source_bucket,
                dest_bucket_name=self.destination_bucket,
                source_bucket_key=key,
                dest_bucket_key=destination_key,
            )

            # Optionally delete the original file
            if self.delete_source:
                self.log.info(f"Deleting {key} from {source_bucket}")
                s3_hook.delete_objects(bucket=source_bucket, keys=[key])

            self.log.info(f"Successfully moved {len(files_and_tables)} files to {self.destination_bucket}")
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
}

with DAG(
        dag_id="Self_hosted_data_loader", default_args=default_args, schedule_interval="@once",
        catchup=False, max_active_runs=1) as dag:
    wait_for_new_file = MultiS3TagSensor(
        task_id="wait_for_new_file",
        buckets_info=[
            {"bucket": "dev-spacelift-self-hosted-data-incoming", "prefix": ""},
            {"bucket": "dev-spacelift-self-hosted-data-incoming-2", "prefix": ""},
        ],
        aws_conn_id="aws_default",
        poke_interval=60,
        timeout=600
    )
    generate_files_identifier = PythonOperator(
        task_id="generate_files_identifier",
        python_callable=generate_files_identifier,
        provide_context=True,
    )
    extracting_from_json = PythonOperator(
        task_id="extracting_from_json",
        python_callable=extracting_tables_from_json,
        op_kwargs={
            "file_bucket": "{{ ti.xcom_pull(task_ids='wait_for_new_file', key='source_bucket', include_prior_dates=False) }}",
            "file_key": "{{ ti.xcom_pull(task_ids='wait_for_new_file', key='file_to_process', include_prior_dates=False) }}",
            "load_id": "{{ ti.xcom_pull(task_ids='generate_files_identifier', key='return_value')['load_id'] }}"
        },
        provide_context=True
    )
    load_task = PythonOperator(
        task_id='load_data_to_redshift',
        python_callable=load_data_to_redshift,
        provide_context=True,
    )
    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=branch_task,
        provide_context=True
    )
    move_to_archive = MoveS3FilesOperator(
        task_id="move_to_archive",
        destination_bucket=ARCHIVE_BUCKET_NAME,
        delete_source=True,
        xcom_key="files_and_tables",
        xcom_task_id="extracting_from_json",
        aws_conn_id="aws_default",
        dag=dag,
    )
    move_to_error = MoveS3FilesOperator(
        task_id="move_to_error",
        destination_bucket=ERROR_BUCKET_NAME,
        delete_source=True,
        xcom_key="files_and_tables",
        xcom_task_id="extracting_from_json",
        aws_conn_id="aws_default",
        dag=dag,
    )
    set_processed_tag_success = UpdateS3TagOperator(
        task_id="set_processed_tag_success",
        tag_key=PROCESSED_TAG_KEY,
        tag_value=PROCESSED_TAG_SUCCESS,
        xcom_key="file_to_process",
        xcom_task_id="wait_for_new_file",
        s3_client=get_s3_client(),
        dag=dag,
    )
    set_processed_tag_error = UpdateS3TagOperator(
        task_id="set_processed_tag_error",
        tag_key=PROCESSED_TAG_KEY,
        tag_value=PROCESSED_TAG_FAIL,
        xcom_key="file_to_process",
        xcom_task_id="wait_for_new_file",
        s3_client=get_s3_client(),
        dag=dag,

    )
    restart_dag = TriggerDagRunOperator(
        task_id="restart_dag",
        trigger_dag_id="Self_hosted_data_loader",
        wait_for_completion=False,
        trigger_rule="none_failed"
    )

wait_for_new_file >> generate_files_identifier >> extracting_from_json >> load_task >> branch_task
branch_task >> move_to_archive >> set_processed_tag_success >> restart_dag
branch_task >> move_to_error >> set_processed_tag_error >> restart_dag