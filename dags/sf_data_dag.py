import boto3
import time
from airflow import DAG, task
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.dates import days_ago
from airflow.models import Variable

import json
import pandas as pd
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
INCOMING_BUCKET_NAME = 'dev-spacelift-self-hosted-data-incoming'
PROCESSING_BUCKET_NAME = 'dev-spacelift-self-hosted-data-processing'
ARCHIVE_BUCKET_NAME = 'dev-spacelift-self-hosted-data-archive'
LOAD_TIME = current_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S')
IAM_ROLE = 'arn:aws:iam::467135700742:role/ssm-analysis-role'


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


def move_s3_file(source_file, source_bucket, target_bucket, target_prefix):
    if target_prefix is None:
        raise ValueError('Target prefix not provided')

    # Extract original file name
    filename = source_file.split("/")[-1]
    new_key = f"{target_prefix}/{filename}"

    # Copy and delete
    logging.info(f"Bucket {source_bucket} - file:{source_file}")
    s3_hook = S3Hook(aws_conn_id="aws_default")
    s3_hook.copy_object(source_bucket_name=source_bucket,
                        source_bucket_key=source_file,
                        dest_bucket_name=target_bucket,
                        dest_bucket_key=new_key)
    s3_hook.delete_objects(source_bucket, source_file)

    print(f"File moved: {source_file} -> {new_key}")

    return {"source_file_key": new_key,
            "target_file_key": new_key}


def identified_oldest_json_file(source_prefix, source_bucket, **kwargs):
    s3_hook = S3Hook(aws_conn_id="aws_default")
    files = s3_hook.list_keys(bucket_name=source_bucket, prefix=source_prefix)
    json_files = [key for key in files if key.endswith('.json')]
    if not json_files:
        raise TypeError(f"No json files in bucket: {source_bucket}")
    # pull files metadata
    files_info = [
        (key, s3_hook.get_key(key, bucket_name=source_bucket).last_modified)
        for key in json_files
    ]
    files_info.sort(key=lambda x: x[1])  # The oldest first
    # oldest file
    the_oldest_file = files_info[0][0]
    logging.info(f"Found oldest file {the_oldest_file}")
    return the_oldest_file


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


def extracting_tables_from_json(file_key, **kwargs):
    s3 = get_s3_client()

    file_folder = file_key.rsplit("/", 1)[0]
    load_id = Variable.get("load_id", default_var=None)

    logging.info(f"path: {file_key}")
    file = s3.get_object(Bucket=PROCESSING_BUCKET_NAME, Key=file_key)

    # data = file['Body']
    data = json.load(file['Body'])

    # extrac DAG Id and load id
    context = kwargs
    dag_id = context['dag_run'].run_id
    load_id= context["ti"].xcom_pull(task_ids="generate_files_identifier", key="load_id")

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
            filename = f"{file_folder}/{table}/{load_id}.csv"
            s3_key = f"{PROCESSING_BUCKET_NAME}/{filename}"
            s3 = get_s3_client()
            s3.put_object(Body=csv_buffer.getvalue(), Bucket=PROCESSING_BUCKET_NAME, Key=filename)
            logging.info(f"Successfully transferred file {filename}")
            files_and_tables.append({"s3_key": s3_key, "table_name": table})
    return files_and_tables


def load_data_to_redshift(ti):
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

def read_s3_json(file_name, bucket=INCOMING_BUCKET_NAME):
    """ Helper function to read a Json file from S3 """
    aws_connection = BaseHook.get_connection("aws_default")
    aws_access_key = aws_connection.login
    aws_secret_key = aws_connection.password
    extra = json.loads(aws_connection.extra) if aws_connection.extra else {}
    region_name = extra.get("region_name", "eu-west-1")
    s3 = boto3.client('s3',
                      aws_access_key_id=aws_access_key,
                      aws_secret_access_key=aws_secret_key,
                      region_name=region_name,
                      endpoint_override="https://s3.eu-west-1.amazonaws.com"
                      )
    try:
        obj = s3.get_object(Bucket=bucket, Key=file_name)
        logging.info(f"Successfully read {file_name} from S3")
        return json.load(f)(obj['Body'])
    except Exception as e:
        logging.error(f"Failed to read {file_name} from S3: {str(e)}")
        raise Exception(f"Failed to read {file_name} from S3: {str(e)}")


# Konfiguracja DAG-a
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
}

with DAG(
        dag_id="Self_hosted_data_loader",
        default_args=default_args,
        schedule_interval="@once",
        catchup=False,
) as dag:
    wait_for_file = S3KeySensor(
        task_id="wait_for_files",
        bucket_name=INCOMING_BUCKET_NAME,
        bucket_key='*',
        wildcard_match=True,
        aws_conn_id=AWS_CONN_ID,
        timeout=600,
        poke_interval=30,
        mode="poke"
    )
    # Generate files id base on run id
    generate_files_identifier = PythonOperator(
        task_id="generate_files_identifier",
        python_callable=generate_files_identifier,
        provide_context=True,
    )
    # Identified the oldest file in S3 bucket
    identified_oldest_json_file = PythonOperator(
        task_id="identified_oldest_json_file",
        python_callable=identified_oldest_json_file,
        provide_context=True,
        op_kwargs={"source_prefix": '',
                   "source_bucket": INCOMING_BUCKET_NAME
                   }
    )

    move_to_processing = PythonOperator(
        task_id="move_to_processing",
        python_callable=move_s3_file,
        op_kwargs={"source_file": "{{ ti.xcom_pull(task_ids='identified_oldest_json_file', key='return_value') }}",
                   "source_bucket": INCOMING_BUCKET_NAME,
                   "target_bucket": PROCESSING_BUCKET_NAME,
                   "target_prefix": "{{ ti.xcom_pull(task_ids='generate_files_identifier', key='return_value')['load_id'] }}"
                   }
    )
    extracting_from_json = PythonOperator(
        task_id="extracting_from_json",
        python_callable=extracting_tables_from_json,
        op_kwargs={
            "file_key": "{{ ti.xcom_pull(task_ids='move_to_processing', key='return_value')['target_file_key'] }}"},
        provide_context=True
    ),
    load_task = PythonOperator(
        task_id='load_data_to_redshift',
        python_callable=load_data_to_redshift,
        provide_context=True,
    )
    restart_dag = TriggerDagRunOperator(
        task_id="restart_dag",
        trigger_dag_id="Self_hosted_data_loader",
        wait_for_completion=False
    )

    wait_for_file >> generate_files_identifier >> identified_oldest_json_file >> move_to_processing >> extracting_from_json >> load_task >> restart_dag
