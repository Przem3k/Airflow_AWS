import boto3
import json
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from datetime import datetime
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
from botocore.exceptions import ClientError


def boto3_client(service):
    aws_connection = BaseHook.get_connection('aws_default')
    aws_access_key = aws_connection.login
    aws_secret_key = aws_connection.password
    extra = json.loads(aws_connection.extra) if aws_connection.extra else {}
    region_name = extra.get("region_name", "eu-west-1")
    client = boto3.client('batch',
                             aws_access_key_id=aws_access_key,
                             aws_secret_access_key=aws_secret_key,
                             region_name=region_name,
                             )
    return client



def submit_batch_job(**kwargs):

    client = boto3_client(service='batch')

    response = client.submit_job(
        jobName='hi_bob_to_redshift',
        jobQueue='meltano_queue',
        jobDefinition='hi_bob_to_redshift',
        shareIdentifier='Hibob',
        schedulingPriorityOverride=1,
        containerOverrides={},      # można pominąć
    )

    job_id = response['jobId']
    print(f"✅ Job submitted with ID: {job_id}")
    # Przechowaj ID jako XCom
    kwargs['ti'].xcom_push(key='batch_job_id', value=job_id)

def wait_for_batch_job(**kwargs):
    client = boto3_client(service='batch')
    job_id = kwargs['ti'].xcom_pull(key='batch_job_id', task_ids='submit_batch_job')

    print(f"⏳ Waiting for job {job_id} to complete...")

    while True:
        try:
            response = client.describe_jobs(jobs=[job_id])
            job = response['jobs'][0]
            status = job['status']
            print(f"📌 Current status: {status}")

            if status in ['SUCCEEDED', 'FAILED']:
                break

            time.sleep(30)

        except ClientError as e:
            print("❌ Błąd podczas sprawdzania statusu zadania:", e)
            raise

    if status == 'SUCCEEDED':
        print("✅ AWS Batch job zakończony sukcesem.")
    else:
        reason = job.get('statusReason', 'No reason provided')
        raise Exception(f"❌ AWS Batch job FAILED. Reason: {reason}")

with DAG(
    dag_id='hi_bob_to_redshift',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
) as dag:

    run_batch = PythonOperator(
        task_id='submit_batch_job',
        python_callable=submit_batch_job,
        provide_context=True,
    )
    wait = PythonOperator(
        task_id='wait_for_batch_job',
        python_callable=wait_for_batch_job,
        provide_context=True
    )

    run_batch >> wait