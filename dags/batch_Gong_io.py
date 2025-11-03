import boto3
import json
import time
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.hooks.base import BaseHook
from botocore.exceptions import ClientError

# Slack Webhook URL - best practice: store in Airflow Variables or Connections
SLACK_WEBHOOK_URL = "{{ var.value.get('slack_webhook_url', '') }}"


def send_slack_notification(message, emoji=":information_source:"):
    """Sends notification to Slack"""
    if not SLACK_WEBHOOK_URL:
        print("⚠️ Slack webhook URL not configured, skipping notification")
        return

    try:
        payload = {
            "text": f"{emoji} {message}"
        }
        response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        response.raise_for_status()
        print(f"✅ Slack notification sent: {message}")
    except Exception as e:
        print(f"⚠️ Failed to send Slack notification: {e}")


def get_batch_client():
    """Returns configured AWS Batch client"""
    aws_connection = BaseHook.get_connection('aws_default')
    extra = json.loads(aws_connection.extra) if aws_connection.extra else {}

    return boto3.client(
        'batch',
        aws_access_key_id=aws_connection.login,
        aws_secret_access_key=aws_connection.password,
        region_name=extra.get("region_name", "eu-west-1")
    )


def submit_and_wait_batch_job(**kwargs):
    """Submits AWS Batch job and waits for completion"""
    client = get_batch_client()
    job_name = 'gong_io_to_redshift'

    # Submit job
    try:
        response = client.submit_job(
            jobName=job_name,
            jobQueue='meltano_queue',
            jobDefinition=job_name,
            shareIdentifier='SF',
            schedulingPriorityOverride=1,
            containerOverrides={},
        )
        job_id = response['jobId']
        print(f"✅ Job submitted with ID: {job_id}")
        send_slack_notification(f"Gong.io batch job started: `{job_id}`", ":rocket:")

    except ClientError as e:
        error_msg = f"Failed to submit Gong.io batch job: {str(e)}"
        print(f"❌ {error_msg}")
        send_slack_notification(error_msg, ":x:")
        raise

    # Wait for completion
    print(f"⏳ Waiting for job {job_id} to complete...")
    max_wait_time = 3600  # 1 hour timeout
    start_time = time.time()

    while True:
        if time.time() - start_time > max_wait_time:
            error_msg = f"Job {job_id} exceeded maximum wait time of {max_wait_time}s"
            send_slack_notification(error_msg, ":warning:")
            raise TimeoutError(error_msg)

        try:
            response = client.describe_jobs(jobs=[job_id])
            job = response['jobs'][0]
            status = job['status']
            print(f"📌 Current status: {status}")

            if status == 'SUCCEEDED':
                success_msg = f"Gong.io batch job completed successfully: `{job_id}`"
                print(f"✅ {success_msg}")
                send_slack_notification(success_msg, ":white_check_mark:")
                return job_id

            elif status == 'FAILED':
                reason = job.get('statusReason', 'No reason provided')
                error_msg = f"Gong.io batch job FAILED: `{job_id}` - {reason}"
                print(f"❌ {error_msg}")
                send_slack_notification(error_msg, ":x:")
                raise Exception(error_msg)

            time.sleep(30)

        except ClientError as e:
            error_msg = f"Error checking job status: {str(e)}"
            print(f"❌ {error_msg}")
            send_slack_notification(error_msg, ":x:")
            raise


default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
        dag_id='gong_io_to_redshift',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@hourly',
        catchup=False,
        tags=['batch', 'gong', 'redshift'],
        description='Runs Gong.io data sync to Redshift via AWS Batch',
) as dag:
    run_batch = PythonOperator(
        task_id='submit_and_wait_batch_job',
        python_callable=submit_and_wait_batch_job,
        provide_context=True,
    )