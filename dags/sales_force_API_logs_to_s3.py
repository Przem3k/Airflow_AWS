from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import requests
import gzip
import json
import boto3
from simple_salesforce import Salesforce

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 1),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'salesforce_eventlog_to_s3',
    default_args=default_args,
    description='Download Salesforce EventLog files to S3',
    schedule_interval='0 3 * * *',
    catchup=False,
)


def get_s3_client():
    """Returns configured AWS S3 client"""
    aws_connection = BaseHook.get_connection('aws_default')
    extra = json.loads(aws_connection.extra) if aws_connection.extra else {}
    return boto3.client(
        's3',
        aws_access_key_id=aws_connection.login,
        aws_secret_access_key=aws_connection.password,
        region_name=extra.get("region_name", "eu-west-1")
    )


def get_salesforce_connection():
    """Returns configured Salesforce connection"""
    sf_connection = BaseHook.get_connection('salesforce_default')
    extra = json.loads(sf_connection.extra) if sf_connection.extra else {}

    # Password format: "password|security_token"
    if '|' in sf_connection.password:
        password, security_token = sf_connection.password.split('|', 1)
    else:
        password = sf_connection.password
        security_token = extra.get('security_token')

    return Salesforce(
        username=sf_connection.login,
        password=password,
        security_token=security_token,
        client_id=extra.get('client_id'),
        domain=extra.get('domain', 'login')
    )


def get_existing_files_in_s3(s3_client, bucket_name, days_back=7, prefix='salesforce-eventlogs/'):
    """Get list of existing files in S3 from last N days (filtered by LastModified)"""
    existing_files = set()
    cutoff_date = datetime.now() - timedelta(days=days_back)

    print(f"Checking S3 for files modified after {cutoff_date.date()}...")

    paginator = s3_client.get_paginator('list_objects_v2')

    try:
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    last_modified = obj['LastModified'].replace(tzinfo=None)

                    if last_modified >= cutoff_date:
                        key = obj['Key']
                        if key.endswith('.csv'):
                            file_id = key.split('/')[-1].replace('.csv', '')
                            existing_files.add(file_id)

        print(f"Found {len(existing_files)} files in S3 from last {days_back} days")
        return existing_files

    except Exception as e:
        print(f"Warning: Could not list S3 files: {str(e)}")
        return set()


def download_eventlogs_to_s3(**context):
    """Download EventLogFile from Salesforce and save to S3"""
    sf = get_salesforce_connection()
    s3_client = get_s3_client()
    bucket_name = 'data-spacelift-airflow-sf-imports-dev'

    existing_files = get_existing_files_in_s3(s3_client, bucket_name, days_back=7)

    query = """
        SELECT Id, EventType, LogDate, LogFile
        FROM EventLogFile 
        WHERE LogDate >= LAST_N_DAYS:3
        ORDER BY LogDate DESC, EventType
    """

    results = sf.query_all(query)
    records = results['records']
    print(f"Found {len(records)} EventLogFile records from Salesforce")

    downloaded = 0
    skipped = 0
    errors = 0
    error_details = []  # Track error details for reporting

    for record in records:
        eventlog_id = record['Id']
        event_type = record['EventType']
        log_date_str = record['LogDate']
        log_date = datetime.strptime(log_date_str.split('T')[0], '%Y-%m-%d')
        log_file_path = record.get('LogFile')

        if not log_file_path:
            skipped += 1
            continue

        if eventlog_id in existing_files:
            print(f"⊙ Skipping {eventlog_id} ({event_type}) - already exists in S3")
            skipped += 1
            continue

        s3_key = (
            f"salesforce-eventlogs/"
            f"event_type={event_type}/"
            f"year={log_date.year}/"
            f"month={log_date.month:02d}/"
            f"day={log_date.day:02d}/"
            f"{eventlog_id}.csv"
        )

        try:
            full_url = f"https://{sf.sf_instance}{log_file_path}"
            headers = {'Authorization': f'Bearer {sf.session_id}'}

            print(f"→ Downloading {event_type} {eventlog_id}...")
            response = requests.get(full_url, headers=headers, timeout=60)
            response.raise_for_status()

            content = response.content
            try:
                log_content = gzip.decompress(content).decode('utf-8')
            except gzip.BadGzipFile:
                log_content = content.decode('utf-8')

            s3_client.put_object(
                Bucket=bucket_name,
                Key=s3_key,
                Body=log_content.encode('utf-8'),
                ContentType='text/csv'
            )

            file_size_kb = len(log_content) / 1024
            print(f"✓ Saved: {event_type}/{eventlog_id}.csv ({file_size_kb:.1f} KB)")
            downloaded += 1

        except Exception as e:
            error_msg = f"{eventlog_id} ({event_type}): {str(e)}"
            print(f"✗ Error with {error_msg}")
            error_details.append(error_msg)
            errors += 1

    print(f"\n{'=' * 60}")
    print(f"SUMMARY")
    print(f"{'=' * 60}")
    print(f"Total SF records:     {len(records)}")
    print(f"Already in S3:        {len(existing_files)}")
    print(f"Downloaded (new):     {downloaded}")
    print(f"Skipped:              {skipped}")
    print(f"Errors:               {errors}")
    print(f"{'=' * 60}")

    # Push metrics to XCom
    context['ti'].xcom_push(key='downloaded', value=downloaded)
    context['ti'].xcom_push(key='skipped', value=skipped)
    context['ti'].xcom_push(key='errors', value=errors)
    context['ti'].xcom_push(key='total_records', value=len(records))

    # FAIL task if ANY errors occurred
    if errors > 0:
        error_summary = "\n".join(error_details[:5])  # Show first 5 errors
        if len(error_details) > 5:
            error_summary += f"\n... and {len(error_details) - 5} more errors"

        raise AirflowException(
            f"❌ Failed to download {errors} out of {len(records)} EventLogFile records.\n\n"
            f"Error details:\n{error_summary}\n\n"
            f"Check logs for full details."
        )

    # Optional: FAIL if nothing was downloaded AND nothing was skipped (unexpected)
    if downloaded == 0 and skipped == 0 and len(records) > 0:
        raise AirflowException(
            f"⚠️ No files downloaded or skipped, but {len(records)} records found in Salesforce. "
            "This is unexpected - check S3 permissions and Salesforce API access."
        )

    print(f"✅ Success: All {downloaded} files downloaded successfully!")


download_task = PythonOperator(
    task_id='download_eventlogs_to_s3',
    python_callable=download_eventlogs_to_s3,
    dag=dag,
)