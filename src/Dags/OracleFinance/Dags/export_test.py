from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

with DAG(
    dag_id='xyz',
    template_searchpath=['/home/airflow/gcs/dags/'],
    start_date=days_ago(1),
    default_args={
        'owner': 'airflow',
        #'email': '',
        #"email_on_failure": True,
        #"email_on_success": True,
        #'retries': 2,
        #'retry_delay': timedelta(minutes=5),
    },
    max_active_runs=1,
    catchup=False,
    tags=['dev'],
) as dag:

    gcs_bucket = 'gs://gcs-sandbox-ods-stage/OracleEBS/inputdata' 
    export_prefix = 'Oracle_EBS'
    max_file_size_mb = 0.1

    tables = ['AP_HOLD_CODES']  # Add your table names here

    for table_id in tables:
        sql_query = f"""
select * from `dataplatr-sandbox.OracleEBS.OracleEbs-AP_HOLD_CODES`
    """

        export_command = f'bq query --destination_format=CSV --compression=GZIP --use_legacy_sql=false --max_rows=0 "{sql_query}" | \
                      split -b {max_file_size_mb}m - {gcs_bucket}/{export_prefix}_{table_id}_part_'

        export_task = BashOperator(
        task_id=f'export_bq_to_gcs_{table_id}',
        bash_command=export_command,
        dag=dag
    )

        export_task
