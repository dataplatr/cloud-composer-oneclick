'''
Code Author: ankit.srivastava@dataplatr.com
   Copyright: 2024
   Status: Dev
   Description: This DAG is used to extract data from Oracle EBS and Load data to OdsStage of Bigquery
'''

# Import libraries - 
import sys
import os
#import logging
from datetime import datetime
from datetime import timedelta
from airflow.models import DAG              #library to use DAG code , main class for geenerating DAG graph and execution flow.
from airflow.utils.dates import days_ago    # ibrary used in dag schedule definiton
from airflow.models import Variable         #Library to import Airflow variables defined at Airflow UI
from airflow.operators.dummy_operator import DummyOperator    #Library to import dummy operator to create dummy tasks for execution -> Start and End Dag Tasks uses this library
from airflow.operators.bash_operator import BashOperator      #Library to import Bash Operatot to make use bash commands or to execute Shell scripts
from airflow.providers.google.cloud.transfers.oracle_to_gcs import OracleToGCSOperator    #Library used to make Oracle Connection and extract dat to GCS as stage location 
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator      #Library used to load data from GCS to BigQuery
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator,BigQueryExecuteQueryOperator    #Latest Library used to load datga from Source Bigquery to Target Bigquery table
from airflow.operators.python import PythonOperator
import pendulum                             #Library used to set right timezone.
from google.cloud.dataform_v1beta1 import WorkflowInvocation

from airflow.utils.log.logging_mixin import LoggingMixin
#logger = logging.getLogger(__name__)
from airflow.utils.state import State
from airflow.providers.google.cloud.operators.dataform import (
    DataformCancelWorkflowInvocationOperator,
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
    DataformGetCompilationResultOperator,
    DataformGetWorkflowInvocationOperator,
)
from airflow.utils.email import send_email

# ******* Setting up environment and dag id based on dag file name ***********
file_path=os.path.abspath(__file__)
path_elements=file_path.split(os.sep)
Environment =path_elements[5]
Dag_Name =path_elements[-1].replace('.py','')
#Dag_Name ='OracleFinance_EBS_to_Bigquery_Dataload'
if Environment in ["Dev", "Qa"]:
	dag_id = Environment + "_" + Dag_Name
else:
	dag_id = Dag_Name 

if Environment == 'Dev':
    Env_Name = "Dev"
    GCP_CONN_ID = "Sandbox_BQ_SA_Connection"
#    Oracle_EBS_CONN_ID = "S_Oracle_Ebs_Connection"
elif Environment =='Qa':
    Env_Name = "Qa"
    GCP_CONN_ID = "Sandbox_BQ_SA_Connection"
#    Oracle_EBS_CONN_ID = "Qa_Oracle_Ebs_Connection"
else:
    Env_Name = "Prod"
    GCP_CONN_ID = "Sandbox_BQ_SA_Connection"
#    Oracle_EBS_CONN_ID = "Prod_Oracle_Ebs_Connection"
    
#est_tz = pendulum.timezone("US/Eastern")

def dirfunc():
    dagModuleName = "Source_To_Ods"
    filePath = os.path.abspath(__file__)
    LoggingMixin().log.error(f"filePath: {filePath}")
    commonPatternFolder = "CommonPattern"
    currentDir = os.path.dirname(os.path.realpath(__file__))
    LoggingMixin().log.error(f"currentDir: {currentDir}")
    parentDir = os.path.dirname(os.path.dirname(currentDir))
    LoggingMixin().log.error(f"parentDir: {parentDir}")
    commonPatternPath = os.path.join(parentDir, commonPatternFolder)
    LoggingMixin().log.error(f"commonPatternPath: {commonPatternPath}")
# Include the Utilities Folder path in the system path
    sys.path.append(commonPatternPath)

        
#  ******* DAG Code Starts here *********
with DAG(
    dag_id=dag_id,
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
   tags=[path_elements[6],Environment],
) as dag:
    #Start task - Dummy task
    Start = DummyOperator(
        task_id = 'Start',
    )
    #archive task - Bash command to move data from extracted path to archive path once data loads completed.
    #archive_task = BashOperator(
        #task_id="archive_task",
        #bash_command="gsutil -m mv gs://{}/{}/*.csv gs://{}/{}/{}/".format(GCS_STAGE_BUCKET, GCS_EXTRACT_PATH, #GCS_STAGE_BUCKET,GCS_ARCHIVE_PATH,LOAD_TIME),
        #execution_timeout=timedelta(hours=1),
    #)
 
    
    Task_GetDirectory = PythonOperator(
        task_id="GetDirectory", python_callable=dirfunc,
        provide_context=True,
        dag=dag,
    )
        
    #call=DummyOperator(task_id='task',
     #         on_success_callback=on_success_callback,
      #        on_failure_callback=on_failure_callback,)
              
        #Task flow
    Start >> Task_GetDirectory 