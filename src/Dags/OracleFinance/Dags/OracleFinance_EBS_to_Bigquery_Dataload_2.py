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


# ********* Initialize Configurations *********
configurations_path = "/home/airflow/gcs/dags/{}/OracleFinance/Configurations/".format(Environment)
common_var_path = "/home/airflow/gcs/dags/{}/OracleFinance/Configurations/Common/".format(Environment)
common_var = common_var_path + "common_variables.json"
dag_var_path = "/home/airflow/gcs/dags/{}/OracleFinance/Configurations/{}/Variables/".format(Environment,Dag_Name)
dag_var = dag_var_path + "dag_variables.json"

# Import the read_json_file module from utility folder
utility_folder_path="/home/airflow/gcs/dags/{}/CommonPattern".format(Environment)
sys.path.append(utility_folder_path)
import Read_config_file 
import Run_Audit_Module_Ankit
common_variables = Read_config_file.read_json(common_var)
dag_variables = Read_config_file.read_json(dag_var) 

# Variable to store current date utc
Current_date_utc = datetime.utcnow().strftime('%Y%m%d')


# ********* Common Config Vairables *********
BQ_PROJECT = common_variables['Project']
DF_REGION = common_variables['DF_Region'] 
BQ_ODS_STAGE = common_variables['Bigquery_ODS_Stage'] 
INITIAL_EXTRACT_DATE= common_variables['Initial_Extract_Date'] 


# ******* Dag Specific Airflow Variables ***********
EDW_TABLE_LIST = Variable.get("data_ingestion_edw_oracleebs_table_list", deserialize_json=True)
EDW_BIGQUERY_LIST = Variable.get("data_ingestion_edw_bigquery_table_list", deserialize_json=True)
EDW_ORACLEEBS_QUERY_LIST = Variable.get("data_ingestion_edw_oracleebs_query")
EDW_ORACLEEBS_QUERY = eval(EDW_ORACLEEBS_QUERY_LIST)
EDW_TRANSACTION_TABLE_LIST = Variable.get("edw_oracleebs_transaction_table_list", deserialize_json=True)
# ******* Dag Specific Config Variables ***********
GCS_STAGE_BUCKET = dag_variables['GCS_Stage_Bucket'] 
GCS_SCHEMA_BUCKET = dag_variables['GCS_Schema_Bucket'] 
GCS_EXTRACT_PATH = dag_variables['Input_Data'] 
GCS_ARCHIVE_PATH = dag_variables['Archive_Folder'] 
GCS_SCHEMA_PATH = dag_variables['Schema_Folder'] 
GCS_BIGQUERY_REGION = dag_variables['BQ_Region'] 
DF_TAGS = dag_variables['DF_Tags'] 
FULL_LOAD_FLAG = dag_variables['Full_Load_Flag'] 

#  ******* Custom Variable Declaration ********* 
FILE_FORMAT = 'csv'
LOAD_TIME = pendulum.now('US/Eastern').strftime("%Y_%m_%d_%H_%M_%S")
LAST_EXECUTION= '{{ prev_data_interval_start_success }}'

if FULL_LOAD_FLAG == 'Y':
   LAST_EXECUTION=INITIAL_EXTRACT_DATE
else:
   LAST_EXECUTION=LAST_EXECUTION
    
#est_tz = pendulum.timezone("US/Eastern")

def on_failure_callback(context):
    try:
        LoggingMixin().log.info("DAG Failed! Updating RunAudit Table...")
        ti = context['task_instance']
        failed_dag_id= ti.dag_id
        failed_tasks = [] 
        for task_instance in context['dag_run'].get_task_instances():
            if task_instance.state == State.FAILED:
                failed_tasks.append(str(task_instance.task_id)) 
        failed_tasks_list = ', '.join(failed_tasks)
        if failed_tasks:
            failure_reason = f"Dag {failed_dag_id} Failure - one or more tasks ended with errors: {failed_tasks_list}."
            reason = failure_reason[:500]
            LoggingMixin().log.error(f"Failed Tasks: {failure_reason}")
        else:
            reason = f"Dag {failed_dag_id} Generic Failure"
        runauditid = ti.xcom_pull(task_ids="GetRunaudit")
        if runauditid is not None:
            Run_Audit_Module_Ankit.update_audit_info(arg1={'RunAuditId': runauditid, 'Status':"Failed",'Reason': reason})
    except Exception as e:
        LoggingMixin().log.error(f"An error occurred in on_failure_callback: {str(e)}")

def on_success_callback(context):
    try:
        LoggingMixin().log.info("DAG Succeeded! Updating RunAudit Table...")
        ti = context['task_instance']
        reason = f"DAG {ti.dag_id} completed successfully..."
        runauditid = ti.xcom_pull(task_ids="GetRunaudit")
        if runauditid is not None:
            Run_Audit_Module_Ankit.update_audit_info(arg1={'RunAuditId': runauditid, 'LastCompletedDate': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),'Status':"Success",  'Reason': reason})
    except Exception as e:
        LoggingMixin().log.error(f"An error occurred in on_success_callback: {str(e)}")
        
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
    on_failure_callback=on_failure_callback,
    on_success_callback=on_success_callback,
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
 
    #Print tasks leverages the print configs from Read json file module to print the map variables
    Task_Print_config_variables = PythonOperator(task_id='Print_config_variables', python_callable=Read_config_file.printconfigs,
    op_kwargs={'arg1': common_variables, 'arg2': dag_variables},)
    
    schedule_start = datetime.now()   
    formatted_start = schedule_start.strftime("%Y-%m-%d %H:%M:%S")
    auditid = f"{Dag_Name}-{formatted_start}"
    
    iAudit_dict = { 'RunAuditId': auditid,'SourceName': dag_variables['TableName'],'ParentSystemId': int(dag_variables['ParentSystemId']),  'SystemId': int(dag_variables['SystemId']), 'Username': GCP_CONN_ID,'JobName': Dag_Name, 'ScheduledStartDate':formatted_start} 

    Task_InsertRunAudit = PythonOperator(
        task_id='InsertRunaudit', python_callable=Run_Audit_Module_Ankit.insert_audit_log,
        op_kwargs={'arg1': iAudit_dict},
        provide_context=True,
        dag=dag,
        ) 

    #End task - Dummy task
    End = DummyOperator(
        task_id = 'End',
    )

 # Task to get the result of execute_sp_GetRunAuditInfo
    Task_GetRunAudit = PythonOperator(
        task_id="GetRunaudit", python_callable=Run_Audit_Module_Ankit.get_runid_timeframe,
        provide_context=True,
        dag=dag,
    )
        
    #call=DummyOperator(task_id='task',
     #         on_success_callback=on_success_callback,
      #        on_failure_callback=on_failure_callback,)
              
        #Task flow
    Start >> Task_Print_config_variables >> Task_InsertRunAudit >> Task_GetRunAudit >> End 