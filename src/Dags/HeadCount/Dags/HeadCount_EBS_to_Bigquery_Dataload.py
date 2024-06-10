'''
Code Author: manoj.kumar@dataplatr.com
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

# ******* Setting up environment and dag id based on dag file name ***********
file_path=os.path.abspath(__file__)
path_elements=file_path.split(os.sep)
Environment =path_elements[5]
Dag_Name =path_elements[-1].replace('.py','')
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
configurations_path = "/home/airflow/gcs/dags/{}/HeadCount/Configurations/".format(Environment)
common_var_path = "/home/airflow/gcs/dags/{}/HeadCount/Configurations/Common/".format(Environment)
common_var = common_var_path + "common_variables.json"
dag_var_path = "/home/airflow/gcs/dags/{}/HeadCount/Configurations/{}/Variables/".format(Environment,Dag_Name)
dag_var = dag_var_path + "dag_variables.json"

# Import the read_json_file module from utility folder
utility_folder_path="/home/airflow/gcs/dags/{}/CommonPattern".format(Environment)
sys.path.append(utility_folder_path)
import Read_config_file 
#import Run_Audit_module
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
EDW_TABLE_LIST = Variable.get("hc_data_ingestion_edw_oracleebs_table_list", deserialize_json=True)
EDW_BIGQUERY_LIST = Variable.get("hc_data_ingestion_edw_bigquery_table_list", deserialize_json=True)
EDW_ORACLEEBS_QUERY_LIST = Variable.get("hc_data_ingestion_edw_oracleebs_query")
EDW_ORACLEEBS_QUERY = eval(EDW_ORACLEEBS_QUERY_LIST)
# ******* Dag Specific Config Variables ***********
GCS_STAGE_BUCKET = dag_variables['GCS_Stage_Bucket'] 
GCS_SCHEMA_BUCKET = dag_variables['GCS_Schema_Bucket'] 
GCS_EXTRACT_PATH = dag_variables['Input_Data'] 
GCS_ARCHIVE_PATH = dag_variables['Archive_Folder'] 
GCS_SCHEMA_PATH = dag_variables['Schema_Folder'] 
GCS_BIGQUERY_REGION = dag_variables['BQ_Region'] 
FULL_LOAD_FLAG = dag_variables['Full_Load_Flag'] 

#  ******* Custom Variable Declaration ********* 
FILE_FORMAT = 'csv'
LOAD_TIME = pendulum.now('US/Eastern').strftime("%Y_%m_%d_%H_%M_%S")
LAST_EXECUTION= '{{ prev_data_interval_start_success }}'

if FULL_LOAD_FLAG == 'Y':
   LAST_EXECUTION=INITIAL_EXTRACT_DATE
else:
   LAST_EXECUTION=LAST_EXECUTION
    
with DAG(
    dag_id=dag_id,
    template_searchpath=['/home/airflow/gcs/dags/'],
    start_date=days_ago(1),
    default_args={
        'owner': 'airflow',
    },
    schedule_interval= f"{dag_variables['Schedule']}",
    max_active_runs=1,
    catchup=False,
   tags=[path_elements[6],Environment],
) as dag:
    #Start task - Dummy task
    Start = DummyOperator(
        task_id = 'Start',
    )

    #Print tasks leverages the print configs from Read json file module to print the map variables
    Task_Print_config_variables = PythonOperator(task_id='Print_config_variables', python_callable=Read_config_file.printconfigs,
    op_kwargs={'arg1': common_variables, 'arg2': dag_variables},)
    
    schedule_start = datetime.now()   
    formatted_start = schedule_start.strftime("%Y-%m-%d %H:%M:%S")
	
    #End task - Dummy task
    End = DummyOperator(
        task_id = 'End',
    )
    # For loop to generate tasks dynamically for all the tables listed in EDW_TABLE_LIST custom varaible.
    
    for table, bq_table,ebs_query in zip(EDW_TABLE_LIST, EDW_BIGQUERY_LIST,EDW_ORACLEEBS_QUERY):
        field_delimiter=","
        autodetect=False
        quote_character=None
        schema_object='{}/{}.json'.format(GCS_SCHEMA_PATH,table)
        #Task to  Extract data from Oracle EBS and load OdsStage
        oracle_to_gcs_task = BigQueryExecuteQueryOperator(
            task_id="oracle_to_gcs_{}".format(table),
            sql="select * from `OracleEBS.{}` where date(last_update_date) >= date(ifnull('{}','{}'))".format(bq_table,LAST_EXECUTION,INITIAL_EXTRACT_DATE),
            use_legacy_sql=False,
            #create_disposition='CREATE_NEVER',
            #write_disposition='WRITE_TRUNCATE',
            destination_dataset_table='OdsStage.{}'.format(bq_table),
            create_disposition='CREATE_NEVER',
            write_disposition='WRITE_TRUNCATE',
            gcp_conn_id=GCP_CONN_ID,
        )
		
		# Task to Ingest data from OdsStage BigQuery to Ods data set
        bq_to_bq_merge_task = BigQueryInsertJobOperator(
            task_id="bq_to_bq_merge_{}".format(table),
            configuration={
                "query": {
                    "query": "{}/HeadCount/Configurations/{}/Sql/{}.sql".format(Env_Name,Dag_Name,table),
                    "useLegacySql":False,
                    "allow_large_results":True,
                }
            },
            params={'BQ_PROJECT': BQ_PROJECT }, # 'BQ_EDW_DATASET': BQ_EDW_DATASET, 'BQ_STAGING_DATASET': BQ_STAGING_DATASET },
            gcp_conn_id=GCP_CONN_ID,
            location=GCS_BIGQUERY_REGION,
        )
        

 
        #Task flow
        Start >> Task_Print_config_variables >> oracle_to_gcs_task >> bq_to_bq_merge_task >> End