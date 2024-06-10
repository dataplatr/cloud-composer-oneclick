'''
Code Author: manoj.kumar@dataplatr.com
   Copyright: 2024
   Status: Dev
   Description: This DAG is used to trigger the Dataform Jobs 
'''
# Import libraries - 
import sys
import os, json
#import logging
from datetime import timedelta,datetime
from airflow.models import DAG              #library to use DAG code , main class for geenerating DAG graph and execution flow.
from airflow.utils.dates import days_ago    # ibrary used in dag schedule definiton
from airflow.models import Variable         #Library to import Airflow variables defined at Airflow UI
from airflow.operators.dummy_operator import DummyOperator    #Library to import dummy operator to create dummy tasks for execution -> Start and End Dag Tasks uses this library
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
import pendulum                             #Library used to set right timezone.
from google.cloud.dataform_v1beta1 import WorkflowInvocation
from airflow.utils.state import State
from airflow.providers.google.cloud.operators.dataform import (
    DataformCancelWorkflowInvocationOperator,
    DataformCreateCompilationResultOperator,
    DataformCreateWorkflowInvocationOperator,
    DataformGetCompilationResultOperator,
    DataformGetWorkflowInvocationOperator,
)
from airflow.utils.log.logging_mixin import LoggingMixin
#logger = logging.getLogger(__name__)

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
    
    GCP_CONN_ID = "Sandbox_BQ_SA_Connection"
elif Environment =='Qa':
   
    GCP_CONN_ID = "Sandbox_BQ_SA_Connection"
else:
    
    GCP_CONN_ID = "Sandbox_BQ_SA_Connection"
    

# ********* Initialize Configurations *********
configurations_path = "/home/airflow/gcs/dags/{}/HeadCount/Configurations/".format(Environment)
common_var_path = "/home/airflow/gcs/dags/{}/HeadCount/Configurations/Common/".format(Environment)
common_var = common_var_path + "common_variables.json" #"Common_Variables.txt"
dag_var_path = "/home/airflow/gcs/dags/{}/HeadCount/Configurations/{}/Variables/".format(Environment,Dag_Name)
dag_var = dag_var_path + "Dag_Variables.json" #"dag_variables.txt" 

# Import the read_json_file module from utility folder
utility_folder_path="/home/airflow/gcs/dags/{}/CommonPattern".format(Environment)
sys.path.append(utility_folder_path)
import Read_config_file
import Run_Audit_module
# returns configs from json files in map variable format based on the  environment settings.
common_variables = Read_config_file.read_json(common_var)
dag_variables = Read_config_file.read_json(dag_var)


#  ******* DAG Code Starts here *********
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
    tags=[path_elements[6], Environment],
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
    

    # task to create dataform compilation result
    create_compilation_result = DataformCreateCompilationResultOperator(
        task_id="create_compilation_result",
        project_id=common_variables['Project'],
        region=common_variables['DF_Region'],
        repository_id=common_variables['DF_Repository_Id'],
        compilation_result={
            "git_commitish": common_variables['DF_GitBranch']
        },
        gcp_conn_id=GCP_CONN_ID,
    )
    # task to create dataform workflow invocation
    create_workflow_invocation = DataformCreateWorkflowInvocationOperator(
        task_id='create_workflow_invocation',
        project_id=common_variables['Project'],
        region=common_variables['DF_Region'],
        repository_id=common_variables['DF_Repository_Id'],
         workflow_invocation={
            "compilation_result": "{{ task_instance.xcom_pull('create_compilation_result')['name'] }}",
            "invocation_config": { "included_tags": {dag_variables['DF_Tags']}, "transitive_dependencies_included": True }
        },
        gcp_conn_id=GCP_CONN_ID,
    )
    #End task - Dummy task
    End = DummyOperator(
        task_id = 'End',
    )
   
   
    Start >> Task_Print_config_variables  >> create_compilation_result   >> create_workflow_invocation >> End