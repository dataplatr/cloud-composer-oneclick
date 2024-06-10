import Run_Audit_Module_Ankit

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