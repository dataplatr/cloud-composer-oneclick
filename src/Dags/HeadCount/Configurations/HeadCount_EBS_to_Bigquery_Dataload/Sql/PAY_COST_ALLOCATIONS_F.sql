MERGE Ods.`OracleEbs-PAY_COST_ALLOCATIONS_F` AS T using OdsStage.`OracleEbs-PAY_COST_ALLOCATIONS_F` AS S on 
T.COST_ALLOCATION_ID = S.COST_ALLOCATION_ID and
T.EFFECTIVE_START_DATE = S.EFFECTIVE_START_DATE and
T.EFFECTIVE_END_DATE = S.EFFECTIVE_END_DATE
 
WHEN MATCHED THEN
Update SET
 T.ParentSystemId  =  S.ParentSystemId
,T.SystemId  =  S.SystemId
,T.COST_ALLOCATION_ID  =  S.COST_ALLOCATION_ID
,T.EFFECTIVE_START_DATE  =  S.EFFECTIVE_START_DATE
,T.EFFECTIVE_END_DATE  =  S.EFFECTIVE_END_DATE
,T.BUSINESS_GROUP_ID  =  S.BUSINESS_GROUP_ID
,T.COST_ALLOCATION_KEYFLEX_ID  =  S.COST_ALLOCATION_KEYFLEX_ID
,T.ASSIGNMENT_ID  =  S.ASSIGNMENT_ID
,T.PROPORTION  =  S.PROPORTION
,T.REQUEST_ID  =  S.REQUEST_ID
,T.PROGRAM_APPLICATION_ID  =  S.PROGRAM_APPLICATION_ID
,T.PROGRAM_ID  =  S.PROGRAM_ID
,T.PROGRAM_UPDATE_DATE  =  S.PROGRAM_UPDATE_DATE
,T.LAST_UPDATE_DATE  =  S.LAST_UPDATE_DATE
,T.LAST_UPDATED_BY  =  S.LAST_UPDATED_BY
,T.LAST_UPDATE_LOGIN  =  S.LAST_UPDATE_LOGIN
,T.CREATED_BY  =  S.CREATED_BY
,T.CREATION_DATE  =  S.CREATION_DATE
,T.OBJECT_VERSION_NUMBER  =  S.OBJECT_VERSION_NUMBER
,T.LoadBy = 'airflow-worker'
,T.LoadDate = CURRENT_TIMESTAMP
,T.LoadProcess = 'Dag - Corp_OracleEbs_BigQuery_PAY_COST_ALLOCATIONS_F_Import'
,T.CreateBy = 'airflow-worker'
,T.CreateDate = CURRENT_TIMESTAMP
,T.CreateProcess = 'Dag - Corp_OracleEbs_BigQuery_PAY_COST_ALLOCATIONS_F_Import'
,T.UpdateBy = 'airflow-worker'
,T.UpdateDate = CURRENT_TIMESTAMP
,T.UpdateProcess = 'Dag - Corp_OracleEbs_BigQuery_PAY_COST_ALLOCATIONS_F_Import'
,T.InactiveInd = False
,T.InactiveReason = ''

	WHEN NOT MATCHED THEN

INSERT (ParentSystemId,	SystemId,	COST_ALLOCATION_ID,	EFFECTIVE_START_DATE,	EFFECTIVE_END_DATE,	BUSINESS_GROUP_ID,	COST_ALLOCATION_KEYFLEX_ID,	ASSIGNMENT_ID,	PROPORTION,	REQUEST_ID,	PROGRAM_APPLICATION_ID,	PROGRAM_ID,	PROGRAM_UPDATE_DATE,	LAST_UPDATE_DATE,	LAST_UPDATED_BY,	LAST_UPDATE_LOGIN,	CREATED_BY,	CREATION_DATE,	OBJECT_VERSION_NUMBER,LoadBy,	LoadDate,	LoadProcess,	CreateBy,	CreateDate,	CreateProcess,	UpdateBy,	UpdateDate,	UpdateProcess,	InactiveInd,	InactiveReason )

VALUES (ParentSystemId,	SystemId,	COST_ALLOCATION_ID,	EFFECTIVE_START_DATE,	EFFECTIVE_END_DATE,	BUSINESS_GROUP_ID,	COST_ALLOCATION_KEYFLEX_ID,	ASSIGNMENT_ID,	PROPORTION,	REQUEST_ID,	PROGRAM_APPLICATION_ID,	PROGRAM_ID,	PROGRAM_UPDATE_DATE,	LAST_UPDATE_DATE,	LAST_UPDATED_BY,	LAST_UPDATE_LOGIN,	CREATED_BY,	CREATION_DATE,	OBJECT_VERSION_NUMBER,'airflow-worker',CURRENT_TIMESTAMP,'Dag - Corp_OracleEbs_BigQuery_PAY_COST_ALLOCATIONS_F_Import','airflow-worker',CURRENT_TIMESTAMP,'Dag - Corp_OracleEbs_BigQuery_PAY_COST_ALLOCATIONS_F_Import','airflow-worker',CURRENT_TIMESTAMP,'Dag - Corp_OracleEbs_BigQuery_PAY_COST_ALLOCATIONS_F_Import',False,'')