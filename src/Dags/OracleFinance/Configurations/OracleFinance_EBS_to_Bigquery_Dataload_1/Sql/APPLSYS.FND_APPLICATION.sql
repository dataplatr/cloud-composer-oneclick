MERGE Ods.`OracleEbs-FND_APPLICATION` AS T using OdsStage.`OracleEbs-FND_APPLICATION` AS S on T.APPLICATION_ID = S.APPLICATION_ID
WHEN MATCHED THEN
Update SET
T.ParentSystemId = 76
,T.SystemId = 2578		 
,T.APPLICATION_ID	=	S.APPLICATION_ID
,T.APPLICATION_SHORT_NAME	=	S.APPLICATION_SHORT_NAME
,T.LAST_UPDATE_DATE	=	S.LAST_UPDATE_DATE
,T.LAST_UPDATED_BY	=	S.LAST_UPDATED_BY
,T.CREATION_DATE	=	S.CREATION_DATE
,T.CREATED_BY	=	S.CREATED_BY
,T.LAST_UPDATE_LOGIN	=	S.LAST_UPDATE_LOGIN
,T.BASEPATH	=	S.BASEPATH
,T.PRODUCT_CODE	=	S.PRODUCT_CODE
,T.LoadBy = 'airflow-worker'
,T.LoadDate = CURRENT_TIMESTAMP
,T.LoadProcess = 'Dag - Corp_OracleEbs_BigQuery_AP_HOLD_CODES_Import'
,T.CreateBy = 'airflow-worker'
,T.CreateDate = CURRENT_TIMESTAMP
,T.CreateProcess = 'Dag - Corp_OracleEbs_BigQuery_AP_HOLD_CODES_Import'
,T.UpdateBy = 'airflow-worker'
,T.UpdateDate = CURRENT_TIMESTAMP
,T.UpdateProcess = 'Dag - Corp_OracleEbs_BigQuery_AP_HOLD_CODES_Import'
,T.InactiveInd = False
,T.InactiveReason = ''

	WHEN NOT MATCHED THEN
INSERT (ParentSystemId,SystemId,APPLICATION_ID, APPLICATION_SHORT_NAME, LAST_UPDATE_DATE, LAST_UPDATED_BY, CREATION_DATE, CREATED_BY, LAST_UPDATE_LOGIN, BASEPATH, PRODUCT_CODE, LoadBy, LoadDate, LoadProcess,CreateBy, CreateDate, CreateProcess,UpdateBy,UpdateDate,UpdateProcess,InactiveInd,InactiveReason)
VALUES (76,2578,APPLICATION_ID, APPLICATION_SHORT_NAME, LAST_UPDATE_DATE, LAST_UPDATED_BY, CREATION_DATE, CREATED_BY, LAST_UPDATE_LOGIN, BASEPATH, PRODUCT_CODE,'airflow-worker',CURRENT_TIMESTAMP,'Dag - Corp_OracleEbs_BigQuery_AP_HOLD_CODES_Import','airflow-worker',CURRENT_TIMESTAMP,'Dag - Corp_OracleEbs_BigQuery_AP_HOLD_CODES_Import','airflow-worker',CURRENT_TIMESTAMP,'Dag - Corp_OracleEbs_BigQuery_AP_HOLD_CODES_Import',False,'')