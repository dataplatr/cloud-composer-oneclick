MERGE Ods.`OracleEbs-AR_DISTRIBUTIONS_ALL` AS T using OdsStage.`OracleEbs-AR_DISTRIBUTIONS_ALL` AS S on T.LINE_ID = S.LINE_ID
WHEN MATCHED THEN
Update SET
T.ParentSystemId = 76
,T.SystemId = 2578		 
,T.LINE_ID	=	S.LINE_ID
,T.SOURCE_ID	=	S.SOURCE_ID
,T.SOURCE_TABLE	=	S.SOURCE_TABLE
,T.SOURCE_TYPE	=	S.SOURCE_TYPE
,T.CODE_COMBINATION_ID	=	S.CODE_COMBINATION_ID
,T.AMOUNT_DR	=	S.AMOUNT_DR
,T.AMOUNT_CR	=	S.AMOUNT_CR
,T.ACCTD_AMOUNT_DR	=	S.ACCTD_AMOUNT_DR
,T.ACCTD_AMOUNT_CR	=	S.ACCTD_AMOUNT_CR
,T.CREATION_DATE	=	S.CREATION_DATE
,T.CREATED_BY	=	S.CREATED_BY
,T.LAST_UPDATED_BY	=	S.LAST_UPDATED_BY
,T.LAST_UPDATE_DATE	=	S.LAST_UPDATE_DATE
,T.LAST_UPDATE_LOGIN	=	S.LAST_UPDATE_LOGIN
,T.ORG_ID	=	S.ORG_ID
,T.SOURCE_TABLE_SECONDARY	=	S.SOURCE_TABLE_SECONDARY
,T.SOURCE_ID_SECONDARY	=	S.SOURCE_ID_SECONDARY
,T.CURRENCY_CODE	=	S.CURRENCY_CODE
,T.CURRENCY_CONVERSION_RATE	=	S.CURRENCY_CONVERSION_RATE
,T.CURRENCY_CONVERSION_TYPE	=	S.CURRENCY_CONVERSION_TYPE
,T.CURRENCY_CONVERSION_DATE	=	S.CURRENCY_CONVERSION_DATE
,T.TAXABLE_ENTERED_DR	=	S.TAXABLE_ENTERED_DR
,T.TAXABLE_ENTERED_CR	=	S.TAXABLE_ENTERED_CR
,T.TAXABLE_ACCOUNTED_DR	=	S.TAXABLE_ACCOUNTED_DR
,T.TAXABLE_ACCOUNTED_CR	=	S.TAXABLE_ACCOUNTED_CR
,T.TAX_LINK_ID	=	S.TAX_LINK_ID
,T.THIRD_PARTY_ID	=	S.THIRD_PARTY_ID
,T.THIRD_PARTY_SUB_ID	=	S.THIRD_PARTY_SUB_ID
,T.REVERSED_SOURCE_ID	=	S.REVERSED_SOURCE_ID
,T.TAX_CODE_ID	=	S.TAX_CODE_ID
,T.LOCATION_SEGMENT_ID	=	S.LOCATION_SEGMENT_ID
,T.SOURCE_TYPE_SECONDARY	=	S.SOURCE_TYPE_SECONDARY
,T.TAX_GROUP_CODE_ID	=	S.TAX_GROUP_CODE_ID
,T.REF_CUSTOMER_TRX_LINE_ID	=	S.REF_CUSTOMER_TRX_LINE_ID
,T.REF_CUST_TRX_LINE_GL_DIST_ID	=	S.REF_CUST_TRX_LINE_GL_DIST_ID
,T.REF_ACCOUNT_CLASS	=	S.REF_ACCOUNT_CLASS
,T.ACTIVITY_BUCKET	=	S.ACTIVITY_BUCKET
,T.REF_LINE_ID	=	S.REF_LINE_ID
,T.FROM_AMOUNT_DR	=	S.FROM_AMOUNT_DR
,T.FROM_AMOUNT_CR	=	S.FROM_AMOUNT_CR
,T.FROM_ACCTD_AMOUNT_DR	=	S.FROM_ACCTD_AMOUNT_DR
,T.FROM_ACCTD_AMOUNT_CR	=	S.FROM_ACCTD_AMOUNT_CR
,T.REF_MF_DIST_FLAG	=	S.REF_MF_DIST_FLAG
,T.REF_DIST_CCID	=	S.REF_DIST_CCID
,T.REF_PREV_CUST_TRX_LINE_ID	=	S.REF_PREV_CUST_TRX_LINE_ID
,T.LoadBy = 'airflow-worker'
,T.LoadDate = CURRENT_TIMESTAMP
,T.LoadProcess = 'Dag - Corp_OracleEbs_BigQuery_AR_DISTRIBUTIONS_ALL_Import'
,T.CreateBy = 'airflow-worker'
,T.CreateDate = CURRENT_TIMESTAMP
,T.CreateProcess = 'Dag - Corp_OracleEbs_BigQuery_AR_DISTRIBUTIONS_ALL_Import'
,T.UpdateBy = 'airflow-worker'
,T.UpdateDate = CURRENT_TIMESTAMP
,T.UpdateProcess = 'Dag - Corp_OracleEbs_BigQuery_AR_DISTRIBUTIONS_ALL_Import'
,T.InactiveInd = False
,T.InactiveReason = ''

	WHEN NOT MATCHED THEN
INSERT (ParentSystemId,SystemId,LINE_ID,	SOURCE_ID,	SOURCE_TABLE,	SOURCE_TYPE,	CODE_COMBINATION_ID,	AMOUNT_DR,	AMOUNT_CR,	ACCTD_AMOUNT_DR,	ACCTD_AMOUNT_CR,	CREATION_DATE,	CREATED_BY,	LAST_UPDATED_BY,	LAST_UPDATE_DATE,	LAST_UPDATE_LOGIN,	ORG_ID,	SOURCE_TABLE_SECONDARY,	SOURCE_ID_SECONDARY,	CURRENCY_CODE,	CURRENCY_CONVERSION_RATE,	CURRENCY_CONVERSION_TYPE,	CURRENCY_CONVERSION_DATE,	TAXABLE_ENTERED_DR,	TAXABLE_ENTERED_CR,	TAXABLE_ACCOUNTED_DR,	TAXABLE_ACCOUNTED_CR,	TAX_LINK_ID,	THIRD_PARTY_ID,	THIRD_PARTY_SUB_ID,	REVERSED_SOURCE_ID,	TAX_CODE_ID,	LOCATION_SEGMENT_ID,	SOURCE_TYPE_SECONDARY,	TAX_GROUP_CODE_ID,	REF_CUSTOMER_TRX_LINE_ID,	REF_CUST_TRX_LINE_GL_DIST_ID,	REF_ACCOUNT_CLASS,	ACTIVITY_BUCKET,	REF_LINE_ID,	FROM_AMOUNT_DR,	FROM_AMOUNT_CR,	FROM_ACCTD_AMOUNT_DR,	FROM_ACCTD_AMOUNT_CR,	REF_MF_DIST_FLAG,	REF_DIST_CCID,	REF_PREV_CUST_TRX_LINE_ID, LoadBy, LoadDate, LoadProcess,CreateBy, CreateDate, CreateProcess,UpdateBy,UpdateDate,UpdateProcess,InactiveInd,InactiveReason)
VALUES (76,2578,LINE_ID,	SOURCE_ID,	SOURCE_TABLE,	SOURCE_TYPE,	CODE_COMBINATION_ID,	AMOUNT_DR,	AMOUNT_CR,	ACCTD_AMOUNT_DR,	ACCTD_AMOUNT_CR,	CREATION_DATE,	CREATED_BY,	LAST_UPDATED_BY,	LAST_UPDATE_DATE,	LAST_UPDATE_LOGIN,	ORG_ID,	SOURCE_TABLE_SECONDARY,	SOURCE_ID_SECONDARY,	CURRENCY_CODE,	CURRENCY_CONVERSION_RATE,	CURRENCY_CONVERSION_TYPE,	CURRENCY_CONVERSION_DATE,	TAXABLE_ENTERED_DR,	TAXABLE_ENTERED_CR,	TAXABLE_ACCOUNTED_DR,	TAXABLE_ACCOUNTED_CR,	TAX_LINK_ID,	THIRD_PARTY_ID,	THIRD_PARTY_SUB_ID,	REVERSED_SOURCE_ID,	TAX_CODE_ID,	LOCATION_SEGMENT_ID,	SOURCE_TYPE_SECONDARY,	TAX_GROUP_CODE_ID,	REF_CUSTOMER_TRX_LINE_ID,	REF_CUST_TRX_LINE_GL_DIST_ID,	REF_ACCOUNT_CLASS,	ACTIVITY_BUCKET,	REF_LINE_ID,	FROM_AMOUNT_DR,	FROM_AMOUNT_CR,	FROM_ACCTD_AMOUNT_DR,	FROM_ACCTD_AMOUNT_CR,	REF_MF_DIST_FLAG,	REF_DIST_CCID,	REF_PREV_CUST_TRX_LINE_ID,'airflow-worker',CURRENT_TIMESTAMP,'Dag - Corp_OracleEbs_BigQuery_AR_DISTRIBUTIONS_ALL_Import','airflow-worker',CURRENT_TIMESTAMP,'Dag - Corp_OracleEbs_BigQuery_AR_DISTRIBUTIONS_ALL_Import','airflow-worker',CURRENT_TIMESTAMP,'Dag - Corp_OracleEbs_BigQuery_AR_DISTRIBUTIONS_ALL_Import',False,'')