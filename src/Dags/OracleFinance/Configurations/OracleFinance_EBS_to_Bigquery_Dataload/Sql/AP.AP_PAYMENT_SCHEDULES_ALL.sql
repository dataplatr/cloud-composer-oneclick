MERGE Ods.`OracleEbs-AP_PAYMENT_SCHEDULES_ALL` AS T using OdsStage.`OracleEbs-AP_PAYMENT_SCHEDULES_ALL` AS S on T.INVOICE_ID = S.INVOICE_ID and T.PAYMENT_NUM = S.PAYMENT_NUM
WHEN MATCHED THEN
Update SET
T.ParentSystemId = 76
,T.SystemId = 2578		 
,T.INVOICE_ID	=	S.INVOICE_ID
,T.LAST_UPDATED_BY	=	S.LAST_UPDATED_BY
,T.LAST_UPDATE_DATE	=	S.LAST_UPDATE_DATE
,T.PAYMENT_CROSS_RATE	=	S.PAYMENT_CROSS_RATE
,T.PAYMENT_NUM	=	S.PAYMENT_NUM
,T.AMOUNT_REMAINING	=	S.AMOUNT_REMAINING
,T.CREATED_BY	=	S.CREATED_BY
,T.CREATION_DATE	=	S.CREATION_DATE
,T.DISCOUNT_DATE	=	S.DISCOUNT_DATE
,T.DUE_DATE	=	S.DUE_DATE
,T.FUTURE_PAY_DUE_DATE	=	S.FUTURE_PAY_DUE_DATE
,T.GROSS_AMOUNT	=	S.GROSS_AMOUNT
,T.HOLD_FLAG	=	S.HOLD_FLAG
,T.LAST_UPDATE_LOGIN	=	S.LAST_UPDATE_LOGIN
,T.PAYMENT_METHOD_LOOKUP_CODE	=	S.PAYMENT_METHOD_LOOKUP_CODE
,T.PAYMENT_PRIORITY	=	S.PAYMENT_PRIORITY
,T.PAYMENT_STATUS_FLAG	=	S.PAYMENT_STATUS_FLAG
,T.SECOND_DISCOUNT_DATE	=	S.SECOND_DISCOUNT_DATE
,T.THIRD_DISCOUNT_DATE	=	S.THIRD_DISCOUNT_DATE
,T.BATCH_ID	=	S.BATCH_ID
,T.DISCOUNT_AMOUNT_AVAILABLE	=	S.DISCOUNT_AMOUNT_AVAILABLE
,T.SECOND_DISC_AMT_AVAILABLE	=	S.SECOND_DISC_AMT_AVAILABLE
,T.THIRD_DISC_AMT_AVAILABLE	=	S.THIRD_DISC_AMT_AVAILABLE
,T.ATTRIBUTE1	=	S.ATTRIBUTE1
,T.ATTRIBUTE10	=	S.ATTRIBUTE10
,T.ATTRIBUTE11	=	S.ATTRIBUTE11
,T.ATTRIBUTE12	=	S.ATTRIBUTE12
,T.ATTRIBUTE13	=	S.ATTRIBUTE13
,T.ATTRIBUTE14	=	S.ATTRIBUTE14
,T.ATTRIBUTE15	=	S.ATTRIBUTE15
,T.ATTRIBUTE2	=	S.ATTRIBUTE2
,T.ATTRIBUTE3	=	S.ATTRIBUTE3
,T.ATTRIBUTE4	=	S.ATTRIBUTE4
,T.ATTRIBUTE5	=	S.ATTRIBUTE5
,T.ATTRIBUTE6	=	S.ATTRIBUTE6
,T.ATTRIBUTE7	=	S.ATTRIBUTE7
,T.ATTRIBUTE8	=	S.ATTRIBUTE8
,T.ATTRIBUTE9	=	S.ATTRIBUTE9
,T.ATTRIBUTE_CATEGORY	=	S.ATTRIBUTE_CATEGORY
,T.DISCOUNT_AMOUNT_REMAINING	=	S.DISCOUNT_AMOUNT_REMAINING
,T.ORG_ID	=	S.ORG_ID
,T.GLOBAL_ATTRIBUTE_CATEGORY	=	S.GLOBAL_ATTRIBUTE_CATEGORY
,T.GLOBAL_ATTRIBUTE1	=	S.GLOBAL_ATTRIBUTE1
,T.GLOBAL_ATTRIBUTE2	=	S.GLOBAL_ATTRIBUTE2
,T.GLOBAL_ATTRIBUTE3	=	S.GLOBAL_ATTRIBUTE3
,T.GLOBAL_ATTRIBUTE4	=	S.GLOBAL_ATTRIBUTE4
,T.GLOBAL_ATTRIBUTE5	=	S.GLOBAL_ATTRIBUTE5
,T.GLOBAL_ATTRIBUTE6	=	S.GLOBAL_ATTRIBUTE6
,T.GLOBAL_ATTRIBUTE7	=	S.GLOBAL_ATTRIBUTE7
,T.GLOBAL_ATTRIBUTE8	=	S.GLOBAL_ATTRIBUTE8
,T.GLOBAL_ATTRIBUTE9	=	S.GLOBAL_ATTRIBUTE9
,T.GLOBAL_ATTRIBUTE10	=	S.GLOBAL_ATTRIBUTE10
,T.GLOBAL_ATTRIBUTE11	=	S.GLOBAL_ATTRIBUTE11
,T.GLOBAL_ATTRIBUTE12	=	S.GLOBAL_ATTRIBUTE12
,T.GLOBAL_ATTRIBUTE13	=	S.GLOBAL_ATTRIBUTE13
,T.GLOBAL_ATTRIBUTE14	=	S.GLOBAL_ATTRIBUTE14
,T.GLOBAL_ATTRIBUTE15	=	S.GLOBAL_ATTRIBUTE15
,T.GLOBAL_ATTRIBUTE16	=	S.GLOBAL_ATTRIBUTE16
,T.GLOBAL_ATTRIBUTE17	=	S.GLOBAL_ATTRIBUTE17
,T.GLOBAL_ATTRIBUTE18	=	S.GLOBAL_ATTRIBUTE18
,T.GLOBAL_ATTRIBUTE19	=	S.GLOBAL_ATTRIBUTE19
,T.GLOBAL_ATTRIBUTE20	=	S.GLOBAL_ATTRIBUTE20
,T.EXTERNAL_BANK_ACCOUNT_ID	=	S.EXTERNAL_BANK_ACCOUNT_ID
,T.INV_CURR_GROSS_AMOUNT	=	S.INV_CURR_GROSS_AMOUNT
,T.CHECKRUN_ID	=	S.CHECKRUN_ID
,T.DBI_EVENTS_COMPLETE_FLAG	=	S.DBI_EVENTS_COMPLETE_FLAG
,T.IBY_HOLD_REASON	=	S.IBY_HOLD_REASON
,T.PAYMENT_METHOD_CODE	=	S.PAYMENT_METHOD_CODE
,T.REMITTANCE_MESSAGE1	=	S.REMITTANCE_MESSAGE1
,T.REMITTANCE_MESSAGE2	=	S.REMITTANCE_MESSAGE2
,T.REMITTANCE_MESSAGE3	=	S.REMITTANCE_MESSAGE3
,T.REMIT_TO_SUPPLIER_NAME	=	S.REMIT_TO_SUPPLIER_NAME
,T.REMIT_TO_SUPPLIER_ID	=	S.REMIT_TO_SUPPLIER_ID
,T.REMIT_TO_SUPPLIER_SITE	=	S.REMIT_TO_SUPPLIER_SITE
,T.REMIT_TO_SUPPLIER_SITE_ID	=	S.REMIT_TO_SUPPLIER_SITE_ID
,T.RELATIONSHIP_ID	=	S.RELATIONSHIP_ID
,T.LoadBy = 'airflow-worker'
,T.LoadDate = CURRENT_TIMESTAMP
,T.LoadProcess = 'Dag - Corp_OracleEbs_BigQuery_AP_PAYMENT_SCHEDULES_ALL_Import'
,T.CreateBy = 'airflow-worker'
,T.CreateDate = CURRENT_TIMESTAMP
,T.CreateProcess = 'Dag - Corp_OracleEbs_BigQuery_AP_PAYMENT_SCHEDULES_ALL_Import'
,T.UpdateBy = 'airflow-worker'
,T.UpdateDate = CURRENT_TIMESTAMP
,T.UpdateProcess = 'Dag - Corp_OracleEbs_BigQuery_AP_PAYMENT_SCHEDULES_ALL_Import'
,T.InactiveInd = False
,T.InactiveReason = ''

	WHEN NOT MATCHED THEN
INSERT (ParentSystemId,SystemId,INVOICE_ID,	LAST_UPDATED_BY,	LAST_UPDATE_DATE,	PAYMENT_CROSS_RATE,	PAYMENT_NUM,	AMOUNT_REMAINING,	CREATED_BY,	CREATION_DATE,	DISCOUNT_DATE,	DUE_DATE,	FUTURE_PAY_DUE_DATE,	GROSS_AMOUNT,	HOLD_FLAG,	LAST_UPDATE_LOGIN,	PAYMENT_METHOD_LOOKUP_CODE,	PAYMENT_PRIORITY,	PAYMENT_STATUS_FLAG,	SECOND_DISCOUNT_DATE,	THIRD_DISCOUNT_DATE,	BATCH_ID,	DISCOUNT_AMOUNT_AVAILABLE,	SECOND_DISC_AMT_AVAILABLE,	THIRD_DISC_AMT_AVAILABLE,	ATTRIBUTE1,	ATTRIBUTE10,	ATTRIBUTE11,	ATTRIBUTE12,	ATTRIBUTE13,	ATTRIBUTE14,	ATTRIBUTE15,	ATTRIBUTE2,	ATTRIBUTE3,	ATTRIBUTE4,	ATTRIBUTE5,	ATTRIBUTE6,	ATTRIBUTE7,	ATTRIBUTE8,	ATTRIBUTE9,	ATTRIBUTE_CATEGORY,	DISCOUNT_AMOUNT_REMAINING,	ORG_ID,	GLOBAL_ATTRIBUTE_CATEGORY,	GLOBAL_ATTRIBUTE1,	GLOBAL_ATTRIBUTE2,	GLOBAL_ATTRIBUTE3,	GLOBAL_ATTRIBUTE4,	GLOBAL_ATTRIBUTE5,	GLOBAL_ATTRIBUTE6,	GLOBAL_ATTRIBUTE7,	GLOBAL_ATTRIBUTE8,	GLOBAL_ATTRIBUTE9,	GLOBAL_ATTRIBUTE10,	GLOBAL_ATTRIBUTE11,	GLOBAL_ATTRIBUTE12,	GLOBAL_ATTRIBUTE13,	GLOBAL_ATTRIBUTE14,	GLOBAL_ATTRIBUTE15,	GLOBAL_ATTRIBUTE16,	GLOBAL_ATTRIBUTE17,	GLOBAL_ATTRIBUTE18,	GLOBAL_ATTRIBUTE19,	GLOBAL_ATTRIBUTE20,	EXTERNAL_BANK_ACCOUNT_ID,	INV_CURR_GROSS_AMOUNT,	CHECKRUN_ID,	DBI_EVENTS_COMPLETE_FLAG,	IBY_HOLD_REASON,	PAYMENT_METHOD_CODE,	REMITTANCE_MESSAGE1,	REMITTANCE_MESSAGE2,	REMITTANCE_MESSAGE3,	REMIT_TO_SUPPLIER_NAME,	REMIT_TO_SUPPLIER_ID,	REMIT_TO_SUPPLIER_SITE,	REMIT_TO_SUPPLIER_SITE_ID,	RELATIONSHIP_ID, LoadBy, LoadDate, LoadProcess,CreateBy, CreateDate, CreateProcess,UpdateBy,UpdateDate,UpdateProcess,InactiveInd,InactiveReason)
VALUES (76,2578,INVOICE_ID,	LAST_UPDATED_BY,	LAST_UPDATE_DATE,	PAYMENT_CROSS_RATE,	PAYMENT_NUM,	AMOUNT_REMAINING,	CREATED_BY,	CREATION_DATE,	DISCOUNT_DATE,	DUE_DATE,	FUTURE_PAY_DUE_DATE,	GROSS_AMOUNT,	HOLD_FLAG,	LAST_UPDATE_LOGIN,	PAYMENT_METHOD_LOOKUP_CODE,	PAYMENT_PRIORITY,	PAYMENT_STATUS_FLAG,	SECOND_DISCOUNT_DATE,	THIRD_DISCOUNT_DATE,	BATCH_ID,	DISCOUNT_AMOUNT_AVAILABLE,	SECOND_DISC_AMT_AVAILABLE,	THIRD_DISC_AMT_AVAILABLE,	ATTRIBUTE1,	ATTRIBUTE10,	ATTRIBUTE11,	ATTRIBUTE12,	ATTRIBUTE13,	ATTRIBUTE14,	ATTRIBUTE15,	ATTRIBUTE2,	ATTRIBUTE3,	ATTRIBUTE4,	ATTRIBUTE5,	ATTRIBUTE6,	ATTRIBUTE7,	ATTRIBUTE8,	ATTRIBUTE9,	ATTRIBUTE_CATEGORY,	DISCOUNT_AMOUNT_REMAINING,	ORG_ID,	GLOBAL_ATTRIBUTE_CATEGORY,	GLOBAL_ATTRIBUTE1,	GLOBAL_ATTRIBUTE2,	GLOBAL_ATTRIBUTE3,	GLOBAL_ATTRIBUTE4,	GLOBAL_ATTRIBUTE5,	GLOBAL_ATTRIBUTE6,	GLOBAL_ATTRIBUTE7,	GLOBAL_ATTRIBUTE8,	GLOBAL_ATTRIBUTE9,	GLOBAL_ATTRIBUTE10,	GLOBAL_ATTRIBUTE11,	GLOBAL_ATTRIBUTE12,	GLOBAL_ATTRIBUTE13,	GLOBAL_ATTRIBUTE14,	GLOBAL_ATTRIBUTE15,	GLOBAL_ATTRIBUTE16,	GLOBAL_ATTRIBUTE17,	GLOBAL_ATTRIBUTE18,	GLOBAL_ATTRIBUTE19,	GLOBAL_ATTRIBUTE20,	EXTERNAL_BANK_ACCOUNT_ID,	INV_CURR_GROSS_AMOUNT,	CHECKRUN_ID,	DBI_EVENTS_COMPLETE_FLAG,	IBY_HOLD_REASON,	PAYMENT_METHOD_CODE,	REMITTANCE_MESSAGE1,	REMITTANCE_MESSAGE2,	REMITTANCE_MESSAGE3,	REMIT_TO_SUPPLIER_NAME,	REMIT_TO_SUPPLIER_ID,	REMIT_TO_SUPPLIER_SITE,	REMIT_TO_SUPPLIER_SITE_ID,	RELATIONSHIP_ID, 'airflow-worker',CURRENT_TIMESTAMP,'Dag - Corp_OracleEbs_BigQuery_AP_PAYMENT_SCHEDULES_ALL_Import','airflow-worker',CURRENT_TIMESTAMP,'Dag - Corp_OracleEbs_BigQuery_AP_PAYMENT_SCHEDULES_ALL_Import','airflow-worker',CURRENT_TIMESTAMP,'Dag - Corp_OracleEbs_BigQuery_AP_PAYMENT_SCHEDULES_ALL_Import',False,'')