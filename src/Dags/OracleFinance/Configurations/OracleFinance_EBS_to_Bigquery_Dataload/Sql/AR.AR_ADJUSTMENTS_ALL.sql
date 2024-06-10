MERGE Ods.`OracleEbs-AR_ADJUSTMENTS_ALL` AS T using OdsStage.`OracleEbs-AR_ADJUSTMENTS_ALL` AS S on T.ADJUSTMENT_ID = S.ADJUSTMENT_ID
WHEN MATCHED THEN
Update SET
T.ParentSystemId = 76
,T.SystemId = 2578		 
,T.ADJUSTMENT_ID	=	S.ADJUSTMENT_ID
,T.LAST_UPDATED_BY	=	S.LAST_UPDATED_BY
,T.LAST_UPDATE_DATE	=	S.LAST_UPDATE_DATE
,T.LAST_UPDATE_LOGIN	=	S.LAST_UPDATE_LOGIN
,T.CREATED_BY	=	S.CREATED_BY
,T.CREATION_DATE	=	S.CREATION_DATE
,T.AMOUNT	=	S.AMOUNT
,T.APPLY_DATE	=	S.APPLY_DATE
,T.GL_DATE	=	S.GL_DATE
,T.SET_OF_BOOKS_ID	=	S.SET_OF_BOOKS_ID
,T.CODE_COMBINATION_ID	=	S.CODE_COMBINATION_ID
,T.TYPE	=	S.TYPE
,T.ADJUSTMENT_TYPE	=	S.ADJUSTMENT_TYPE
,T.STATUS	=	S.STATUS
,T.LINE_ADJUSTED	=	S.LINE_ADJUSTED
,T.FREIGHT_ADJUSTED	=	S.FREIGHT_ADJUSTED
,T.TAX_ADJUSTED	=	S.TAX_ADJUSTED
,T.RECEIVABLES_CHARGES_ADJUSTED	=	S.RECEIVABLES_CHARGES_ADJUSTED
,T.ASSOCIATED_CASH_RECEIPT_ID	=	S.ASSOCIATED_CASH_RECEIPT_ID
,T.CHARGEBACK_CUSTOMER_TRX_ID	=	S.CHARGEBACK_CUSTOMER_TRX_ID
,T.BATCH_ID	=	S.BATCH_ID
,T.CUSTOMER_TRX_ID	=	S.CUSTOMER_TRX_ID
,T.CUSTOMER_TRX_LINE_ID	=	S.CUSTOMER_TRX_LINE_ID
,T.SUBSEQUENT_TRX_ID	=	S.SUBSEQUENT_TRX_ID
,T.PAYMENT_SCHEDULE_ID	=	S.PAYMENT_SCHEDULE_ID
,T.RECEIVABLES_TRX_ID	=	S.RECEIVABLES_TRX_ID
,T.DISTRIBUTION_SET_ID	=	S.DISTRIBUTION_SET_ID
,T.GL_POSTED_DATE	=	S.GL_POSTED_DATE
,T.COMMENTS	=	S.COMMENTS
,T.AUTOMATICALLY_GENERATED	=	S.AUTOMATICALLY_GENERATED
,T.CREATED_FROM	=	S.CREATED_FROM
,T.REASON_CODE	=	S.REASON_CODE
,T.POSTABLE	=	S.POSTABLE
,T.APPROVED_BY	=	S.APPROVED_BY
,T.ATTRIBUTE_CATEGORY	=	S.ATTRIBUTE_CATEGORY
,T.ATTRIBUTE1	=	S.ATTRIBUTE1
,T.ATTRIBUTE2	=	S.ATTRIBUTE2
,T.ATTRIBUTE3	=	S.ATTRIBUTE3
,T.ATTRIBUTE4	=	S.ATTRIBUTE4
,T.ATTRIBUTE5	=	S.ATTRIBUTE5
,T.ATTRIBUTE6	=	S.ATTRIBUTE6
,T.ATTRIBUTE7	=	S.ATTRIBUTE7
,T.ATTRIBUTE8	=	S.ATTRIBUTE8
,T.ATTRIBUTE9	=	S.ATTRIBUTE9
,T.ATTRIBUTE10	=	S.ATTRIBUTE10
,T.POSTING_CONTROL_ID	=	S.POSTING_CONTROL_ID
,T.ACCTD_AMOUNT	=	S.ACCTD_AMOUNT
,T.ATTRIBUTE11	=	S.ATTRIBUTE11
,T.ATTRIBUTE12	=	S.ATTRIBUTE12
,T.ATTRIBUTE13	=	S.ATTRIBUTE13
,T.ATTRIBUTE14	=	S.ATTRIBUTE14
,T.ATTRIBUTE15	=	S.ATTRIBUTE15
,T.PROGRAM_APPLICATION_ID	=	S.PROGRAM_APPLICATION_ID
,T.PROGRAM_ID	=	S.PROGRAM_ID
,T.PROGRAM_UPDATE_DATE	=	S.PROGRAM_UPDATE_DATE
,T.REQUEST_ID	=	S.REQUEST_ID
,T.ADJUSTMENT_NUMBER	=	S.ADJUSTMENT_NUMBER
,T.ORG_ID	=	S.ORG_ID
,T.USSGL_TRANSACTION_CODE	=	S.USSGL_TRANSACTION_CODE
,T.USSGL_TRANSACTION_CODE_CONTEXT	=	S.USSGL_TRANSACTION_CODE_CONTEXT
,T.DOC_SEQUENCE_VALUE	=	S.DOC_SEQUENCE_VALUE
,T.DOC_SEQUENCE_ID	=	S.DOC_SEQUENCE_ID
,T.ASSOCIATED_APPLICATION_ID	=	S.ASSOCIATED_APPLICATION_ID
,T.CONS_INV_ID	=	S.CONS_INV_ID
,T.MRC_GL_POSTED_DATE	=	S.MRC_GL_POSTED_DATE
,T.MRC_POSTING_CONTROL_ID	=	S.MRC_POSTING_CONTROL_ID
,T.MRC_ACCTD_AMOUNT	=	S.MRC_ACCTD_AMOUNT
,T.ADJ_TAX_ACCT_RULE	=	S.ADJ_TAX_ACCT_RULE
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
,T.LINK_TO_TRX_HIST_ID	=	S.LINK_TO_TRX_HIST_ID
,T.EVENT_ID	=	S.EVENT_ID
,T.UPGRADE_METHOD	=	S.UPGRADE_METHOD
,T.AX_ACCOUNTED_FLAG	=	S.AX_ACCOUNTED_FLAG
,T.INTEREST_HEADER_ID	=	S.INTEREST_HEADER_ID
,T.INTEREST_LINE_ID	=	S.INTEREST_LINE_ID
,T.LoadBy = 'airflow-worker'
,T.LoadDate = CURRENT_TIMESTAMP
,T.LoadProcess = 'Dag - Corp_OracleEbs_BigQuery_AR_ADJUSTMENTS_ALL_Import'
,T.CreateBy = 'airflow-worker'
,T.CreateDate = CURRENT_TIMESTAMP
,T.CreateProcess = 'Dag - Corp_OracleEbs_BigQuery_AR_ADJUSTMENTS_ALL_Import'
,T.UpdateBy = 'airflow-worker'
,T.UpdateDate = CURRENT_TIMESTAMP
,T.UpdateProcess = 'Dag - Corp_OracleEbs_BigQuery_AR_ADJUSTMENTS_ALL_Import'
,T.InactiveInd = False
,T.InactiveReason = ''

	WHEN NOT MATCHED THEN
INSERT (ParentSystemId,SystemId,ADJUSTMENT_ID,	LAST_UPDATED_BY,	LAST_UPDATE_DATE,	LAST_UPDATE_LOGIN,	CREATED_BY,	CREATION_DATE,	AMOUNT,	APPLY_DATE,	GL_DATE,	SET_OF_BOOKS_ID,	CODE_COMBINATION_ID,	TYPE,	ADJUSTMENT_TYPE,	STATUS,	LINE_ADJUSTED,	FREIGHT_ADJUSTED,	TAX_ADJUSTED,	RECEIVABLES_CHARGES_ADJUSTED,	ASSOCIATED_CASH_RECEIPT_ID,	CHARGEBACK_CUSTOMER_TRX_ID,	BATCH_ID,	CUSTOMER_TRX_ID,	CUSTOMER_TRX_LINE_ID,	SUBSEQUENT_TRX_ID,	PAYMENT_SCHEDULE_ID,	RECEIVABLES_TRX_ID,	DISTRIBUTION_SET_ID,	GL_POSTED_DATE,	COMMENTS,	AUTOMATICALLY_GENERATED,	CREATED_FROM,	REASON_CODE,	POSTABLE,	APPROVED_BY,	ATTRIBUTE_CATEGORY,	ATTRIBUTE1,	ATTRIBUTE2,	ATTRIBUTE3,	ATTRIBUTE4,	ATTRIBUTE5,	ATTRIBUTE6,	ATTRIBUTE7,	ATTRIBUTE8,	ATTRIBUTE9,	ATTRIBUTE10,	POSTING_CONTROL_ID,	ACCTD_AMOUNT,	ATTRIBUTE11,	ATTRIBUTE12,	ATTRIBUTE13,	ATTRIBUTE14,	ATTRIBUTE15,	PROGRAM_APPLICATION_ID,	PROGRAM_ID,	PROGRAM_UPDATE_DATE,	REQUEST_ID,	ADJUSTMENT_NUMBER,	ORG_ID,	USSGL_TRANSACTION_CODE,	USSGL_TRANSACTION_CODE_CONTEXT,	DOC_SEQUENCE_VALUE,	DOC_SEQUENCE_ID,	ASSOCIATED_APPLICATION_ID,	CONS_INV_ID,	MRC_GL_POSTED_DATE,	MRC_POSTING_CONTROL_ID,	MRC_ACCTD_AMOUNT,	ADJ_TAX_ACCT_RULE,	GLOBAL_ATTRIBUTE_CATEGORY,	GLOBAL_ATTRIBUTE1,	GLOBAL_ATTRIBUTE2,	GLOBAL_ATTRIBUTE3,	GLOBAL_ATTRIBUTE4,	GLOBAL_ATTRIBUTE5,	GLOBAL_ATTRIBUTE6,	GLOBAL_ATTRIBUTE7,	GLOBAL_ATTRIBUTE8,	GLOBAL_ATTRIBUTE9,	GLOBAL_ATTRIBUTE10,	GLOBAL_ATTRIBUTE11,	GLOBAL_ATTRIBUTE12,	GLOBAL_ATTRIBUTE13,	GLOBAL_ATTRIBUTE14,	GLOBAL_ATTRIBUTE15,	GLOBAL_ATTRIBUTE16,	GLOBAL_ATTRIBUTE17,	GLOBAL_ATTRIBUTE18,	GLOBAL_ATTRIBUTE19,	GLOBAL_ATTRIBUTE20,	LINK_TO_TRX_HIST_ID,	EVENT_ID,	UPGRADE_METHOD,	AX_ACCOUNTED_FLAG,	INTEREST_HEADER_ID,	INTEREST_LINE_ID, LoadBy, LoadDate, LoadProcess,CreateBy, CreateDate, CreateProcess,UpdateBy,UpdateDate,UpdateProcess,InactiveInd,InactiveReason)
VALUES (76,2578,ADJUSTMENT_ID,	LAST_UPDATED_BY,	LAST_UPDATE_DATE,	LAST_UPDATE_LOGIN,	CREATED_BY,	CREATION_DATE,	AMOUNT,	APPLY_DATE,	GL_DATE,	SET_OF_BOOKS_ID,	CODE_COMBINATION_ID,	TYPE,	ADJUSTMENT_TYPE,	STATUS,	LINE_ADJUSTED,	FREIGHT_ADJUSTED,	TAX_ADJUSTED,	RECEIVABLES_CHARGES_ADJUSTED,	ASSOCIATED_CASH_RECEIPT_ID,	CHARGEBACK_CUSTOMER_TRX_ID,	BATCH_ID,	CUSTOMER_TRX_ID,	CUSTOMER_TRX_LINE_ID,	SUBSEQUENT_TRX_ID,	PAYMENT_SCHEDULE_ID,	RECEIVABLES_TRX_ID,	DISTRIBUTION_SET_ID,	GL_POSTED_DATE,	COMMENTS,	AUTOMATICALLY_GENERATED,	CREATED_FROM,	REASON_CODE,	POSTABLE,	APPROVED_BY,	ATTRIBUTE_CATEGORY,	ATTRIBUTE1,	ATTRIBUTE2,	ATTRIBUTE3,	ATTRIBUTE4,	ATTRIBUTE5,	ATTRIBUTE6,	ATTRIBUTE7,	ATTRIBUTE8,	ATTRIBUTE9,	ATTRIBUTE10,	POSTING_CONTROL_ID,	ACCTD_AMOUNT,	ATTRIBUTE11,	ATTRIBUTE12,	ATTRIBUTE13,	ATTRIBUTE14,	ATTRIBUTE15,	PROGRAM_APPLICATION_ID,	PROGRAM_ID,	PROGRAM_UPDATE_DATE,	REQUEST_ID,	ADJUSTMENT_NUMBER,	ORG_ID,	USSGL_TRANSACTION_CODE,	USSGL_TRANSACTION_CODE_CONTEXT,	DOC_SEQUENCE_VALUE,	DOC_SEQUENCE_ID,	ASSOCIATED_APPLICATION_ID,	CONS_INV_ID,	MRC_GL_POSTED_DATE,	MRC_POSTING_CONTROL_ID,	MRC_ACCTD_AMOUNT,	ADJ_TAX_ACCT_RULE,	GLOBAL_ATTRIBUTE_CATEGORY,	GLOBAL_ATTRIBUTE1,	GLOBAL_ATTRIBUTE2,	GLOBAL_ATTRIBUTE3,	GLOBAL_ATTRIBUTE4,	GLOBAL_ATTRIBUTE5,	GLOBAL_ATTRIBUTE6,	GLOBAL_ATTRIBUTE7,	GLOBAL_ATTRIBUTE8,	GLOBAL_ATTRIBUTE9,	GLOBAL_ATTRIBUTE10,	GLOBAL_ATTRIBUTE11,	GLOBAL_ATTRIBUTE12,	GLOBAL_ATTRIBUTE13,	GLOBAL_ATTRIBUTE14,	GLOBAL_ATTRIBUTE15,	GLOBAL_ATTRIBUTE16,	GLOBAL_ATTRIBUTE17,	GLOBAL_ATTRIBUTE18,	GLOBAL_ATTRIBUTE19,	GLOBAL_ATTRIBUTE20,	LINK_TO_TRX_HIST_ID,	EVENT_ID,	UPGRADE_METHOD,	AX_ACCOUNTED_FLAG,	INTEREST_HEADER_ID,	INTEREST_LINE_ID, 'airflow-worker',CURRENT_TIMESTAMP,'Dag - Corp_OracleEbs_BigQuery_AR_ADJUSTMENTS_ALL_Import','airflow-worker',CURRENT_TIMESTAMP,'Dag - Corp_OracleEbs_BigQuery_AR_ADJUSTMENTS_ALL_Import','airflow-worker',CURRENT_TIMESTAMP,'Dag - Corp_OracleEbs_BigQuery_AR_ADJUSTMENTS_ALL_Import',False,'')