MERGE Ods.`OracleEbs-RA_BATCH_SOURCES_ALL` AS T using OdsStage.`OracleEbs-RA_BATCH_SOURCES_ALL` AS S on T.BATCH_SOURCE_ID = S.BATCH_SOURCE_ID and ifnull(T.ORG_ID,0)=ifnull(S.ORG_ID,0)
WHEN MATCHED THEN
Update SET
T.BATCH_SOURCE_ID=S.BATCH_SOURCE_ID
,T.CREATION_DATE=S.CREATION_DATE
,T.CREATED_BY=S.CREATED_BY
,T.LAST_UPDATE_DATE=S.LAST_UPDATE_DATE
,T.LAST_UPDATED_BY=S.LAST_UPDATED_BY
,T.LAST_UPDATE_LOGIN=S.LAST_UPDATE_LOGIN
,T.NAME=S.NAME
,T.ORG_ID=S.ORG_ID
,T.DESCRIPTION=S.DESCRIPTION
,T.STATUS=S.STATUS
,T.LAST_BATCH_NUM=S.LAST_BATCH_NUM
,T.DEFAULT_INV_TRX_TYPE=S.DEFAULT_INV_TRX_TYPE
,T.ATTRIBUTE_CATEGORY=S.ATTRIBUTE_CATEGORY
,T.ATTRIBUTE1=S.ATTRIBUTE1
,T.ATTRIBUTE2=S.ATTRIBUTE2
,T.ATTRIBUTE3=S.ATTRIBUTE3
,T.ATTRIBUTE4=S.ATTRIBUTE4
,T.ATTRIBUTE5=S.ATTRIBUTE5
,T.ATTRIBUTE6=S.ATTRIBUTE6
,T.ATTRIBUTE7=S.ATTRIBUTE7
,T.ATTRIBUTE8=S.ATTRIBUTE8
,T.ATTRIBUTE9=S.ATTRIBUTE9
,T.ATTRIBUTE10=S.ATTRIBUTE10
,T.ACCOUNTING_FLEXFIELD_RULE=S.ACCOUNTING_FLEXFIELD_RULE
,T.ACCOUNTING_RULE_RULE=S.ACCOUNTING_RULE_RULE
,T.AGREEMENT_RULE=S.AGREEMENT_RULE
,T.AUTO_BATCH_NUMBERING_FLAG=S.AUTO_BATCH_NUMBERING_FLAG
,T.AUTO_TRX_NUMBERING_FLAG=S.AUTO_TRX_NUMBERING_FLAG
,T.BATCH_SOURCE_TYPE=S.BATCH_SOURCE_TYPE
,T.BILL_ADDRESS_RULE=S.BILL_ADDRESS_RULE
,T.BILL_CONTACT_RULE=S.BILL_CONTACT_RULE
,T.BILL_CUSTOMER_RULE=S.BILL_CUSTOMER_RULE
,T.CREATE_CLEARING_FLAG=S.CREATE_CLEARING_FLAG
,T.CUST_TRX_TYPE_RULE=S.CUST_TRX_TYPE_RULE
,T.DERIVE_DATE_FLAG=S.DERIVE_DATE_FLAG
,T.END_DATE=S.END_DATE
,T.FOB_POINT_RULE=S.FOB_POINT_RULE
,T.GL_DATE_PERIOD_RULE=S.GL_DATE_PERIOD_RULE
,T.INVALID_LINES_RULE=S.INVALID_LINES_RULE
,T.INVALID_TAX_RATE_RULE=S.INVALID_TAX_RATE_RULE
,T.INVENTORY_ITEM_RULE=S.INVENTORY_ITEM_RULE
,T.INVOICING_RULE_RULE=S.INVOICING_RULE_RULE
,T.MEMO_REASON_RULE=S.MEMO_REASON_RULE
,T.REV_ACC_ALLOCATION_RULE=S.REV_ACC_ALLOCATION_RULE
,T.SALESPERSON_RULE=S.SALESPERSON_RULE
,T.SALES_CREDIT_RULE=S.SALES_CREDIT_RULE
,T.SALES_CREDIT_TYPE_RULE=S.SALES_CREDIT_TYPE_RULE
,T.SALES_TERRITORY_RULE=S.SALES_TERRITORY_RULE
,T.SHIP_ADDRESS_RULE=S.SHIP_ADDRESS_RULE
,T.SHIP_CONTACT_RULE=S.SHIP_CONTACT_RULE
,T.SHIP_CUSTOMER_RULE=S.SHIP_CUSTOMER_RULE
,T.SHIP_VIA_RULE=S.SHIP_VIA_RULE
,T.SOLD_CUSTOMER_RULE=S.SOLD_CUSTOMER_RULE
,T.START_DATE=S.START_DATE
,T.TERM_RULE=S.TERM_RULE
,T.UNIT_OF_MEASURE_RULE=S.UNIT_OF_MEASURE_RULE
,T.ATTRIBUTE11=S.ATTRIBUTE11
,T.ATTRIBUTE12=S.ATTRIBUTE12
,T.ATTRIBUTE13=S.ATTRIBUTE13
,T.ATTRIBUTE14=S.ATTRIBUTE14
,T.ATTRIBUTE15=S.ATTRIBUTE15
,T.CUSTOMER_BANK_ACCOUNT_RULE=S.CUSTOMER_BANK_ACCOUNT_RULE
,T.MEMO_LINE_RULE=S.MEMO_LINE_RULE
,T.RECEIPT_METHOD_RULE=S.RECEIPT_METHOD_RULE
,T.RELATED_DOCUMENT_RULE=S.RELATED_DOCUMENT_RULE
,T.ALLOW_SALES_CREDIT_FLAG=S.ALLOW_SALES_CREDIT_FLAG
,T.GROUPING_RULE_ID=S.GROUPING_RULE_ID
,T.CREDIT_MEMO_BATCH_SOURCE_ID=S.CREDIT_MEMO_BATCH_SOURCE_ID
,T.GLOBAL_ATTRIBUTE1=S.GLOBAL_ATTRIBUTE1
,T.GLOBAL_ATTRIBUTE2=S.GLOBAL_ATTRIBUTE2
,T.GLOBAL_ATTRIBUTE3=S.GLOBAL_ATTRIBUTE3
,T.GLOBAL_ATTRIBUTE4=S.GLOBAL_ATTRIBUTE4
,T.GLOBAL_ATTRIBUTE5=S.GLOBAL_ATTRIBUTE5
,T.GLOBAL_ATTRIBUTE6=S.GLOBAL_ATTRIBUTE6
,T.GLOBAL_ATTRIBUTE7=S.GLOBAL_ATTRIBUTE7
,T.GLOBAL_ATTRIBUTE8=S.GLOBAL_ATTRIBUTE8
,T.GLOBAL_ATTRIBUTE9=S.GLOBAL_ATTRIBUTE9
,T.GLOBAL_ATTRIBUTE10=S.GLOBAL_ATTRIBUTE10
,T.GLOBAL_ATTRIBUTE11=S.GLOBAL_ATTRIBUTE11
,T.GLOBAL_ATTRIBUTE12=S.GLOBAL_ATTRIBUTE12
,T.GLOBAL_ATTRIBUTE13=S.GLOBAL_ATTRIBUTE13
,T.GLOBAL_ATTRIBUTE14=S.GLOBAL_ATTRIBUTE14
,T.GLOBAL_ATTRIBUTE15=S.GLOBAL_ATTRIBUTE15
,T.GLOBAL_ATTRIBUTE16=S.GLOBAL_ATTRIBUTE16
,T.GLOBAL_ATTRIBUTE17=S.GLOBAL_ATTRIBUTE17
,T.GLOBAL_ATTRIBUTE18=S.GLOBAL_ATTRIBUTE18
,T.GLOBAL_ATTRIBUTE19=S.GLOBAL_ATTRIBUTE19
,T.GLOBAL_ATTRIBUTE20=S.GLOBAL_ATTRIBUTE20
,T.GLOBAL_ATTRIBUTE_CATEGORY=S.GLOBAL_ATTRIBUTE_CATEGORY
,T.COPY_DOC_NUMBER_FLAG=S.COPY_DOC_NUMBER_FLAG
,T.DEFAULT_REFERENCE=S.DEFAULT_REFERENCE
,T.COPY_INV_TIDFF_TO_CM_FLAG=S.COPY_INV_TIDFF_TO_CM_FLAG
,T.RECEIPT_HANDLING_OPTION=S.RECEIPT_HANDLING_OPTION
,T.ALLOW_DUPLICATE_TRX_NUM_FLAG=S.ALLOW_DUPLICATE_TRX_NUM_FLAG
,T.LEGAL_ENTITY_ID=S.LEGAL_ENTITY_ID
,T.GEN_LINE_LEVEL_BAL_FLAG=S.GEN_LINE_LEVEL_BAL_FLAG
,T.PAYMENT_DET_DEF_HIERARCHY=S.PAYMENT_DET_DEF_HIERARCHY
,T.LoadBy = 'airflow-worker'
,T.LoadDate = CURRENT_TIMESTAMP
,T.LoadProcess = 'Dag - Corp_OracleEbs_BigQuery_RA_BATCH_SOURCES_ALL_Import'
,T.UpdateBy = 'airflow-worker'
,T.UpdateDate = CURRENT_TIMESTAMP
,T.UpdateProcess = 'Dag - Corp_OracleEbs_BigQuery_RA_BATCH_SOURCES_ALL_Import'
,T.InactiveInd = False
,T.InactiveDate = CURRENT_TIMESTAMP
,T.InactiveReason = ''

	WHEN NOT MATCHED THEN
INSERT (ParentSystemId,SystemId,BATCH_SOURCE_ID, LAST_UPDATE_DATE,LAST_UPDATED_BY,CREATION_DATE,CREATED_BY,    LAST_UPDATE_LOGIN,     NAME,  ORG_ID,    DESCRIPTION, STATUS,   LAST_BATCH_NUM,  DEFAULT_INV_TRX_TYPE, ATTRIBUTE_CATEGORY,  ATTRIBUTE1, ATTRIBUTE2, ATTRIBUTE3, ATTRIBUTE4, ATTRIBUTE5, ATTRIBUTE6, ATTRIBUTE7, ATTRIBUTE8, ATTRIBUTE9, ATTRIBUTE10, ACCOUNTING_FLEXFIELD_RULE, ACCOUNTING_RULE_RULE,  AGREEMENT_RULE,  AUTO_BATCH_NUMBERING_FLAG,   AUTO_TRX_NUMBERING_FLAG,   BATCH_SOURCE_TYPE,  BILL_ADDRESS_RULE,  BILL_CONTACT_RULE,  BILL_CUSTOMER_RULE, CREATE_CLEARING_FLAG,   CUST_TRX_TYPE_RULE,  DERIVE_DATE_FLAG,   END_DATE,          FOB_POINT_RULE,  GL_DATE_PERIOD_RULE,  INVALID_LINES_RULE,  INVALID_TAX_RATE_RULE,  INVENTORY_ITEM_RULE,INVOICING_RULE_RULE,  MEMO_REASON_RULE,  REV_ACC_ALLOCATION_RULE,  SALESPERSON_RULE,  SALES_CREDIT_RULE,  SALES_CREDIT_TYPE_RULE,  SALES_TERRITORY_RULE,SHIP_ADDRESS_RULE,  SHIP_CONTACT_RULE         ,  SHIP_CUSTOMER_RULE,SHIP_VIA_RULE,SOLD_CUSTOMER_RULE,START_DATE,TERM_RULE,UNIT_OF_MEASURE_RULE,  ATTRIBUTE11, ATTRIBUTE12    , ATTRIBUTE13, ATTRIBUTE14, ATTRIBUTE15, CUSTOMER_BANK_ACCOUNT_RULE,  MEMO_LINE_RULE  ,  RECEIPT_METHOD_RULE,  RELATED_DOCUMENT_RULE,  ALLOW_SALES_CREDIT_FLAG,   GROUPING_RULE_ID,    CREDIT_MEMO_BATCH_SOURCE_ID,GLOBAL_ATTRIBUTE1, GLOBAL_ATTRIBUTE2, GLOBAL_ATTRIBUTE3, GLOBAL_ATTRIBUTE4, GLOBAL_ATTRIBUTE5, GLOBAL_ATTRIBUTE6, GLOBAL_ATTRIBUTE7, GLOBAL_ATTRIBUTE8,GLOBAL_ATTRIBUTE9, GLOBAL_ATTRIBUTE10, GLOBAL_ATTRIBUTE11, GLOBAL_ATTRIBUTE12, GLOBAL_ATTRIBUTE13, GLOBAL_ATTRIBUTE14, GLOBAL_ATTRIBUTE15, GLOBAL_ATTRIBUTE16,GLOBAL_ATTRIBUTE17, GLOBAL_ATTRIBUTE18, GLOBAL_ATTRIBUTE19, GLOBAL_ATTRIBUTE20, GLOBAL_ATTRIBUTE_CATEGORY,  COPY_DOC_NUMBER_FLAG,   DEFAULT_REFERENCE, COPY_INV_TIDFF_TO_CM_FLAG,   RECEIPT_HANDLING_OPTION,  ALLOW_DUPLICATE_TRX_NUM_FLAG,   LEGAL_ENTITY_ID,    GEN_LINE_LEVEL_BAL_FLAG,   PAYMENT_DET_DEF_HIERARCHY,LoadBy, LoadDate, LoadProcess,CreateBy, CreateDate,CreateProcess,UpdateBy,UpdateDate,UpdateProcess,InactiveInd,InactiveDate,InactiveReason)
VALUES(76,2578,BATCH_SOURCE_ID, LAST_UPDATE_DATE,LAST_UPDATED_BY,CREATION_DATE,CREATED_BY,    LAST_UPDATE_LOGIN,     NAME,  ORG_ID,    DESCRIPTION, STATUS,   LAST_BATCH_NUM,  DEFAULT_INV_TRX_TYPE, ATTRIBUTE_CATEGORY,  ATTRIBUTE1, ATTRIBUTE2, ATTRIBUTE3, ATTRIBUTE4, ATTRIBUTE5, ATTRIBUTE6, ATTRIBUTE7, ATTRIBUTE8, ATTRIBUTE9, ATTRIBUTE10, ACCOUNTING_FLEXFIELD_RULE, ACCOUNTING_RULE_RULE,  AGREEMENT_RULE,  AUTO_BATCH_NUMBERING_FLAG,   AUTO_TRX_NUMBERING_FLAG,   BATCH_SOURCE_TYPE,  BILL_ADDRESS_RULE,  BILL_CONTACT_RULE,  BILL_CUSTOMER_RULE, CREATE_CLEARING_FLAG,   CUST_TRX_TYPE_RULE,  DERIVE_DATE_FLAG,   END_DATE,          FOB_POINT_RULE,  GL_DATE_PERIOD_RULE,  INVALID_LINES_RULE,  INVALID_TAX_RATE_RULE,  INVENTORY_ITEM_RULE,INVOICING_RULE_RULE,  MEMO_REASON_RULE,  REV_ACC_ALLOCATION_RULE,  SALESPERSON_RULE,  SALES_CREDIT_RULE,  SALES_CREDIT_TYPE_RULE,  SALES_TERRITORY_RULE,SHIP_ADDRESS_RULE,  SHIP_CONTACT_RULE         ,  SHIP_CUSTOMER_RULE,SHIP_VIA_RULE,SOLD_CUSTOMER_RULE,START_DATE,TERM_RULE,UNIT_OF_MEASURE_RULE,  ATTRIBUTE11, ATTRIBUTE12    , ATTRIBUTE13, ATTRIBUTE14, ATTRIBUTE15, CUSTOMER_BANK_ACCOUNT_RULE,  MEMO_LINE_RULE  ,  RECEIPT_METHOD_RULE,  RELATED_DOCUMENT_RULE,  ALLOW_SALES_CREDIT_FLAG,   GROUPING_RULE_ID,    CREDIT_MEMO_BATCH_SOURCE_ID,GLOBAL_ATTRIBUTE1, GLOBAL_ATTRIBUTE2, GLOBAL_ATTRIBUTE3, GLOBAL_ATTRIBUTE4, GLOBAL_ATTRIBUTE5, GLOBAL_ATTRIBUTE6, GLOBAL_ATTRIBUTE7, GLOBAL_ATTRIBUTE8,GLOBAL_ATTRIBUTE9, GLOBAL_ATTRIBUTE10, GLOBAL_ATTRIBUTE11, GLOBAL_ATTRIBUTE12, GLOBAL_ATTRIBUTE13, GLOBAL_ATTRIBUTE14, GLOBAL_ATTRIBUTE15, GLOBAL_ATTRIBUTE16,GLOBAL_ATTRIBUTE17, GLOBAL_ATTRIBUTE18, GLOBAL_ATTRIBUTE19, GLOBAL_ATTRIBUTE20, GLOBAL_ATTRIBUTE_CATEGORY,  COPY_DOC_NUMBER_FLAG,   DEFAULT_REFERENCE, COPY_INV_TIDFF_TO_CM_FLAG,   RECEIPT_HANDLING_OPTION,  ALLOW_DUPLICATE_TRX_NUM_FLAG,   LEGAL_ENTITY_ID,    GEN_LINE_LEVEL_BAL_FLAG,   PAYMENT_DET_DEF_HIERARCHY,'airflow-worker',CURRENT_TIMESTAMP,'Dag - Corp_OracleEbs_BigQuery_RA_BATCH_SOURCES_ALL_Import','airflow-worker',CURRENT_TIMESTAMP,'Dag - Corp_OracleEbs_BigQuery_RA_BATCH_SOURCES_ALL_Import','airflow-worker',CURRENT_TIMESTAMP,'Dag - Corp_OracleEbs_BigQuery_RA_BATCH_SOURCES_ALL_Import',False,CURRENT_TIMESTAMP,'')