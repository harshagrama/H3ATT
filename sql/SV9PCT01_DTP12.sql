select * from SV_PROD.H3ATT_DWH_SERVICE_HISTORY_V where service_name = '012034532'

select 
* --CREDIT_COMMENTS,TAX_CLASS_CODE,BANK_ACCOUNT_NUMBER,SALES_CHANNEL_CODE,GENERAL_1,GENERAL_2,GENERAL_4,GENERAL_5
from customer_node_history where customer_node_id=34340652

select st.*
from schedule s
join schedule_type st on s.schedule_type_id = st.schedule_type_id and s. schedule_id=26122416
where 1=1


select 
PRODUCT_INSTANCE_STATUS_CODE,
EFFECTIVE_START_DATE, EFFECTIVE_END_DATE, LAST_MODIFIED,
GENERAL_1,
------GENERAL_2,
GENERAL_3,
------GENERAL_4,
GENERAL_5,GENERAL_6,
------GENERAL_7,
GENERAL_8,
------GENERAL_9,
GENERAL_10
from h3att_dwh_prod_inst_hist where general_1='61098411'

select * from h3att_dwh_service_history_v where service_type_id <> 4100005

select 
SERVICE_NAME,GENERAL_1,GENERAL_4,GENERAL_6,INDUSTRY_GENERAL_1,INDUSTRY_GENERAL_3 
from H3ATT_DWH_SERVICE_HISTORY_V
where service_name='10154025'

select * from service_history where base_product_instance_id=148423718

from 