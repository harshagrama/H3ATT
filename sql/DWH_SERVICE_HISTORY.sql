SELECT  
  SH.SERVICE_ID,              
  SH.LAST_MODIFIED,           
  SH.EFFECTIVE_START_DATE,    
  SH.EFFECTIVE_END_DATE,      
  SH.ATLANTA_OPERATOR_ID,     
  SH.SERVICE_NAME,            
  SH.NETWORK_NAME,            
  SH.SERVICE_TYPE_ID,         
  SH.SERVICE_SUBTYPE,         
  SH.CUSTOMER_NODE_ID,        
  SH.SERVICE_STATUS_CODE,     
  SH.BASE_PRODUCT_INSTANCE_ID,
  SH.PERSON_ID,               
  SH.CONTRACT_ID,             
  SH.CONTRACT_REFERENCE,      
  SH.A_ADDRESS_ID,            
  SH.B_ADDRESS_ID,            
  SH.REQUIRED_BY_DATE,        
  SH.PROVISIONED_DATE,        
  SH.ACTIVE_DATE,             
  SH.BILLING_PRIORITY,        
  SH.BILLING_COMPLEXITY,      
  SH.GRADE_OF_SERVICE,        
  SH.SERVICE_USAGE,           
  SH.COMMS_TYPE,              
  SH.GL_CODE_ID,              
  (SELECT 'dTT_RAT_RP_' || ABBREVIATION 
     FROM REFERENCE_CODE
     WHERE REFERENCE_TYPE_ID = 31001285
       AND REFERENCE_CODE= SH.INDUSTRY_GENERAL_8) as INDUSTRY_GENERAL_1,      
  DECODE (SH.INDUSTRY_GENERAL_8,NULL,'2','1') as INDUSTRY_GENERAL_2,      
  SH.INDUSTRY_GENERAL_9 as INDUSTRY_GENERAL_3,      
  SH.INDUSTRY_GENERAL_4,      
  SH.INDUSTRY_GENERAL_5,      
  SH.INDUSTRY_GENERAL_6,      
  SH.INDUSTRY_GENERAL_7,      
  SH.INDUSTRY_GENERAL_8,      
  SH.INDUSTRY_GENERAL_9,      
  SH.INDUSTRY_GENERAL_10,     
  SUBSTR(SH.NETWORK_NAME,4) as GENERAL_1,               
  DECODE (SH.GENERAL_2,NULL,'0',SH.GENERAL_2) as GENERAL_2,              
  DECODE (SH.GENERAL_3,NULL,'0',SH.GENERAL_3) as GENERAL_3,                
  SDA.RESULT1_VALUE as GENERAL_4,               
  PIH.GENERAL_9 as GENERAL_5,               
  PIH.INDUSTRY_GENERAL_10 as GENERAL_6,               
  SH.GENERAL_7,               
  SH.GENERAL_8,               
  SH.GENERAL_9,               
  SH.GENERAL_10
FROM SERVICE_HISTORY SH
JOIN ACCOUNT ACC ON SH.CUSTOMER_NODE_ID = ACC.CUSTOMER_NODE_ID AND ACC.GENERAL_2 = 'CLARIFY'
JOIN SERVICE_DA_ARRAY SDA ON SH.SERVICE_ID = SDA.SERVICE_ID AND SDA.DERIVED_ATTRIBUTE_ID = 31001194 
      AND SH.EFFECTIVE_START_DATE BETWEEN SDA.EFFECTIVE_START_DATE AND SDA.EFFECTIVE_END_DATE   
JOIN PRODUCT_INSTANCE_HISTORY PIH ON SH.BASE_PRODUCT_INSTANCE_ID = PIH.PRODUCT_INSTANCE_ID 
      AND SH.EFFECTIVE_START_DATE BETWEEN PIH.EFFECTIVE_START_DATE AND PIH.EFFECTIVE_END_DATE
WHERE 1=1
AND sh.service_name in ('0724243985', '0724290500','05223504','07242252876')
--AND service_name='014026861'




select * from OPS$SV6DEV01.PRODUCT_INSTANCE_HISTORY
where product_instance_id=26882556 --PBP10459269


SELECT *
     FROM REFERENCE_CODE
     WHERE REFERENCE_TYPE_ID = 31001285


select * from account where customer_node_id=34336789

select * from OPS$SV6DEV01.derived_attribute_history where derived_attribute_name like '%Service%'


select * 
-- result1_value as FINANCE_CODE 
from OPS$SV6DEV01.service_da_array where derived_attribute_id=31001194 and service_id=26173196
--31001194


select count(*) from OPS$SV6DEV01.H3ATT_DWH_SERVICE_HISTORY_V --994
select count(*) from OPS$SV6DEV01.H3ATT_DWH_SERVICE_HISTORY --994

--------------------------------------------------------------------------------
----------------Verification Steps----------------------------------------------
--------------------------------------------------------------------------------
select * from H3ATT_DWH_SERVICE_HISTORY_V where service_name='014026861'-- --general_1='10210376'
select * from service_history where service_name='014026861' --general_1='10210376'

/*
Findings:
* GENERAL_5 Does not Match, Product_instance_history - General_9
* GENERAL_& Does not Match, EDI ID is not populated after migration
*/

