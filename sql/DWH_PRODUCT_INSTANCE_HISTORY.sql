select COUNT(*) from (
SELECT 
    PIH.PRODUCT_INSTANCE_ID,          
    PIH.LAST_MODIFIED,          
    PIH.ATLANTA_OPERATOR_ID,         
    PIH.EFFECTIVE_START_DATE,        
    PIH.EFFECTIVE_END_DATE,       
    PIH.PRODUCT_ID,                   
    PIH.PRODUCT_INSTANCE_STATUS_CODE, 
    PIH.CUSTOMER_NODE_ID,             
    PIH.BASE_PRODUCT_INSTANCE_ID,     
    PIH.CONTRACT_ID,                  
    PIH.FROM_PRODUCT_INSTANCE_ID,     
    PIH.FROM_PRODUCT_ID,              
    PIH.TO_PRODUCT_INSTANCE_ID,       
    PIH.TO_PRODUCT_ID,                
    PIH.UNCOMPLETED_IND_CODE,         
    PIH.GENERAL_1,                    
    PIH.GENERAL_2,                    
    PIH.GENERAL_3,                    
    PIH.GENERAL_4,                    
    PIH.GENERAL_5,                    
    PIH.GENERAL_6,                    
    PIH.GENERAL_7,                    
    PIH.GENERAL_8,                    
    PIH.GENERAL_9,                    
    PIH.GENERAL_10,                   
    PIH.ACTIVE_DATE,                  
  --PIH.PRODUCT_BUNDLE_ID ,           
    PIH.INDUSTRY_GENERAL_1,          
    PIH.INDUSTRY_GENERAL_2,           
    PIH.INDUSTRY_GENERAL_3,          
    PIH.INDUSTRY_GENERAL_4,           
    PIH.INDUSTRY_GENERAL_5,           
    PIH.INDUSTRY_GENERAL_6,           
    PIH.INDUSTRY_GENERAL_7,           
    PIH.INDUSTRY_GENERAL_8,           
    PIH.INDUSTRY_GENERAL_9,           
    PIH.INDUSTRY_GENERAL_10 
FROM PRODUCT_INSTANCE_HISTORY PIH
JOIN ACCOUNT ACC ON PIH.CUSTOMER_NODE_ID = ACC.CUSTOMER_NODE_ID AND ACC.GENERAL_2 = 'CLARIFY'
WHERE 1=1
AND PIH.LAST_MODIFIED >= TO_DATE('01-10-2016 00:00:00','DD-MM-YYYY HH24:MI:SS')
AND PIH.PRODUCT_ID IN (31000882,31000922,31000881,31000921)
)


select COUNT(*) from (
SELECT 
    PIH.PRODUCT_INSTANCE_ID,          
    PIH.LAST_MODIFIED,          
    PIH.ATLANTA_OPERATOR_ID,         
    PIH.EFFECTIVE_START_DATE,        
    PIH.EFFECTIVE_END_DATE,       
    PIH.PRODUCT_ID,                   
    PIH.PRODUCT_INSTANCE_STATUS_CODE, 
    PIH.CUSTOMER_NODE_ID,             
    PIH.BASE_PRODUCT_INSTANCE_ID,     
    PIH.CONTRACT_ID,                  
    PIH.FROM_PRODUCT_INSTANCE_ID,     
    PIH.FROM_PRODUCT_ID,              
    PIH.TO_PRODUCT_INSTANCE_ID,       
    PIH.TO_PRODUCT_ID,                
    PIH.UNCOMPLETED_IND_CODE,         
    PIH.GENERAL_1,                    
    PIH.GENERAL_2,                    
    PIH.GENERAL_3,                    
    PIH.GENERAL_4,                    
    PIH.GENERAL_5,                    
    PIH.GENERAL_6,                    
    PIH.GENERAL_7,                    
    PIH.GENERAL_8,                    
    PIH.GENERAL_9,                    
    PIH.GENERAL_10,                   
    PIH.ACTIVE_DATE,                  
  --PIH.PRODUCT_BUNDLE_ID ,           
    PIH.INDUSTRY_GENERAL_1,          
    PIH.INDUSTRY_GENERAL_2,           
    PIH.INDUSTRY_GENERAL_3,          
    PIH.INDUSTRY_GENERAL_4,           
    PIH.INDUSTRY_GENERAL_5,           
    PIH.INDUSTRY_GENERAL_6,           
    PIH.INDUSTRY_GENERAL_7,           
    PIH.INDUSTRY_GENERAL_8,           
    PIH.INDUSTRY_GENERAL_9,           
    PIH.INDUSTRY_GENERAL_10 
FROM PRODUCT_INSTANCE_HISTORY PIH
JOIN ACCOUNT ACC ON PIH.CUSTOMER_NODE_ID = ACC.CUSTOMER_NODE_ID AND ACC.GENERAL_2 = 'CLARIFY'
WHERE 1=1
AND PIH.LAST_MODIFIED >= TO_DATE('01-10-2016 00:00:00','DD-MM-YYYY HH24:MI:SS')
AND NOT EXISTS (SELECT 
                  NULL  
                FROM PRODUCT_HISTORY PH 
                WHERE PH.PRODUCT_ID=PIH.PRODUCT_ID 
                AND PH.PRODUCT_DISPLAY_NAME like '%Discount%'
                AND PH.LAST_MODIFIED BETWEEN PH.EFFECTIVE_START_DATE AND PH.EFFECTIVE_END_DATE ) 
)



select * from OPS$SV6DEV01.service_history where  base_product_instance_id=26820743

-- History record test for PIH BASE-PROD
select * from OPS$SV6DEV01.product_instance_history where  product_instance_id=26820743 --customer_node_id=26053215
select * from OPS$SV6DEV01.product_instance_history where base_product_instance_id=26820743

select * from OPS$SV6DEV01.H3ATT_DWH_PROD_INST_HIST where  product_instance_id=26820743
select * from OPS$SV6DEV01.H3ATT_DWH_PROD_INST_HIST where  base_product_instance_id=26820743


select * from OPS$SV6DEV01.product_instance_history where base_product_instance_id=26918691
select * from OPS$SV6DEV01.product_history where product_display_name like '%Discount%'
select * from product_history where product_id=31000882


select * from OPS$SV6DEV01.H3ATT_DWH_PROD_INST_HIST where  base_product_instance_id IS NOT NULL

select * from product_history where product_id in (
select unique (product_id) from H3ATT_DWH_PROD_INST_HIST where  base_product_instance_id I)

select * from product_instance_history where customer_node_id=26053215
select * from H3ATT_DWH_PROD_INST_HIST where customer_node_id=26053215

--where product_id in (31000923,31000924,31000901,31000922,31000881,31000921 )

select * from OPS$SV6DEV01.product_instance_history where product_instance_id=2691868

select count(*) from H3ATT_DWH_PROD_INST_HIST

-------------------------------------------------------------------------------
Verification
-------------------------------------------------------------------------------

select  sh.service_name
from  h3att_dwh_prod_inst_hist pih
JOIN service_history sh on pih.customer_node_id=sh.customer_node_id
where 1=1
AND pih.customer_node_id=34337911 --34342851

select * from h3att_dwh_prod_inst_hist where customer_node_id=34337911 and base_product_instance_id IS NOT NULL
and last_modified between effective_start_date and effective_end_date


select * from product_instance_history where  product_instance_id=26918680 
select * from product_instance_history where base_product_instance_id=26918680

select * from service_history where base_product_instance_id=26918680
 
select * from H3ATT_DWH_PROD_INST_HIST where  product_instance_id=26918680
select * from H3ATT_DWH_PROD_INST_HIST where  base_product_instance_id=26918680

-----DWH----------------------------------------------------
select * from SV_BUS.PRODUCT_INSTANCE_HISTORY 
where customer_node_id=508165 and base_product_instance_id IS NOT NULL
and last_modified between effective_start_date and effective_end_date



select  sh.customer_node_id
from  SV_BUS.product_instance_history pih
JOIN SV_BUS.service_history sh on pih.customer_node_id=sh.customer_node_id
where 1=1
AND sh.service_name='0722961290' 

--------------------------------------------------------------------------------
----------------Verification Steps----------------------------------------------
--------------------------------------------------------------------------------
--BASE-PRODUCT INSTACNE
select * from H3ATT_DWH_PROD_INST_HIST where general_1='10469519' and base_product_instance_id is null
select * from sv_bus.product_instance_history where  base_product_instance_id in (148206664, 148206666, 148206669, 148206671)
select * from product_instance_history where product_instance_id=148206664
/*
Findings:
* GENERAL_7 Does not Match in most of the cases (Invoice text looks slightly different)
* GENERAL_9 Does not match, looks totally different
*/

--COMPANION-PRODUCT INSTACNE
select * from H3ATT_DWH_PROD_INST_HIST where base_product_instance_id in (148206664, 148206666, 148206669, 148206671)
select * from sv_bus.product_instance_history where base_product_instance_id in (base_product_instance_id)

/*
Findings:
All looks good here
*/