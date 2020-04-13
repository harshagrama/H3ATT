select * from sv_prod.h3att_dwh_ebill_v

select * from h3att_dwh_discount_overview_v

------------------------- MAIN -------------------------------------

SELECT ac.account_name
       ,itd.index1_value as ITD_DISCOUNT_PRD
       ,itd.result2_value as ITD_DISCOUNT_PCT
       ,itd.effective_start_date as ITD_START
       ,itd.effective_end_date as ITD_END
       ,pcd.PCD_PRODUCT_DESCR
       ,pcd.index3_value as PCD_PRODUCT_CLASS
       ,pcd.result2_value as PCD_DISCOUNT_PCT
       ,pcd.effective_start_date as PCD_START
       ,pcd.effective_end_date as PCD_END
       ,DECODE (cud.result2_value,1,'Usage_Discount',NULL) as CUD_DISCOUNT_DESCR
       ,cud.index1_value as CUD_LOWER_LIMIT
       ,DECODE (cud.result2_value,1,'Stepped',NULL) as CUD_TYPE
       ,cud.result1_value as CUD_DISCOUNT_PCT
       ,cud.effective_start_date as CUD_START
       ,cud.effective_end_date as CUD_END
  FROM service_history sh
  JOIN account ac ON sh.customer_node_id=ac.customer_node_id and ac.general_2='CLARIFY' and sh.service_name=ac.account_name
  LEFT JOIN service_da_array itd ON itd.service_id=sh.service_id and itd.derived_attribute_id=31001434 and itd.index1_value=31001181
  LEFT OUTER JOIN (SELECT *
              FROM ( 
              SELECT ac.account_name
                     ,ac.customer_node_id
                     ,DECODE(pd.index1_value,31001221,'Service_Discount',pd.index1_value) as PCD_PRODUCT_DESCR
                     ,pd.result2_value 
                     ,pd.effective_start_date 
                     ,pd.effective_end_date 
                FROM service_history sh
                JOIN account ac ON sh.customer_node_id=ac.customer_node_id and ac.general_2='CLARIFY' and sh.service_name=ac.account_name
                LEFT JOIN service_da_array pd ON pd.service_id=sh.service_id and pd.derived_attribute_id=31001434 and pd.index1_value=31001221
               WHERE 1=1
                AND pd.result2_value IS NOT NULL) pd, (SELECT index3_value FROM derived_attribute_array where derived_attribute_id=31001614 and index1_value=148) daa) pcd
                ON sh.customer_node_id=pcd.customer_node_id and sysdate between pcd.effective_start_date and pcd.effective_end_date
  LEFT OUTER JOIN   (SELECT cd.account_name
                             ,cd.customer_node_id
                             ,daa.index1_value
                             ,daa.index2_value
                             ,daa.result1_value
                             ,daa.result2_value
                             ,daa.effective_start_date
                             ,daa.effective_end_date
                        FROM (SELECT ac.account_name
                                    ,ac.customer_node_id
                                 FROM service_history sh
                                 JOIN account ac ON sh.customer_node_id=ac.customer_node_id and ac.general_2='CLARIFY' and sh.service_name=ac.account_name
                                 JOIN product_instance_history pih ON sh.base_product_instance_id=pih.base_product_instance_id and product_id=31001222
                               WHERE 1=1) cd, (SELECT index1_value, index2_value,result1_value,result2_value,effective_start_date, effective_end_date
                                                 from derived_attribute_array where derived_attribute_id=31001534 and result1_value > 0) daa  
                      ) cud ON sh.customer_node_id=cud.customer_node_id and sysdate between cud.effective_start_date and cud.effective_end_date                    
 WHERE 1=1
  AND (itd.result2_value IS NOT NULL OR pcd.result2_value IS NOT NULL OR cud.result1_value IS NOT NULL)
ORDER BY ac.account_name


---------------------PCD-------------------------------------------
SELECT *
              FROM ( 
              SELECT ac.account_name
                     ,ac.customer_node_id
                     ,DECODE(pd.index1_value,31001221,'Service_Discount',pd.index1_value) 
                     ,pd.result2_value 
                     ,pd.effective_start_date 
                     ,pd.effective_end_date 
                FROM service_history sh
                JOIN account ac ON sh.customer_node_id=ac.customer_node_id and ac.general_2='CLARIFY' and sh.service_name=ac.account_name
                LEFT JOIN service_da_array pd ON pd.service_id=sh.service_id and pd.derived_attribute_id=31001434 and pd.index1_value=31001221
               WHERE 1=1
                AND pd.result2_value IS NOT NULL) pd, (SELECT index3_value FROM derived_attribute_array where derived_attribute_id=31001614 and index1_value=148) daa

---------------------CUD-------------------------------------------
SELECT cd.account_name
       ,cd.customer_node_id
       ,daa.index1_value
       ,daa.index2_value
       ,daa.result1_value
       ,daa.result2_value
  FROM (SELECT ac.account_name
              ,ac.customer_node_id
           FROM service_history sh
           JOIN account ac ON sh.customer_node_id=ac.customer_node_id and ac.general_2='CLARIFY' and sh.service_name=ac.account_name
           JOIN product_instance_history pih ON sh.base_product_instance_id=pih.base_product_instance_id and product_id=31001222
         WHERE 1=1) cd, (SELECT index1_value, index2_value,result1_value,result2_value from derived_attribute_array where derived_attribute_id=31001534 and result1_value > 0) daa  


------------------------------------------------------------------
select * from derived_attribute_array where derived_attribute_id=31001614