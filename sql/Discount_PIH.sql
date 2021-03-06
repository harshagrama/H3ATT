select * from OPS$SV6DEV01.H3ATT_DWH_PROD_INST_HIST where customer_node_id=260532

SELECT 
        PIH.GENERAL_5 as DISCOUNT_AMOUNT,
        PIH.GENERAL_6 as DISCOUNT_PERCENTAGE,
        PIH.GENERAL_9 as DISCOUNT_INVOICE_TEXT,
        PIH.BASE_PRODUCT_INSTANCE_ID,
        PIH.CUSTOMER_NODE_ID,
        PIH.PRODUCT_ID,
        PIH.PRODUCT_INSTANCE_ID,
        PIH.PRODUCT_INSTANCE_STATUS_CODE,
        PIH.LAST_MODIFIED,
        PIH.INDUSTRY_GENERAL_10

    FROM  PRODUCT_INSTANCE_HISTORY PIH
    JOIN ACCOUNT ACC ON PIH.CUSTOMER_NODE_ID = ACC.CUSTOMER_NODE_ID AND ACC.GENERAL_2 = 'CLARIFY'
    JOIN PRODUCT_HISTORY PH ON PIH.PRODUCT_ID=PH.PRODUCT_ID AND PH.PRODUCT_DISPLAY_NAME like '%Discount%' 
    WHERE 1=1
    --AND PIH.LAST_MODIFIED >= TO_DATE('01-10-2019 00:00:00','DD-MM-YYYY HH24:MI:SS')
    
    
     
    