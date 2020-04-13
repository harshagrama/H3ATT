SELECT DECODE(rp.product_name,'* Any *','GLOBAL',SUBSTR (rp.product_name,LENGTH('H3ATT_RP_')+1)) AS PREISMODELL,
  CASE
    WHEN daa.index3_value > 10000
    THEN
      (SELECT result1_value
      FROM derived_attribute_array
      WHERE derived_attribute_id=31001074
      AND index1_value          ='REFERENCE_TYPE'
      AND index2_value          ='EVENT_TYPE_CODE'
      AND index3_value          = daa.index3_value
      )
    ELSE daa.index3_value
  END AS EVENT_TYPE_ID,
  CASE
    WHEN daa.index4_value > 200000
    THEN
      (SELECT result1_value
      FROM derived_attribute_array
      WHERE derived_attribute_id=31001074
      AND index1_value          ='REFERENCE_TYPE'
      AND index2_value          ='EVENT_SUB_TYPE_CODE'
      AND index3_value          = daa.index4_value
      )
    ELSE daa.index4_value
  END                                            AS EVENT_SUB_TYPE_ID,
  daa.index5_value                               AS CHARGEBAND_ID,
  DECODE (daa.index6_value,2,0,daa.index6_value) AS IS_PEAK,
  REPLACE (daa.result1_value,'.',',')            AS SURCHARGE,
  REPLACE (daa.result4_value,'.',',')            AS CHARGE,
  daa.effective_start_date,
  daa.effective_end_date
FROM derived_attribute_array daa
JOIN
  (SELECT product_id,
    product_name,
    SUBSTR(description, 1, 80)
  FROM product_history
  WHERE companion_ind_code = 1
  AND sysdate BETWEEN effective_start_date AND effective_end_date
  AND product_name LIKE 'H3ATT%RP%'
  UNION
  SELECT reference_code,
    abbreviation,
    SUBSTR(description, 1, 80)
  FROM reference_code
  WHERE reference_type_id =
    (SELECT reference_type_id
    FROM reference_type
    WHERE type_label = 'H3G_CMN_WC_NA_EVENT_TYPES'
    )
  AND reference_code IN (-1, 0)
  ORDER BY 2
  ) rp
ON daa.index2_value     = rp.product_id
WHERE 1                 =1
AND derived_attribute_id=31000954