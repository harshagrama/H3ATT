--------------------------------------------------------------------------------
-------------------EVENT_TYPE_CODE---------------------------------------------
-------------------------------------------------------------------------------
select 
    '31001074' as DERIVED_ATTRIBUTE_ID,
    to_date('01-01-1996 00:00:00', 'DD-MM-YYYY HH24:MI:SS') as EFFECTIVE_START_DATE, 
    to_date('30-12-9999 00:00:00', 'DD-MM-YYYY HH24:MI:SS') as EFFECTOVE_END_DATE,
    (ROWNUM + 10) as SEQNR, 
    to_date(sysdate, 'DD-MM-YYYY HH24:MI:SS') as LAST_MODIFIED,
    'REFERENCE_TYPE' as INDEX1_VALUE,
    'EVENT_TYPE_CODE' as INDEX2_VALUE,
    reference_code as INDEX3_VALUE, 
    (reference_code - 10000) as RESULT1_VALUE 
FROM reference_code 
where reference_type_id=4100006 and reference_code > 10001
ORDER BY reference_code


INSERT INTO DERIVED_ATTRIBUTE_ARRAY 
    (DERIVED_ATTRIBUTE_ID,
    EFFECTIVE_START_DATE,
    EFFECTIVE_END_DATE,
    SEQNR,
    LAST_MODIFIED,
    INDEX1_VALUE,
    INDEX2_VALUE,
    INDEX3_VALUE,
    RESULT1_VALUE)
 select 
    '31001074' as DERIVED_ATTRIBUTE_ID,
    to_date('01-01-1996 00:00:00', 'DD-MM-YYYY HH24:MI:SS') as EFFECTIVE_START_DATE, 
    to_date('30-12-9999 00:00:00', 'DD-MM-YYYY HH24:MI:SS') as EFFECTOVE_END_DATE,
    (ROWNUM + 10) as SEQNR, 
    to_date(sysdate, 'DD-MM-YYYY HH24:MI:SS') as LAST_MODIFIED,
    'REFERENCE_TYPE' as INDEX1_VALUE,
    'EVENT_TYPE_CODE' as INDEX2_VALUE,
    reference_code as INDEX3_VALUE, 
    (reference_code - 10000) as RESULT1_VALUE 
FROM reference_code 
where reference_type_id=4100006 and reference_code > 10001
ORDER BY reference_code

commit

select * from derived_attribute_array where DERIVED_ATTRIBUTE_ID = 31001074 and index1_value='REFERENCE_TYPE' 
select index1_value, index2_value, index3_value, result1_value from derived_attribute_array where DERIVED_ATTRIBUTE_ID = 31001074 and index1_value='REFERENCE_TYPE' 
------------------------------------------------------------------------------------------------------------------------------------------



select 
    '31001074' as DERIVED_ATTRIBUTE_ID,
    to_date('01-01-1996 00:00:00', 'DD-MM-YYYY HH24:MI:SS') as EFFECTIVE_START_DATE, 
    to_date('30-12-9999 00:00:00', 'DD-MM-YYYY HH24:MI:SS') as EFFECTOVE_END_DATE,
    (ROWNUM + 185) as SEQNR, 
    to_date(sysdate, 'DD-MM-YYYY HH24:MI:SS') as LAST_MODIFIED,
    'REFERENCE_TYPE' as INDEX1_VALUE,
    'EVENT_SUB_TYPE_CODE' as INDEX2_VALUE,
    reference_code as INDEX3_VALUE, 
    (reference_code - 200000) as RESULT1_VALUE 
FROM reference_code 
where reference_type_id=4100057 and reference_code > 200000
ORDER BY reference_code

INSERT INTO DERIVED_ATTRIBUTE_ARRAY 
    (DERIVED_ATTRIBUTE_ID,
    EFFECTIVE_START_DATE,
    EFFECTIVE_END_DATE,
    SEQNR,
    LAST_MODIFIED,
    INDEX1_VALUE,
    INDEX2_VALUE,
    INDEX3_VALUE,
    RESULT1_VALUE)
    select 
    '31001074' as DERIVED_ATTRIBUTE_ID,
    to_date('01-01-1996 00:00:00', 'DD-MM-YYYY HH24:MI:SS') as EFFECTIVE_START_DATE, 
    to_date('30-12-9999 00:00:00', 'DD-MM-YYYY HH24:MI:SS') as EFFECTOVE_END_DATE,
    (ROWNUM + 185) as SEQNR, 
    to_date(sysdate, 'DD-MM-YYYY HH24:MI:SS') as LAST_MODIFIED,
    'REFERENCE_TYPE' as INDEX1_VALUE,
    'EVENT_SUB_TYPE_CODE' as INDEX2_VALUE,
    reference_code as INDEX3_VALUE, 
    (reference_code - 200000) as RESULT1_VALUE 
FROM reference_code 
where reference_type_id=4100057 and reference_code > 200000
ORDER BY reference_code

commit
