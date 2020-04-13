declare
    type tabCol IS VARRAY(5) OF VARCHAR2(255);
    l_col_dwh tabcol;
    l_col_sv tabcol;
    total_col_sv NUMBER;
    total_col_dwh NUMBER;
    l_columns_dwh varchar(255);
    l_columns_sv varchar(255);
    l_count NUMBER;
    l_sql varchar2(2000);
    l_exclude_acc varchar2(255);
    l_pba varchar2(5);
    l_bsp varchar2(5);
    l_date date;
BEGIN
    l_col_dwh := tabcol('substr(SVSH.SERVICE_NAME,3) as SERVICE_NAME');
    l_col_sv := tabcol('SERVICE_NAME');
    total_col_dwh := l_col_dwh.COUNT;
    total_col_sv := l_col_sv.COUNT;
    l_exclude_acc := 'PBA22643580,PBA60086806,PBA37052669,PBA17837525,PBA99398691';
    l_pba:= 'PBA';
    l_bsp:= 'BSP%';
    l_date :=TO_DATE('03-02-2020 00:00:00','DD-MM-YYYY HH24:MI:SS');
    
    IF total_col_dwh <> total_col_sv THEN
      DBMS_OUTPUT.put_line ('Number of columns between DWH and SV does not match!!!');
      return;
    END IF;
    
   FOR i in 1 .. total_col_dwh LOOP
      l_sql := 
        ' select count(*) INTO l_count from (
          select  distinct '||l_col_dwh(i)||' from SV_PROD.H3ATT_DWH_SERVICE_HISTORY_V@SV9PCT01.IT.INTERNAL SVSH
          JOIN SV_PROD.ACCOUNT@SV9PCT01.IT.INTERNAL ACC ON SVSH.CUSTOMER_NODE_ID = ACC.CUSTOMER_NODE_ID 
                AND ACCOUNT_NAME NOT IN (:1)
          JOIN SV_PROD.H3AT_PBE_EVENT_AUDIT@SV9PCT01.IT.INTERNAL HPEA ON  ACC.ACCOUNT_NAME = :2 || to_char (HPEA.X_BILL_SITE_ID) 
                AND HPEA.TASK_QUEUE_ID=1001003390
          WHERE 1=1
          minus 
          select distinct '||l_col_sv(i)||' from SV_BUS.SERVICE_HISTORY DWHSH
          JOIN SV_BUS.ACCOUNT ACC ON DWHSH.CUSTOMER_NODE_ID = ACC.CUSTOMER_NODE_ID 
                AND ACCOUNT_NAME NOT IN (22643580,60086806,37052669,17837525,99398691)
          JOIN SV_PROD.H3AT_PBE_EVENT_AUDIT@SV9PCT01.IT.INTERNAL HPEA ON  ACC.ACCOUNT_NAME = to_char (HPEA.X_BILL_SITE_ID) 
                AND HPEA.TASK_QUEUE_ID=1001003390 
          where 1=1 
          and  DWHSH. EFFECTIVE_END_DATE > sysdate 
          and DWHSH.SERVICE_STATUS_CODE not in (9)
          and DWHSH. LAST_MODIFIED < :3
          and DWHSH.SERVICE_NAME NOT LIKE :4       
          )';
          
        EXECUTE IMMEDIATE l_sql USING  l_exclude_acc, l_pba, l_date, l_bsp;
        
        DBMS_OUTPUT.put_line (l_columns_sv || ' ---> Diff Count --> '|| l_count);
        
    END LOOP;
    
END;
