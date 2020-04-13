declare
    type tabCol IS VARRAY(5) OF VARCHAR2(255);
    l_col_dwh tabcol;
    l_col_sv tabcol;
    total_col_sv NUMBER;
    total_col_dwh NUMBER;
    l_columns_dwh varchar(255);
    l_columns_sv varchar(255);
    l_count NUMBER;
BEGIN
    l_col_dwh := tabcol('GENERAL_1, GENERAL_3');
    l_col_sv := tabcol('GENERAL_1, GENERAL_3');
    total_col_dwh := l_col_dwh.COUNT;
    total_col_sv := l_col_sv.COUNT;
    
    IF total_col_dwh <> total_col_sv THEN
      DBMS_OUTPUT.put_line ('Number of columns between DWH and SV does not match!!!');
      return;
    END IF;
    
   FOR i in 1 .. total_col_dwh LOOP
                
        IF i = 1 THEN
          l_columns_dwh := l_col_dwh(i); 
          l_columns_sv  := l_col_sv(i); 
        ELSIF i = total_col_dwh OR i > 1  THEN
          l_columns_dwh := l_columns_dwh ||','||l_col_dwh(i);
          l_columns_sv := l_columns_sv ||','||l_col_sv(i);
        END IF;
       
       l_count := 0;        
       SELECT  count(*) INTO l_count FROM (
              SELECT  DPIH.GENERAL_1, DPIH.GENERAL_3 
                FROM SV_BUS.PRODUCT_INSTANCE_HISTORY DPIH
                JOIN SV_PROD.H3ATT_DWH_PROD_INST_HIST@SV9PCT01.IT.INTERNAL SPIH ON DPIH.GENERAL_1 = SPIH.GENERAL_1 
              WHERE 1=1
                AND DPIH.PRODUCT_INSTANCE_STATUS_CODE <> 9
                AND DPIH.EFFECTIVE_END_DATE > sysdate
                AND DPIH.LAST_MODIFIED < TO_DATE('03-02-2020 00:00:00','DD-MM-YYYY HH24:MI:SS')
                
                MINUS
                
                SELECT  GENERAL_1, GENERAL_3
                FROM SV_PROD.H3ATT_DWH_PROD_INST_HIST@SV9PCT01.IT.INTERNAL 
        ); 
      
        DBMS_OUTPUT.put_line (l_columns_sv || ' ---> Diff Count --> '|| l_count);
        
    END LOOP;
    
END;
