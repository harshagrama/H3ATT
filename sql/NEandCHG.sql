SELECT n.*,c.* 
from h3att_dwh_normalised_event n
join h3att_dwh_charge c on n.normalised_event_id=c.normalised_event_id
where 1=1
and n.general_5 = 'a3 925208837 252214852'
ORDER BY N.PERIOD_START_DATE

select * from normalised_event where normalised_event_id=85267070182

select * from charge where normalised_event_id=85267070182

SELECT n.*,c.* 
from h3att_dwh_normalised_event n
join h3att_dwh_charge c on n.normalised_event_id=c.normalised_event_id
where 1=1
AND n.event_class_code=1

select  from h3att_dwh_normalised_event where event_class_code=2

select n.* 
from normalised_event n
join charge c on n.normalised_event_id=c.normalised_event_id and service_id=42629180 
where 1=1

select * from normalised_event where charge_start_date > sysdate - 60 and event_class_code=2

select * from charge where customer_node_id=34599419 and charge_date > sysdate - 90
--normalised_event_id=85267012218
where 
--service_id=42629180 
normalised_event_id=1022211141 and tariff_id <> 11000001

select * from service_history where service_id=42624727 --service_name='0662640608'

select * from product_history where product_id=31000943

select * from normalised_event_file where PROCESS_START_DATE > sysdate - 1

select * from normalised_event where normalised_event_file_id=1022211195

desc sv_prod.normalised_event_file

select c.*
from charge c
join normalised_event n on c.normalised_event_id=n.normalised_event_id 
join normalised_event_file nf on n.normalised_event_file_id=nf.normalised_event_file_id and PROCESS_START_DATE > sysdate - 1 and n.event_class_code=1
where 1=1
and c.account_id=34673508


-------------------
--DWH
--------------------
select GENERAL_1, charge, c.* from sv_bus.charge c
where c.charge_date >= to_date('01.03.2020','dd.mm.yyyy')
and c.charge_date < to_date('02.03.2020','dd.mm.yyyy')
and TARIFF_ID not in (25000135,25000141,25000142,25000185)
and GENERAL_1 is not null
and GENERAL_1 <> charge 


                        CASE WHEN discount_txt LIKE '%Sorglos%' THEN SUBSTR(discount_txt,18,INSTR(SUBSTR(discount_txt,18,25),'}')-1) ELSE  '' END AS free_units_pot
                       ,CASE WHEN discount_txt LIKE '%FlatRate%' THEN 1 ELSE  0 END AS is_flatrate 
                       ,CASE WHEN discount_txt LIKE '%Sorglos%' THEN 1 ELSE  0 END AS is_free_unit    
                       
                       
                       CASE WHEN TARIFF_ID not in (25000135,25000141,25000142,25000185) THEN REGEXP_SUBSTR(general_4, '[^;]+', 1, 2) END AS volume_invoice 
                       
                       [?07.?04.?2020 11:31]  Schueller,Martin:  
                      ,CASE WHEN INSTR(ch.general_5,'Service_Discount',1) > 0 
                            THEN TO_NUMBER(REPLACE(TRIM(SUBSTR(ch.general_5,INSTR(ch.general_5,':',-1)+1)),',','.'),'9999999999999999D9999999999999','NLS_NUMERIC_CHARACTERS = ''.,''') 
                            ELSE NULL
                       END AS service_discount_pct 
 
