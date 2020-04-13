
/*
ARGOS Trigger to populate audit table of NORMALISED_EVENT.
During rating revoke events are deleted from NORMALISED_EVENT table and files are deleted from NORMALISED_EVENT_FILE tables, 
deleted events must be also deleted from DWH. Hence we pupulate the deleted events data into NORMALISED_EVENT_A table, so that the DWH 
can read the deleted events and update there reports accordingly.
*/
create trigger T_NORMALISED_EVENT_DART
         after delete on NORMALISED_EVENT
for each row
when (old.NORMALISED_EVENT_TYPE_ID=4100003 OR old.NORMALISED_EVENT_TYPE_ID=3100062)
declare
    opcode number (2);
    current_datetime date;
    threshold number (2);
    l_exist number(1);
begin
    threshold         := 5;
    current_datetime := SYSDATE;
    
    SELECT  CASE
                WHEN  EXISTS ( SELECT NEF.NORMALISED_EVENT_FILE_ID 
                                 FROM NORMALISED_EVENT_FILE NEF
                                WHERE NEF.NORMALISED_EVENT_FILE_ID = :old.NORMALISED_EVENT_FILE_ID
                                  AND (NEF.FILENAME LIKE 'SV9_EWSD_BUS_%' OR NEF.FILENAME LIKE 'SV9_IN_BUS_%' 
                                                                          OR NEF.FILENAME LIKE 'NUC_%'
                                                                          OR NEF.FILENAME LIKE 'RGP_%')) 
                THEN 1
                ELSE 0
            END into l_exist
      FROM dual;
    
    if (l_exist = 1) then 
    
        if (deleting or updating) then

            if months_between(current_datetime, :old.charge_start_date) < threshold then

                if (deleting) then
                    opcode := 1;
                else
                    opcode := 2;
                end if;

                insert into H3ATT_DWH_NORMALISED_EVENT_A
                          ( ENTRY_DATE, USERNAME, OPERATION_CODE,
                            NORMALISED_EVENT_ID, CHARGE_START_DATE)
                    values( current_datetime, USER, opcode,
                            :old.NORMALISED_EVENT_ID, :old.CHARGE_START_DATE);
            end if;
        end if;

        if (inserting or updating) then

            if months_between(current_datetime, :new.charge_start_date) < threshold then

                if (inserting) then
                    opcode := 0;
                else
                    opcode := 3;
                end if;

                insert into H3ATT_DWH_NORMALISED_EVENT_A
                          ( ENTRY_DATE, USERNAME, OPERATION_CODE,
                            NORMALISED_EVENT_ID, CHARGE_START_DATE)
                    values( current_datetime, USER, opcode,
                            :old.NORMALISED_EVENT_ID, :old.CHARGE_START_DATE);
            end if;
        end if;
    end if;
end;

/

/* Work around to make sure the trigger is recreated */

create or replace trigger T_NORMALISED_EVENT_DART
         after delete on NORMALISED_EVENT
for each row
when (old.NORMALISED_EVENT_TYPE_ID=4100003 OR old.NORMALISED_EVENT_TYPE_ID=3100062)
declare
    opcode number (2);
    current_datetime date;
    threshold number (2);
    l_exist number(1);
begin
    threshold         := 5;
    current_datetime := SYSDATE;
    
    SELECT  CASE
                WHEN  EXISTS ( SELECT NEF.NORMALISED_EVENT_FILE_ID 
                                 FROM NORMALISED_EVENT_FILE NEF
                                WHERE NEF.NORMALISED_EVENT_FILE_ID = :old.NORMALISED_EVENT_FILE_ID
                                  AND (NEF.FILENAME LIKE 'SV9_EWSD_BUS_%' OR NEF.FILENAME LIKE 'SV9_IN_BUS_%' 
                                                                          OR NEF.FILENAME LIKE 'NUC_%'
                                                                          OR NEF.FILENAME LIKE 'RGP_%')) 
                THEN 1
                ELSE 0
            END into l_exist
      FROM dual;
    
    if (l_exist = 1) then 
    
        if (deleting or updating) then

            if months_between(current_datetime, :old.charge_start_date) < threshold then

                if (deleting) then
                    opcode := 1;
                else
                    opcode := 2;
                end if;

                insert into H3ATT_DWH_NORMALISED_EVENT_A
                          ( ENTRY_DATE, USERNAME, OPERATION_CODE,
                            NORMALISED_EVENT_ID, CHARGE_START_DATE)
                    values( current_datetime, USER, opcode,
                            :old.NORMALISED_EVENT_ID, :old.CHARGE_START_DATE);
            end if;
        end if;

        if (inserting or updating) then

            if months_between(current_datetime, :new.charge_start_date) < threshold then

                if (inserting) then
                    opcode := 0;
                else
                    opcode := 3;
                end if;

                insert into H3ATT_DWH_NORMALISED_EVENT_A
                          ( ENTRY_DATE, USERNAME, OPERATION_CODE,
                            NORMALISED_EVENT_ID, CHARGE_START_DATE)
                    values( current_datetime, USER, opcode,
                            :old.NORMALISED_EVENT_ID, :old.CHARGE_START_DATE);
            end if;
        end if;
    end if;
end;

/

spool off
exit success

