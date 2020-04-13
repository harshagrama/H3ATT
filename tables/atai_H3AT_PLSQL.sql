CREATE OR REPLACE FUNCTION within_payment_stop
(effective_date date, payment_info varchar2) RETURN integer IS
start_date date;
end_date date;
BEGIN
   if (payment_info is null) then
   	  return 0;
   end if;
   start_date := to_date(substr(payment_info, 1, 8), 'yyyymmdd'); 
   end_date := to_date(substr(payment_info, 10, 8), 'yyyymmdd');
   if (effective_date < start_date or effective_date > end_date) then
   	  return 0;
   end if;
   
   RETURN 1;
   
   EXCEPTION
     WHEN OTHERS THEN
       return 0;
END within_payment_stop;
/

CREATE OR REPLACE FUNCTION make_date
(inDate varchar2) RETURN date IS
tmpVar date;
BEGIN
   tmpVar := to_date(nvl(inDate, sysdate), 'dd-mm-yyyy hh24:mi:ss');
   RETURN tmpVar;
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       return to_date('31.12.9000', 'dd.mm.yyyy');
     WHEN OTHERS THEN
       -- Consider logging the error and then re-raise
       return to_date('31.12.9000', 'dd.mm.yyyy');
END make_date;
/

rem
rem Create Procedures
rem

whenever sqlerror exit sql.sqlcode;

CREATE OR REPLACE PROCEDURE update_uninvoiced_old_payments
(
	commit_count number, updated_count out number, run_hours number
) IS
	real_task_end date;

    -- Fetch charge rows to be updated.
    cursor charge_ids is
    select charge_id 
    from charge
    where charge_date < (
        select min(from_date) 
        from atlanta_table_partition
        where from_date is not null
    )
    and (payment_id is not null or adjustment_id is not null)
    and uninvoiced_ind_code = 1;

    tmp_count number;
	a_charge_id number;

BEGIN
    updated_count := 0;
    tmp_count := 0; 
    real_task_end := sysdate + (run_hours/24);

	-- Check if max. task end date reached...
	if(sysdate >= real_task_end) then
		return;
	end if;

    BEGIN
		/* Get charge ids... */
        OPEN charge_ids;
        LOOP
			FETCH charge_ids INTO a_charge_id;
            EXIT WHEN charge_ids%NOTFOUND;

            /* update row(s) .. */
            update charge
            set uninvoiced_ind_code = null,
            last_modified = sysdate
            where charge_id = a_charge_id; 
            
            /* Save count of updated  rows... */
            tmp_count := tmp_count + SQL%rowcount;
            updated_count := updated_count + SQL%rowcount;
            
            /* Check if commit count has been exceeded.. */
            IF (tmp_count >= commit_count) THEN
                COMMIT;
                tmp_count := 0;
                /* Exit is run_hours parameter has been reached.. */
                EXIT WHEN sysdate >= real_task_end;
            END IF;

        END LOOP;
        COMMIT;
    END;

    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            NULL;
        WHEN OTHERS THEN
            -- Consider logging the error and then re-raise
            RAISE;
END update_uninvoiced_old_payments;
/

GRANT EXECUTE ON update_uninvoiced_old_payments TO ATA_MANAGER;
GRANT EXECUTE ON within_payment_stop TO ATA_MANAGER;
GRANT EXECUTE ON make_date TO ATA_MANAGER;
/
CREATE OR REPLACE PACKAGE H3AT_DEV IS

PROCEDURE rebuild_prod_comp(
    offer_family_ref_code varchar2
);

PROCEDURE rebuild_prod_comp(
    offer_family_ref_code varchar2,
    check_only number
);

end H3AT_DEV;
/


CREATE OR REPLACE PACKAGE BODY H3AT_DEV IS

PROCEDURE rebuild_prod_comp(
    offer_family_ref_code varchar2
) IS
BEGIN
    rebuild_prod_comp(offer_family_ref_code, 0);
END rebuild_prod_comp;


PROCEDURE rebuild_prod_comp(
    offer_family_ref_code varchar2,
    check_only number
) IS

    /* ALL Offer Family Products (except HWR Package) */
    CURSOR offer_family_products_cur
    IS
        SELECT
            p.product_id, p.product_name, p.companion_ind_code
        FROM 
            product_history p, derived_attribute_array pc 
        WHERE 
            p.product_id > 16000000 
            AND p.product_id != 26001386 /* HWR Package */
            AND sysdate BETWEEN p.effective_start_date AND p.effective_end_date
            AND p.product_id = pc.index1_value 
            AND sysdate between pc.effective_start_date and pc.effective_end_date
            and pc.derived_attribute_id = 4200347 /* dH3G_ProductClassification& */
            and (
                pc.result2_value = offer_family_ref_code or 
                pc.result2_value = '1'
            )
        ;
    product_rec offer_family_products_cur%ROWTYPE;

    /* ALL other Offer Family COMPANION Products with missing compatibility to current product */
    CURSOR missing_comp_products_cur(for_product_id number)
    IS
        SELECT
            p.product_id, p.product_name, p.companion_ind_code, p.description 
        FROM 
            product_history p, derived_attribute_array pc
        WHERE 
            p.product_id > 16000000 
            AND p.product_id != 26001386 /* HWR Package */
            AND sysdate BETWEEN p.effective_start_date AND p.effective_end_date
            AND p.product_id = pc.index1_value 
            AND sysdate between pc.effective_start_date and pc.effective_end_date
            and pc.derived_attribute_id = 4200347 /* dH3G_ProductClassification& */
            and (
                pc.result2_value = offer_family_ref_code or
                pc.result2_value = '1' /* GENERAL purpose products.. */
            )
            and p.product_id != for_product_id
            and p.companion_ind_code = 1
            and not exists(
                select 1 
                from product_compatibility comp
                where comp.product_id = for_product_id
                and comp.compatible_product_id = p.product_id
            )
        order by p.product_id
        ;
    missing_comp_rec missing_comp_products_cur%ROWTYPE;

    i number; 

BEGIN
    /* Iterate all products of specified offer_family */
    FOR product_rec IN offer_family_products_cur
    LOOP
            dbms_output.put_line('processing product: ' || product_rec.product_name || '(' || product_rec.product_id || ')');
            i := 0;
            /* Iterate missing compatibilities.. */
            FOR missing_comp_rec IN missing_comp_products_cur(product_rec.product_id)
            LOOP
                dbms_output.put_line('Missing comp >' || product_rec.product_name || '< => >' || missing_comp_rec.product_name || '<');
                BEGIN
                    IF check_only != 1 THEN
                        dbms_output.put_line('INSERTING!!!!!!!');
                        INSERT INTO product_compatibility (
                            PRODUCT_ID, COMPATIBLE_PRODUCT_ID, EFFECTIVE_START_DATE, EFFECTIVE_END_DATE, DESCRIPTION
                        ) 
                        VALUES(
                            product_rec.product_id, 
                            missing_comp_rec.product_id, 
                            to_date('1996.01.01 00:00:00','yyyy.mm.dd hh24:mi:ss'),
                            to_date('9999.12.30 23:59:59','yyyy.mm.dd hh24:mi:ss'),
                            missing_comp_rec.description
                        ); 
                        
                        -- Tell ECCS that product has been modified..
                        UPDATE product_history 
                        SET last_modified = sysdate
                        WHERE product_id = product_rec.product_id
                        ; 
                    END IF;
                    i := i + 1;                                        
                EXCEPTION WHEN OTHERS THEN RAISE_APPLICATION_ERROR(-20000, 'prod1:' || product_rec.product_id || ', prod2:' || missing_comp_rec.product_id || '---' || SQLERRM);
                END;
            END LOOP;   
            commit;
            dbms_output.put_line('_____________' || i || ' products missing compatibility.');
    END LOOP;

END rebuild_prod_comp;




end H3AT_DEV;
/
exit;
CREATE OR REPLACE PACKAGE H3AT_CDMMIG IS
    /*
    *
    *   Constants...
    *
    */
    CDM_SCHEDULE_ID constant number := 16480000;
    CDM_DD_TASK_QUEUE_ID constant number := 26000001;
    CDM_STD_REJECT_CODE constant number := 16; -- STORNO
    DUNNING_FEE_ADJ_TYPE_ID constant number := 27000301;
    REJECT_UNINV_ADJ_FLAG constant number := 0;
    DUMMY_GL_CODE_ID constant number := 4400001;

      PAYMENT_TYPE_SAP_BANK constant number := 26000093;
      PAYMENT_TYPE_SAP_CC constant number := 26000094;
      PAYMENT_METHOD_DD constant number := 1;
      PAYMENT_METHOD_CC constant number := 2;

    /*
    *
    *       Generate a history of entities to import from the staging tables..
    *       a_account_number is the account to migrate.
    *
    */
    /* [2015-11-18 WD] Order results by date... */
    CURSOR ar_history_cur(a_account_number varchar2)
    IS
        select * from (
        select
            i.account_number
            ,i.ISSUE_DATE           "EFF_DATE"
            ,decode(i.DUNNING_FEE_FLAG, 1, 'ADJ', 'INV')  "ENTITY_TYPE"
            ,i.INVOICE_RECEIPT_NR
            ,i.BILL_RUN_DATE
            ,i.PAYMENT_DUE_DATE
            ,decode(                       /* B150519 - Debit Adjustments have a NEGATIVE amount - need to convert these.. */
                i.DUNNING_FEE_FLAG, 1,
                i.INVOICE_AMOUNT * -1,
                i.INVOICE_AMOUNT
            ) "INVOICE_AMOUNT"
            ,i.CURRENT_DUE
            ,i.PAY_DUNNING_STOP_DETAILS
            ,null                   "PAYMENT_RECEIPT_NR"
            ,null                   "PAYMENT_RECEIVED_DATE"
            ,null                   "AMOUNT"
            ,null                   "PAYMENT_TYPE_ID"
            ,null                   "PAYMENT_COMMENTS"
        from IF_SV_INVOICE i
        where i.account_number = a_account_number
        UNION
        select
            p.account_number
            ,p.PAYMENT_DATE         "EFF_DATE"
            ,'PAY'                  "ENTITY_TYPE"
            ,null                   "INVOICE_RECEIPT_NR"
            ,null                   "BILL_RUN_DATE"
            ,null                   "PAYMENT_DUE_DATE"
            ,null                   "INVOICE_AMOUNT"
            ,null                   "CURRENT_DUE"
            ,null                   "PAY_DUNNING_STOP_DETAILS"
            ,p.PAYMENT_RECEIPT_NR
            ,p.PAYMENT_RECEIVED_DATE
            ,p.AMOUNT * -1 "AMOUNT"
            ,p.PAYMENT_TYPE_ID
            ,p.PAYMENT_COMMENTS
        from IF_SV_PAYMENT p
        where p.account_number = a_account_number
        ) 
        order by 1,2,3,13 desc
    ;

    /*
    *
    *       Generate a list of allocations for a "payment_receipt_nr" of an account "a_account_number".
    *
    */
    CURSOR ar_pay2inv_cur(a_account_number varchar2, a_payment_receipt_nr varchar2)
    IS
        select
            pi.INVOICE_RECEIPT_NR,
            pi.AMOUNT * -1 "AMOUNT"
        from
            IF_SV_PAYMENT_TO_INVOICE pi
        where
            pi.account_number = a_account_number and
            pi.payment_receipt_nr = a_payment_receipt_nr
    ;



    /*
    *   TYPE definitions used for caching and bulk insert of rows.
    */
    TYPE uninv_adjs_type IS TABLE OF ar_history_cur%ROWTYPE INDEX BY PLS_INTEGER;
    TYPE inv_id_remap_type IS TABLE OF NUMBER INDEX BY pls_integer;
    TYPE receipt2id_map_type IS TABLE OF NUMBER INDEX BY VARCHAR2(30);

    TYPE pay2inv_type is table of payment_invoice%rowtype index by pls_integer;
    TYPE adj2inv_type is table of adjustment_invoice%rowtype index by pls_integer;
    TYPE h3g_direct_debit_type is table of h3g_direct_debit%rowtype index by pls_integer;
    TYPE account_history_type is table of account_history%rowtype index by pls_integer;
    TYPE payment_type is table of payment%rowtype index by pls_integer;
    TYPE invoice_type is table of invoice%rowtype index by pls_integer;
    TYPE adjustment_type is table of adjustment%rowtype index by pls_integer;
    TYPE inv_report_accounts_t_TYPE is table of inv_report_accounts_t%rowtype index by pls_integer;


    /*
    *
    *       Interface functions....
    *
    */
      /* Logs to PLSQL_LOG table in autonomous transaction.. */
      procedure loggi(
              a_err_text varchar2
      );

      /*
              Just calls "migrate_ar_internal" but logs actual ORA exception(if any) using "loggi".
      */
    PROCEDURE migrate_ar(
        a_account_number varchar2,
        out_acc_balance out number
    );


    PROCEDURE migrate_ar_internal(
        a_account_number varchar2,
        out_acc_balance out number
    );



    PROCEDURE remove_ar(
        a_account_number varchar2
    );

    PROCEDURE remove_account(
        a_account_number varchar2
    );

    /*
    *       Verifies for correct number range of receipt numbers..
    */
    procedure verify_number_range(
        ar_history_rec ar_history_cur%ROWTYPE
    );

    function validate_pay_stop_string(
        a_str varchar2
    ) return varchar2;

    /*
    *       Returns the date/time AR data for an account was exported from SAP..
    */
    function get_sap_extract_date (
        a_account_number varchar2
    ) RETURN DATE;


    /*
    *       Returns the dummy BILL_RUN_ID to use for INVOICE inserts..
    */
    function get_sv_bill_run_id(
        a_effective_date date,
        last_bill_run_id in out number
    ) RETURN NUMBER;


    /*
    *       Generates and caches SV INVOICE_IDs for SAP receipt numbers
    */
    FUNCTION geta_sv_invoice_id(
        a_invoice_receipt_nr varchar2
    ) RETURN NUMBER;

    /*
    *       Generates and caches SV PAYMENT_IDs for SAP receipt numbers
    */
    FUNCTION geta_sv_payment_id(
        a_payment_receipt_nr varchar2
    ) RETURN NUMBER;

    /*
    *       Returns the TOTAL_PAYMENTS allocated to an invoice (and the yet unallocated adjustments assigned to this invoice)
    */
    FUNCTION get_inv_total_payments(
        a_account_number varchar2,
        a_invoice_receipt_nr varchar2
        , uninv_adjs in out uninv_adjs_type
    )
    RETURN NUMBER;

    PROCEDURE insert_invoice(
        a_sv_account_id number,
        a_sv_cust_node_id number,
        ar_history_rec ar_history_cur%ROWTYPE,
        a_sv_balance in out number,
        a_sv_seqnr in out number,
        a_sv_invoice_id out number,
        a_sv_bill_run_id in out number
        , uninv_adjs in out uninv_adjs_type
    );

    PROCEDURE insert_payment(
        a_sv_account_id number,
        a_sv_cust_node_id number,
        ar_hist ar_history_cur%ROWTYPE,
        a_sv_balance in out number,
        a_sv_seqnr in out number
        , uninv_adjs in out uninv_adjs_type
    );

    procedure insert_charge_for(
        adj_row adjustment%rowtype
    );

    PROCEDURE reject_payment(
        a_sv_account_id number,
        a_sv_cust_node_id number,
        ar_hist ar_history_cur%ROWTYPE,
        a_sv_balance in out number,
        a_sv_seqnr in out number
    );

    PROCEDURE set_pay_inv_code(
        a_sv_payment_id number,
        a_pay_inv_code number,
        a_sv_invoice_id number
    );

    PROCEDURE insert_payment_allocations(
        a_sv_account_id number,
        a_sv_payment_id number,
        a_account_number varchar2,
        a_payment_receipt_nr varchar2,
        inv_allocs_count out number,
        inv_allocs_total out number,
        a_sv_invoice_id out number
        , uninv_adjs in out uninv_adjs_type
    );

    PROCEDURE insert_payment_allocation(
        a_sv_account_id number,
        a_sv_payment_id number,
        ar_pay2inv_rec ar_pay2inv_cur%ROWTYPE,
        a_pay2inv_seqnr in out number
        , uninv_adjs in out uninv_adjs_type
    );

    PROCEDURE insert_adjustment(
        a_sv_account_id number,
        a_sv_cust_node_id number,
        ar_hist ar_history_cur%ROWTYPE,
        a_sv_balance in out number,
        a_sv_seqnr in out number
        , uninv_adjs in out uninv_adjs_type
    );

    PROCEDURE new_account_history(
        a_sv_account_id number,
        a_effective_date date,
        a_sv_invoice_id number,
        a_sv_payment_id number,
        a_sv_adjustment_id number,
        a_sv_old_balance number,
        a_sv_new_balance number,
        a_sv_seqnr in out number,
        ar_history_rec ar_history_cur%ROWTYPE
    );

    PROCEDURE new_h3g_direct_debit(
        a_sv_invoice_id number
    );

      PROCEDURE pay_h3g_direct_debit(
              a_sv_invoice_id number,
              a_sv_payment_id number,
              payment_amount number
      );

      end H3AT_CDMMIG;
/

CREATE OR REPLACE PACKAGE BODY H3AT_CDMMIG AS
    inv_receipt2id_map receipt2id_map_type;
    pay_receipt2id_map receipt2id_map_type;

    inv_id_remap inv_id_remap_type;

    cur_account_number varchar2(100);
    cur_payment_method_code number;
    cur_extract_date date;
    
    payment_invoice_coll pay2inv_type;
    adjustment_invoice_coll adj2inv_type;
    h3g_direct_debit_coll h3g_direct_debit_type;
    account_history_coll account_history_type;

    payment_coll payment_type;
    invoice_coll invoice_type;
    adjustment_coll adjustment_type;
    inv_report_accounts_t_coll inv_report_accounts_t_type;
  

procedure get_sv_account_details(
    a_account_number varchar2,
    sv_account_id out number,
    sv_cust_node_id out number,
    sv_mig_status out varchar2,
    sv_payment_method_code out number
)
IS
BEGIN
    BEGIN
        SELECT distinct prime_account_id, customer_Node_id, general_10, to_number(payment_method_code)
        INTO sv_account_id, sv_cust_node_id, sv_mig_status, sv_payment_method_code
        from customer_Node_history
        where primary_identifier = a_account_number
        ;

        IF sv_payment_method_code IS NULL THEN
            RAISE_APPLICATION_ERROR(-20013, 'Payment method is missing for customer.');
        END IF;

    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20001, 'Account not found.');
        WHEN TOO_MANY_ROWS THEN
            RAISE_APPLICATION_ERROR(-20015, 'Different history records for customer found.');
    END;
END get_sv_account_details;

PROCEDURE migrate_ar(
    a_account_number varchar2,
    out_acc_balance out number
) IS
    sql_code NUMBER;
    sql_errm VARCHAR2(4000);    
BEGIN
    migrate_ar_internal(a_account_number, out_acc_balance);
    EXCEPTION
        WHEN OTHERS THEN
            sql_code := SQLCODE;
            sql_errm := SQLERRM;
            loggi(
                'AR account >' || a_account_number || '< (1): ' ||
                'SQLCODE >' || to_char(sql_code) || '<, ' ||
                'SQLERRM >' || to_char(sql_errm) || '<'
            );
            loggi(
                'AR account >' || a_account_number || '< (2): ' ||
                'ERROR_STACK >' || to_char(DBMS_UTILITY.FORMAT_ERROR_STACK()) || '<'
            );
            loggi(
                'AR account >' || a_account_number || '< (3): ' ||
                'ERROR_BACKTRACE >' || to_char(DBMS_UTILITY.FORMAT_ERROR_BACKTRACE()) || '<'
            );
            if sql_code > -20000 then /* Use custom error code for any unknown "OTHERS" exceptions.. */
                sql_code := -20030;
            end if;
            RAISE_APPLICATION_ERROR(sql_code, sql_errm);
END migrate_ar;

procedure loggi(
    a_err_text varchar2
) IS
    PRAGMA AUTONOMOUS_TRANSACTION;
BEGIN
    insert into H3AT_plsql_log(when, what) values(sysdate, a_err_text);
    commit;
END loggi;

PROCEDURE migrate_ar_internal(
    a_account_number varchar2,
    out_acc_balance out number
) IS
    /* Get AR history to import..  */

    ar_history_rec ar_history_cur%ROWTYPE;
    a_sv_balance number;
    a_sv_seqnr number;
    sv_account_id number;
    sv_cust_node_id number;
    
    sv_acc_invoice_id number;
    sv_acc_prev_invoice_id number;
    sv_acc_balance_date date;
    
    sv_last_bill_run_id number;
    sv_last_eff_date date;
    i number;
    
    sv_mig_status varchar2(1000);
    sv_payment_method_code number;
    
    -- Stores adjustments since last invoice:
    -- These adjustments need to be added to the next invoice's TOTAL_ADJUSTMENTS/CURRENT_DUE columns
    uninv_adjs uninv_adjs_type;
    
    -- Keeps track of dunning-fee-invoices  modeled as adjustments:
    -- Payments for such invoices have to be allocated to the invoice the adjustment was allocated to.

BEGIN
    
    inv_receipt2id_map.delete();
    pay_receipt2id_map.delete();

    inv_id_remap.delete();

    payment_invoice_coll.delete();
    adjustment_invoice_coll.delete();
    account_history_coll.delete();

    payment_coll.delete();
    invoice_coll.delete();
    h3g_direct_debit_coll.delete();
    adjustment_coll.delete();
    inv_report_accounts_t_coll.delete();

    cur_account_number := '';
    cur_payment_method_code := '0';
    
    cur_account_number := a_account_number;
    cur_extract_date := get_sap_extract_date(cur_account_number);
    
    get_sv_account_details(
        a_account_number,
        sv_account_id,
        sv_cust_node_id,
        sv_mig_status,
        sv_payment_method_code
    );
    cur_payment_method_code := sv_payment_method_code;
    
    -- Ensure that we're working with a migrated customer
    if sv_mig_status is null or sv_mig_status != 'MIG_IN_PROGRESS' then
        RAISE_APPLICATION_ERROR(-20002, 'Wrong migration state >' || sv_mig_status || '<.');
    end if;

    SELECT count(*) 
    INTO i 
    from invoice
    where account_id = sv_account_id
    ;
    
    if i > 0 then
        raise_application_error(-20005, 'SV invoices found for account');
    end if;
    
    SELECT count(*) 
    INTO i 
    from payment
    where to_account_id = sv_account_id
    ;
    
    if i > 0 then
        raise_application_error(-20006, 'SV payments found for account');
    end if;
    
    SELECT count(*) 
    INTO i 
    from adjustment
    where to_account_id = sv_account_id
    ;
    
    if i > 0 then
        raise_application_error(-20007, 'SV adjustments found for account');
    end if;
    
    SELECT count(*) 
    INTO i 
    from account_history
    where account_id = sv_account_id
    ;
    
    if i > 1 then
        raise_application_error(-20008, 'SV account history found');
    end if;
    
    SELECT count(*) 
    INTO i 
    from service_history
    where customer_node_id = sv_cust_node_id
    ;
    
    if i > 0 then
        raise_application_error(-20009, 'SV services found');
    end if;



    a_sv_balance := 0;
    a_sv_seqnr := 1;
    sv_last_bill_run_id := 0;

    /* Get AR history to import..*/
    FOR ar_history_rec IN ar_history_cur(a_account_number)
    LOOP
        dbms_output.put_line('ACC_BALANCE at ' || a_sv_seqnr || ' is ' || a_sv_balance);
        -- start with first available billrun for every new date
        IF ar_history_rec.eff_date != sv_last_eff_date THEN
            dbms_output.put_line('new eff date (' || ar_history_rec.eff_date || ' <> ' || sv_last_eff_date || ')');
            sv_last_bill_run_id := 0;
        END IF;

        sv_last_eff_date := ar_history_rec.eff_date;

        IF ar_history_rec.ENTITY_TYPE = 'INV' THEN
            sv_acc_prev_invoice_id := sv_acc_invoice_id;
            insert_invoice(sv_account_id, sv_cust_node_id, ar_history_rec, a_sv_balance, a_sv_seqnr, sv_acc_invoice_id, sv_last_bill_run_id
                , uninv_adjs
            );
            dbms_output.put_line('!!! LAST BILLRUN ID is now >' || sv_last_bill_run_id || '<');

        ELSIF ar_history_rec.ENTITY_TYPE = 'PAY' THEN
            IF ar_history_rec.PAYMENT_TYPE_ID > -1 THEN
                insert_payment(sv_account_id, sv_cust_node_id, ar_history_rec, a_sv_balance, a_sv_seqnr
                    , uninv_adjs
                );
            ELSE
                reject_payment(sv_account_id, sv_cust_node_id, ar_history_rec, a_sv_balance, a_sv_seqnr);
            END IF;
        ELSIF ar_history_rec.ENTITY_TYPE = 'ADJ' THEN
            insert_adjustment(sv_account_id, sv_cust_node_id, ar_history_rec, a_sv_balance, a_sv_seqnr
                , uninv_adjs
            );
            
            
        END IF;
        sv_acc_balance_date := ar_history_rec.eff_date;
    END LOOP;

    /*    
        Raise error in case no AR data was found..
    */
    IF
        invoice_coll.count = 0
    THEN
        RAISE_APPLICATION_ERROR(-20016, 'No invoices to migrate found.');
    END IF;
    
    -- Raise error in case of uninvoiced adjustments - a separate adjustment type(do exclude them from GL Uploads)
    -- needs to be implemented for such adjustments.
    --  => DO this only if REJECT_UNINV_ADJ_FLAG is set..
    IF H3AT_CDMMIG.REJECT_UNINV_ADJ_FLAG  = 1 THEN
        if uninv_adjs.count > 0 then
            RAISE_APPLICATION_ERROR(-20012, 'Uninvoiced dunning fee receipt found.');
        end if;
    END IF;


    -- Insert invoices..
    FORALL i in indices of invoice_coll
        INSERT INTO invoice VALUES invoice_coll(i);
    -- Insert invoices (inv_report_accounts_t)..
    FORALL i in indices of inv_report_accounts_t_coll
        INSERT INTO inv_report_accounts_t VALUES inv_report_accounts_t_coll(i);

    --
    --  Start of SV9 adaptions: Populate invoice_history with single entries for all invoices...
    --
    insert into invoice_history(
        invoice_id, 
        receivable_type_id, 
        last_modified, 
        seqnr, 
        effective_start_date,
        effective_end_date,
        previous_due,
        current_due,
        payment_id,
        adjustment_id,
        allocated_invoice_id
    )
    select 
        i.invoice_id,                                       --invoice_id
        null,                                               --receivable_type_id, 
        sysdate,                                            --last_modified, 
        1,                                                  --seqnr, 
        i.issue_date,                                       --effective_start_date,
        to_date('99991230 235959', 'yyyymmdd hh24miss'),    --effective_end_date,
        0,                                                  --previous_due,
        i.current_due,                                      --current_due,
        null,                                               --payment_id,
        null,                                               --adjustment_id,
        null                                                --allocated_invoice_id
    from 
        invoice i
    where 
        i.customer_Node_id = sv_cust_node_id
    ;
    --
    --  End of SV9 adaptions
    --
    
    
      
        
    i := payment_coll.FIRST;
    WHILE i IS NOT NULL
    LOOP
        dbms_output.put_line('inserting payment >' || payment_coll(i).RECEIPT_NR || '< with id >' || payment_coll(i).payment_id || '< reject >' || payment_coll(i).PAYMENT_REJECTED_CODE ||'<');
        dbms_output.put_line('2inserting payment for inv >' || payment_coll(i).INVOICE_ID || '<');
        
        if 
            payment_coll(i).INVOICE_ID is not null 
        then
            /* [2015-11-18 WD] Only remap invoices if remapped invoice number exists(this is NOT the case for uninvoiced adjustments)... */
            if inv_id_remap.EXISTS(payment_coll(i).INVOICE_ID) then
                payment_coll(i).INVOICE_ID := inv_id_remap(payment_coll(i).INVOICE_ID);
            else 
                payment_coll(i).INVOICE_ID := null;
                payment_coll(i).PAYMENT_INVOICE_CODE := 1;
                dbms_output.put_line('No invoice number remap found for >' || payment_coll(i).INVOICE_ID || '<');
            end if;
        end if;
        dbms_output.put_line('2ainserting payment for inv >' || payment_coll(i).INVOICE_ID || '<');
            
        -- Insert payments
        INSERT INTO payment VALUES payment_coll(i);
        dbms_output.put_line('done with payment >' || payment_coll(i).RECEIPT_NR || '<');
        i := payment_coll.NEXT(i);
    END LOOP;

        
    -- Insert adjustments
    FORALL i in indices of adjustment_coll
        INSERT INTO adjustment VALUES adjustment_coll(i);

    -- Insert CHARGE records for uninvoiced adjustments..
    i := uninv_adjs.FIRST;
    WHILE i IS NOT NULL
    LOOP
        insert_charge_for(adjustment_coll(i));
        i := uninv_adjs.NEXT(i);
    END LOOP;

    -- Insert adjustment-invoice-allocations..
    FORALL i in indices of adjustment_invoice_coll
        INSERT INTO adjustment_invoice VALUES adjustment_invoice_coll(i);

    -- Remap invoice receipt nrs to real ids
    i := payment_invoice_coll.FIRST;
    WHILE i IS NOT NULL
    LOOP
        dbms_output.put_line('remapping  idx >' || i  || '<');
        dbms_output.put_line('remapping id for id>' || payment_invoice_coll(i).INVOICE_ID  || '<');
        if 
            payment_invoice_coll(i).INVOICE_ID is not null 
        then
            dbms_output.put_line('payment >' || payment_invoice_coll(i).PAYMENT_ID|| '< invoice_id specified, need to remap: ' || payment_invoice_coll(i).INVOICE_ID);
            /* [2015-11-18 WD] Only remap invoices if remapped invoice number exists(this is NOT the case for uninvoiced adjustments)... */
            if inv_id_remap.EXISTS(payment_invoice_coll(i).INVOICE_ID) then
                dbms_output.put_line('payments invoice id remapping....');
                payment_invoice_coll(i).INVOICE_ID := inv_id_remap(payment_invoice_coll(i).INVOICE_ID);
                dbms_output.put_line('..payments invoice id remapped.');
            else 
                dbms_output.put_line('No invoice number remap found for >' || payment_invoice_coll(i).INVOICE_ID || '<');
                payment_invoice_coll(i).INVOICE_ID := null;
                dbms_output.put_line('..set to null');
            end if;
            
            -- eventually insert h3g_direct_debit rows..
            pay_h3g_direct_debit(
                payment_invoice_coll(i).INVOICE_ID,
                payment_invoice_coll(i).PAYMENT_ID,
                payment_invoice_coll(i).AMOUNT
            );

        end if;

        
        dbms_output.put_line('moving on to next invoice..');
        
        i := payment_invoice_coll.NEXT(i);

        
    END LOOP;

    -- Insert payment-invoice-allocations..
    FORALL i IN indices of payment_invoice_coll
        insert into payment_invoice values payment_invoice_coll(i);
    
    -- Insert H3G_DIRECT_DEBIT rows..
    FORALL i IN INDICES OF h3g_direct_debit_coll
        insert into h3g_direct_debit values h3g_direct_debit_coll(i);
    
    -- Insert account_history
    update account_history 
    set EFFECTIVE_END_DATE = account_history_coll(1).EFFECTIVE_START_DATE - 1/86400
    where account_id = sv_account_id;

    FORALL i IN indices of account_history_coll
        insert into account_history values account_history_coll(i);
    
    /* Set current account balance */
    update account
    set 
        account_balance = a_sv_balance,
        balance_date = sv_acc_balance_date,
        invoice_id = sv_acc_invoice_id,
        previous_invoice_id = sv_acc_prev_invoice_id
    where
        account_id = sv_account_id
    ;

    /* Pass account balance to caller */
    out_acc_balance := a_sv_balance;

END migrate_ar_internal;    
    
    
procedure verify_number_range(
    ar_history_rec ar_history_cur%ROWTYPE
)
IS
BEGIN
    BEGIN
        if ar_history_rec.ENTITY_TYPE = 'INV' then
            if(
                ar_history_rec.INVOICE_RECEIPT_NR between 14135350 and 150000000 OR
                ar_history_rec.INVOICE_RECEIPT_NR between 1000000000 AND 1999999999 OR
                ar_history_rec.INVOICE_RECEIPT_NR between 1000000000 AND 1999999999 OR
                ar_history_rec.INVOICE_RECEIPT_NR between 1000000000 AND 1999999999 OR
                ar_history_rec.INVOICE_RECEIPT_NR between 1000000000 AND 1999999999 OR
                ar_history_rec.INVOICE_RECEIPT_NR between 70000000000 AND 70999999999 OR
                ar_history_rec.INVOICE_RECEIPT_NR between 170000000000 AND 170599999999 OR
                ar_history_rec.INVOICE_RECEIPT_NR between 20000000000 AND 20999999999 OR
                ar_history_rec.INVOICE_RECEIPT_NR between 121000000000 AND 121799999999 OR
                ar_history_rec.INVOICE_RECEIPT_NR between 20000000000 AND 20999999999 OR
                ar_history_rec.INVOICE_RECEIPT_NR between 121000000000 AND 121799999999
                /* 20160202 - Additional number ranges for payments modeled as GUTSCHRIFT invoices */
                OR 
                ar_history_rec.INVOICE_RECEIPT_NR between 20000000000	AND 20999999999   OR
                ar_history_rec.INVOICE_RECEIPT_NR between 30000000000	AND 30999999999   OR
                ar_history_rec.INVOICE_RECEIPT_NR between 121000000000	AND 121799999999  OR
                ar_history_rec.INVOICE_RECEIPT_NR between 130000000000	AND 130599999999  OR
                ar_history_rec.INVOICE_RECEIPT_NR between 131000000000	AND 131599999999 
                /* 20160208 - Additional number ranges for payments modeled as GUTSCHRIFT invoices */
                OR 
                ar_history_rec.INVOICE_RECEIPT_NR between 120000000000	AND 120599999999
                /* 20160215 - Additional number ranges for dummy invoices */
                OR 
                ar_history_rec.INVOICE_RECEIPT_NR between 1	AND 14135349 OR
                ar_history_rec.INVOICE_RECEIPT_NR between 800000000000	AND 800014135349 OR
		ar_history_rec.INVOICE_RECEIPT_NR between 300000000 AND 300250000 OR
                ar_history_rec.INVOICE_RECEIPT_NR between 301000000 AND 301020000
		/* 20160607 WD - additional number ranges for AR OCA migration */
		OR
		ar_history_rec.INVOICE_RECEIPT_NR between 3274470 AND 14233332 OR
		ar_history_rec.INVOICE_RECEIPT_NR between 13481928 AND 13567242 OR
		ar_history_rec.INVOICE_RECEIPT_NR between 800033667326 AND 800037919391 
            ) THEN
                return;
            else 
                RAISE_APPLICATION_ERROR(-20011, 'Invalid receipt nr range');
            end IF;
        elsif ar_history_rec.entity_type = 'ADJ' then
            if(
                ar_history_rec.INVOICE_RECEIPT_NR between 140000000000 AND 140599999999
            ) THEN
                return;
            else 
                RAISE_APPLICATION_ERROR(-20011, 'Invalid receipt nr range');
            end IF;
        end if;
    END;
END verify_number_range;


FUNCTION validate_pay_stop_string(
    a_str varchar2
) 
RETURN varchar2 IS 
    PAYMENT_STOP_START_DATE date;
    PAYMENT_STOP_END_DATE date;
    PAYMENT_STOP_REASON number;
    PAYMENT_STOP_TYPE  number;
    PAYMENT_STOP_AGENT_ID varchar2(8);
    PAYMENT_STOP_NOTE varchar2(200);
BEGIN
    BEGIN

        -- Do not validate empty string..
        if a_str is null then
            return a_str;
        end if;
        
        dbms_output.put_line('validating >' || a_str || '<');
        select 
            to_date(
                substr(a_str, 1, instr(a_str, '|', 1, 1) -1)
                ,'yyyymmdd'
            )"from" 
            ,to_date(
                substr(a_str, instr(a_str, '|', 1, 1) +1 , instr(a_str, '|', 1, 2) - instr(a_str, '|', 1, 1) -1)
                ,'yyyymmdd'
            )"to"  
            ,to_number(
                substr(a_str, instr(a_str, '|', 1, 2) +1 , instr(a_str, '|', 1, 3) - instr(a_str, '|', 1, 2) -1) 
            )"reason" 
            ,to_number(
                substr(a_str, instr(a_str, '|', 1, 3) +1 , instr(a_str, '|', 1, 4) - instr(a_str, '|', 1, 3) -1)
            ) "type" 
            ,substr(a_str, instr(a_str, '|', 1, 4) +1 , instr(a_str, '|', 1, 5) - instr(a_str, '|', 1, 4) -1) "agent"
            ,substr(a_str, instr(a_str, '|', 1, 5) +1) "desc"
        into
            PAYMENT_STOP_START_DATE
            ,PAYMENT_STOP_END_DATE
            ,PAYMENT_STOP_REASON
            ,PAYMENT_STOP_TYPE
            ,PAYMENT_STOP_AGENT_ID
            ,PAYMENT_STOP_NOTE
        from dual;
        
        if PAYMENT_STOP_REASON not between 1 and 8 then
            raise_application_error(-20014, 'Could not parse payment/dunning stop string');
        end if;
        if PAYMENT_STOP_TYPE not between 1 and 2 then
            raise_application_error(-20014, 'Could not parse payment/dunning stop string');
        end if;
        if PAYMENT_STOP_AGENT_ID is null then
            raise_application_error(-20014, 'Could not parse payment/dunning stop string');
        end if;
        
        return a_str;
    EXCEPTION
        WHEN OTHERS THEN
            RAISE_APPLICATION_ERROR(-20014, 'Could not parse payment/dunning stop string');
    END;

END validate_pay_stop_string;


--
--      Returns the date/time AR data for an account was exported from SAP..
--
function get_sap_extract_date (
    a_account_number varchar2
) RETURN DATE IS
    a_extract_date date;
BEGIN
    BEGIN
        select migration_dttm
        into a_extract_date
        from if_sv_migdate
        where account_number = a_account_number
        ;
        
        return a_extract_date;
    EXCEPTION
        WHEN TOO_MANY_ROWS THEN
            RAISE_APPLICATION_ERROR(-20023, 'Multiple extract dates found for account');
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20024, 'No extract date found for account');
    END;
END get_sap_extract_date;



function get_sv_bill_run_id(
    a_effective_date date,
    last_bill_run_id in out number
)
RETURN NUMBER IS
    bill_run_id number;
BEGIN
    BEGIN
        select bill_run_id
        into bill_run_id
        from bill_run
        where billing_schedule_id = H3AT_CDMMIG.CDM_SCHEDULE_ID
        and effective_date = a_effective_date
        and bill_run_id > nvl(last_bill_run_id, 0)
        and rownum = 1
        ;
        last_bill_run_id := bill_run_id;
        
        return last_bill_run_id;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20010, 'No billrun found for date >' || to_char(a_effective_date, 'yyyy-mm-dd') || '<');
    END;
END get_sv_bill_run_id;


FUNCTION geta_sv_invoice_id(
    a_invoice_receipt_nr varchar2
)
RETURN NUMBER IS 
    sv_invoice_id number;
BEGIN
    if a_invoice_receipt_nr is null then 
        return null;
    end if;
        dbms_output.put_line('----- Getting invoice id for inv receipt >' || a_invoice_receipt_nr || '<');
        if inv_receipt2id_map.EXISTS(a_invoice_receipt_nr) then
            sv_invoice_id := inv_receipt2id_map(a_invoice_receipt_nr);
            dbms_output.put_line('----- Got invoice id from collection >' || sv_invoice_id || '<');
        else
            BEGIN
                select SEQ_INVOICE.nextval INTO sv_invoice_id FROM DUAL;
                inv_receipt2id_map(a_invoice_receipt_nr) := sv_invoice_id;
                dbms_output.put_line('----- Got invoice id from SEQUENCE >' || sv_invoice_id || '<');
            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    RAISE_APPLICATION_ERROR(-20017, 'Could not get invoice id from sequence.');
            END;
        end if;
    
    return sv_invoice_id;
end geta_sv_invoice_id;


FUNCTION geta_sv_payment_id(
    a_payment_receipt_nr varchar2
)
RETURN NUMBER IS 
    sv_payment_id number;
BEGIN
    if a_payment_receipt_nr is null then 
        return null;
    end if;
    
    if pay_receipt2id_map.EXISTS(a_payment_receipt_nr) then
        sv_payment_id := pay_receipt2id_map(a_payment_receipt_nr);
    else
        BEGIN
            select SEQ_PAYMENT.nextval INTO sv_payment_id FROM DUAL;
            pay_receipt2id_map(a_payment_receipt_nr) := sv_payment_id;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                RAISE_APPLICATION_ERROR(-20018, 'Could not get payment id from sequence.');
        END;
        
    end if;

    return sv_payment_id;
end geta_sv_payment_id;




FUNCTION get_inv_total_payments(
    a_account_number varchar2, 
    a_invoice_receipt_nr varchar2
    , uninv_adjs in out uninv_adjs_type
)
RETURN NUMBER IS
    total_payments number;
    tmp_amount number;
    i number;
BEGIN
    select sum(pi.amount) "TOTAL_PAYMENTS"
    into total_payments
    from IF_SV_PAYMENT_TO_INVOICE pi
    where pi.account_number = a_account_number
    and pi.invoice_receipt_nr = a_invoice_receipt_nr
    ;
    if total_payments is null then
        total_payments := 0.0;
    end if;

    i := uninv_adjs.FIRST;
    WHILE i IS NOT NULL
    LOOP
        dbms_output.put_line('at adj# ' || i || ' with receipt_nr ' || uninv_adjs(i).INVOICE_RECEIPT_NR);
        
        select sum(pi.amount) "TOTAL_PAYMENTS"
        into tmp_amount
        from IF_SV_PAYMENT_TO_INVOICE pi
        where pi.account_number = a_account_number
        and pi.invoice_receipt_nr = uninv_adjs(i).INVOICE_RECEIPT_NR;
        
        total_payments := total_payments + nvl(tmp_amount, 0);

        i := uninv_adjs.NEXT(i);
    END LOOP;

    return total_payments;
end get_inv_total_payments;



procedure insert_charge_for(
    adj_row adjustment%rowtype
) is
    a_charge_id number;
    a_charge_rec charge%rowtype;
begin

    -- Get unique charge id from sequence..
    BEGIN
        select SEQ_CHARGE.nextval * 100 INTO a_charge_id FROM DUAL;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20019, 'Could not get charge id from sequence.');
    END;

    -- Insert charge record for uninvoiced adjustment.
    a_charge_rec.CHARGE_ID := a_charge_id;
    a_charge_rec.LAST_MODIFIED := sysdate;
    a_charge_rec.CHARGE := adj_row.AMOUNT;
    a_charge_rec.CURRENCY_ID := 1021;
    a_charge_rec.ACCOUNT_ID := adj_row.TO_ACCOUNT_ID;
    a_charge_rec.GL_CODE_ID := H3AT_CDMMIG.DUMMY_GL_CODE_ID;
    a_charge_rec.CHARGE_DATE := adj_row.ADJUSTMENT_DATE;
    a_charge_rec.ADJUSTMENT_ID := adj_row.ADJUSTMENT_ID;
    a_charge_rec.GENERAL_8 := '1002|0';
    a_charge_rec.PARTITION_NR := 1;
    a_charge_rec.UNINVOICED_IND_CODE := 1;

    insert into charge values a_charge_rec;
    
end insert_charge_for;

procedure insert_charge_for(
    payment_row payment%rowtype
) is
    a_charge_id number;
    a_charge_rec charge%rowtype;
begin

    -- Get unique charge id from sequence..
    BEGIN
        select SEQ_CHARGE.nextval * 100 INTO a_charge_id FROM DUAL;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20019, 'Could not get charge id from sequence.');
    END;

    -- Insert charge record for uninvoiced adjustment.
    a_charge_rec.CHARGE_ID := a_charge_id;
    a_charge_rec.LAST_MODIFIED := sysdate;
    a_charge_rec.CHARGE := payment_row.AMOUNT;
    a_charge_rec.CURRENCY_ID := 1021;
    a_charge_rec.ACCOUNT_ID := payment_row.TO_ACCOUNT_ID;
    a_charge_rec.GL_CODE_ID := H3AT_CDMMIG.DUMMY_GL_CODE_ID;
    a_charge_rec.CHARGE_DATE := payment_row.PAYMENT_DATE;
    a_charge_rec.PAYMENT_ID := payment_row.PAYMENT_ID;
    a_charge_rec.GENERAL_8 := '1002|0';
    a_charge_rec.PARTITION_NR := 1;
    a_charge_rec.UNINVOICED_IND_CODE := 1;

    insert into charge values a_charge_rec;
    
end insert_charge_for;


PROCEDURE remove_ar(
        a_account_number varchar2
) IS
    sv_mig_status varchar2(1000);
    sv_account_id number;
    sv_cust_node_id number;
    sv_acc_start_date date;
BEGIN
    BEGIN
        SELECT cnh.prime_account_id, cnh.customer_Node_id, cnh.general_10
        INTO sv_account_id, sv_cust_node_id, sv_mig_status
        from customer_Node_history cnh
        where cnh.primary_identifier = a_account_number
        and cnh.effective_start_date = (
            select max(cnh2.effective_start_date)
            from customer_node_history cnh2
            where cnh2.customer_node_id = cnh.customer_node_id
        )
        ;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20001, 'Account not found.');
    END;

    -- Ensure that we're working with a migrated customer
    if sv_mig_status is null then
        RAISE_APPLICATION_ERROR(-20002, 'Wrong migration state >' || sv_mig_status || '<.');
    end if;
    
    -- delete all references to mentioned account
    /* AR data */
    delete from charge where account_id = sv_account_id
        and adjustment_id is not null;
    delete from payment_invoice  where account_id = sv_account_id;
    delete from adjustment_invoice where account_id = sv_account_id;
    delete from h3g_direct_debit where account_name = a_account_number;
    delete from account_history where account_id = sv_account_id
        and seqnr > 1;
    -- Set MAX_DATE in account_history again - otherwise 'NULL oPrimeAccountId' errors during billrun!!
    update account_history
    set EFFECTIVE_END_DATE = to_date('9999-12-30 23:59:59', 'yyyy-mm-dd hh24:mi:ss')
    where account_id = sv_account_id;

    select effective_start_date
    into sv_acc_start_date
    from account_history
    where account_id = sv_account_id;

    update account
    set account_balance = 0,
    invoice_id = null,
    previous_invoice_id = null,
    balance_date = sv_acc_start_date
    where account_id = sv_account_id;

    delete from payment where to_account_id = sv_account_id;
    delete from adjustment where to_account_id = sv_account_id;
    delete from invoice_history ih where ih.invoice_id in(
        select i.invoice_id
        from invoice i
        where i.customer_node_id = sv_cust_node_id
    );
    delete from invoice where customer_Node_id = sv_cust_node_id;
    delete from inv_report_accounts_t where customer_Node_id = sv_cust_node_id;

END remove_ar;



PROCEDURE remove_account(
        a_account_number varchar2
) IS
    sv_mig_status varchar2(1000);
    sv_account_id number;
    sv_cust_node_id number;
BEGIN
    BEGIN
        SELECT cnh.prime_account_id, cnh.customer_Node_id, cnh.general_10
        INTO sv_account_id, sv_cust_node_id, sv_mig_status
        from customer_Node_history cnh
        where cnh.primary_identifier = a_account_number
        and cnh.effective_start_date = (
            select max(cnh2.effective_start_date)
            from customer_node_history cnh2
            where cnh2.customer_node_id = cnh.customer_node_id
        )
        ;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20001, 'Account not found.');
    END;


    -- Ensure that we're working with a migrated customer
    if sv_mig_status is null then
        RAISE_APPLICATION_ERROR(-20002, 'Wrong migration state >' || sv_mig_status || '<.');
    end if;
    

    -- delete all references to mentioned account
    /* AR data */
    remove_ar(a_account_number);

    /* General account data */
    delete from customer_node_inv_format where customer_node_id = sv_cust_node_id;
    delete from customer_node_history where customer_Node_id = sv_cust_node_id;
    delete from customer_Node_da_array where customer_Node_id = sv_cust_node_id;
    delete from customer_Node_da where customer_Node_id = sv_cust_node_id;

    -- delete account itself
    delete from account_history where account_id = sv_account_id;
    delete from account where account_id = sv_account_id;
    delete from customer_Node where customer_Node_id = sv_cust_node_id;

END remove_account;

PROCEDURE new_h3g_direct_debit(
    a_sv_invoice_id number
) IS
    a_h3g_direct_debit_id number;
BEGIN
    -- no need to process payments for non-direct-debit customers..
    if not (cur_payment_method_code = PAYMENT_METHOD_DD or cur_payment_method_code = PAYMENT_METHOD_CC) then
        return;
    end if;

    -- To be collected in H3G stack?
    IF cur_extract_date <= invoice_coll(a_sv_invoice_id).PAYMENT_DUE_DATE THEN 
        dbms_output.put_line('!! To be collected in H3G stack..');
        -- Nothing to pay?
        IF invoice_coll(a_sv_invoice_id).CURRENT_DUE <= 0 then
            dbms_output.put_line('!! Nothing due, return..');
            return;
        END IF;
    ELSE 
        dbms_output.put_line('!! Collected in ORA stack already!');
        -- Already collected in ORA stack
        -- [2015-06-17 WD] Ticket B152854: only insert debit invoices to direct debit table
        IF invoice_coll(a_sv_invoice_id).INVOICE_AMOUNT <= 0 then
            dbms_output.put_line('!! Nothing due, return..');
            return;
        END IF;
    END IF;

    dbms_output.put_line('!!!! Yes, DD for => sv_invoice_id ' || a_sv_invoice_id);

    -- new invoice to insert?
    if h3g_direct_debit_coll.EXISTS(a_sv_invoice_id) then
        RAISE_APPLICATION_ERROR(-20022, 'Problem while inserting DD entry');
    end if;

    BEGIN 
        select SEQ_H3G_DIRECT_DEBIT.nextval INTO a_h3g_direct_debit_id FROM DUAL;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20020, 'Could not get direct debit id from sequence.');
    END;

    h3g_direct_debit_coll(a_sv_invoice_id).h3G_direct_debit_id :=           a_h3g_direct_debit_id;
    h3g_direct_debit_coll(a_sv_invoice_id).task_queue_id :=                 H3AT_CDMMIG.CDM_DD_TASK_QUEUE_ID;
    h3g_direct_debit_coll(a_sv_invoice_id).invoice_nr :=                    invoice_coll(a_sv_invoice_id).CUSTOMER_INVOICE_STR;
    dbms_output.put_line('!!!! DD => set invoice_nr to ' || invoice_coll(a_sv_invoice_id).CUSTOMER_INVOICE_STR);

    h3g_direct_debit_coll(a_sv_invoice_id).payment_due_date :=              invoice_coll(a_sv_invoice_id).PAYMENT_DUE_DATE;
    h3g_direct_debit_coll(a_sv_invoice_id).payment_process_date :=          invoice_coll(a_sv_invoice_id).PAYMENT_DUE_DATE;
    h3g_direct_debit_coll(a_sv_invoice_id).account_name :=                  cur_account_number;
    h3g_direct_debit_coll(a_sv_invoice_id).payment_method_type :=           cur_payment_method_code;
    h3g_direct_debit_coll(a_sv_invoice_id).payment_currency :=              1021;
    h3g_direct_debit_coll(a_sv_invoice_id).bill_cycle_name :=               'BC_AR_Migration_Dummy';
    h3g_direct_debit_coll(a_sv_invoice_id).payment_amount :=                invoice_coll(a_sv_invoice_id).CURRENT_DUE;
    h3g_direct_debit_coll(a_sv_invoice_id).direct_debit_processed_flag :=   0;

    -- payment due date at time of extract passed? Must have been collected in ORA stack..
    IF cur_extract_date > invoice_coll(a_sv_invoice_id).PAYMENT_DUE_DATE THEN 
        h3g_direct_debit_coll(a_sv_invoice_id).direct_debit_processed_flag :=   1;
        invoice_coll(a_sv_invoice_id).GENERAL_10 :=                             1;
    END IF;



END new_h3g_direct_debit;


PROCEDURE pay_h3g_direct_debit(
    a_sv_invoice_id number,
    a_sv_payment_id number,
    payment_amount number
) IS
    a_h3g_direct_debit_id number;
BEGIN
    -- no need to process payments for non-direct-debit customers..
    if not (cur_payment_method_code = PAYMENT_METHOD_DD or cur_payment_method_code = PAYMENT_METHOD_CC) then
        return;
    end if;

    -- no need to process non-direct-debit payments.
    if not (
        payment_coll(a_sv_payment_id).PAYMENT_TYPE_ID = PAYMENT_TYPE_SAP_BANK or -- SAP_BANK
        payment_coll(a_sv_payment_id).PAYMENT_TYPE_ID = PAYMENT_TYPE_SAP_CC  -- SAP_CC
    ) then
        return;
    end if;

    -- No h3g_direct_debit record to be inserted for this invoice?
    if NOT h3g_direct_debit_coll.EXISTS(a_sv_invoice_id) then
        return;
    end if;
    
    h3g_direct_debit_coll(a_sv_invoice_id).payment_amount :=        h3g_direct_debit_coll(a_sv_invoice_id).payment_amount + (payment_amount);
    h3g_direct_debit_coll(a_sv_invoice_id).payment_process_date :=  payment_coll(a_sv_payment_id).PAYMENT_DATE;

END pay_h3g_direct_debit;


PROCEDURE insert_invoice(
    a_sv_account_id number,
    a_sv_cust_node_id number,
    ar_history_rec ar_history_cur%ROWTYPE,
    
    a_sv_balance in out number,
    a_sv_seqnr in out number,
    a_sv_invoice_id out number,
    a_sv_bill_run_id in out number
    
    , uninv_adjs in out uninv_adjs_type
    
    
) IS
    
    inv_allocs_count number;
    inv_allocs_total number;
    total_adj_amount number;
    total_adj_current_due number;
    total_payments number;
    
    idx number;
    adj_rec ar_history_cur%rowtype;
    
    a_adjustment_id number;
    
BEGIN
    dbms_output.put_line('inserting invoice ' || a_sv_account_id || ',' || ar_history_rec.invoice_receipt_nr || '(billrunid >' || a_sv_bill_run_id || '<)');

    a_sv_invoice_id := geta_sv_invoice_id(
        ar_history_rec.invoice_receipt_nr
    );
    a_sv_bill_run_id := get_sv_bill_run_id(
        ar_history_rec.eff_date,
        a_sv_bill_run_id
    );
    dbms_output.put_line('Found bill_run_id ' || a_sv_bill_run_id || ' for ' || to_char(ar_history_rec.eff_date, 'yyyymmdd'));
    
    verify_number_range(ar_history_rec);
    

    -- Fetch all payments allocated to this invoice - also consider adjs not billed until now.
    total_payments := get_inv_total_payments(
        ar_history_rec.account_number, ar_history_rec.invoice_receipt_nr
        , uninv_adjs
    );
    
    -- Fetch all adjustments billed with this invoice - use uninv_adjs structure..
    total_adj_amount := 0;
    total_adj_current_due := 0;

    a_adjustment_id := uninv_adjs.FIRST;
    WHILE a_adjustment_id IS NOT NULL
    LOOP
        dbms_output.put_line('Iterating >' || uninv_adjs.count || '< adjustments..');

        adj_rec := uninv_adjs(a_adjustment_id);
        dbms_output.put_line('at adj# ' || a_adjustment_id);
        
        total_adj_amount := 
                total_adj_amount + adj_rec.INVOICE_AMOUNT;
        total_adj_current_due := 
                total_adj_current_due + adj_rec.CURRENT_DUE;
        
        -- Set invoice id for adjustment
        adjustment_coll(a_adjustment_id).INVOICE_ID := a_sv_invoice_id;
        adjustment_coll(a_adjustment_id).ADJUSTMENT_INVOICE_CODE := 0; -- INVOICE
        adjustment_invoice_coll(a_adjustment_id).INVOICE_ID := a_sv_invoice_id;

        dbms_output.put_line('_____ remapping ' || geta_sv_invoice_id(adj_rec.INVOICE_RECEIPT_NR) || '(for ' || adj_rec.INVOICE_RECEIPT_NR || ') to ' || a_sv_invoice_id);

        -- Remap adj receiptnr to this invoice receipt nr => for payment allocations
        inv_id_remap(geta_sv_invoice_id(adj_rec.INVOICE_RECEIPT_NR)) := 
                a_sv_invoice_id;

        a_adjustment_id := uninv_adjs.NEXT(a_adjustment_id);
    END LOOP;

    dbms_output.put_line('inserting >' || ar_history_rec.invoice_amount || '< and >' || total_adj_amount  || '<');

    dbms_output.put_line('###### inserting invoice ' || a_sv_invoice_id || ',' || ar_history_rec.invoice_receipt_nr);

    
    idx := a_sv_invoice_id;
    invoice_coll(idx).INVOICE_ID :=                    a_sv_invoice_id;
    invoice_coll(idx).LAST_MODIFIED :=                 sysdate;
    invoice_coll(idx).CUSTOMER_INVOICE_STR :=          ar_history_rec.INVOICE_RECEIPT_NR;
    invoice_coll(idx).INVOICE_TYPE_ID :=               4200001;
    invoice_coll(idx).BILL_RUN_ID :=                   a_sv_bill_run_id;
    invoice_coll(idx).ACCOUNT_ID :=                    a_sv_account_id;
    invoice_coll(idx).CUSTOMER_NODE_ID :=              a_sv_cust_node_id;
    invoice_coll(idx).IMAGE_GENERATED_IND_CODE :=      1;
    invoice_coll(idx).APPLIED_IND_CODE :=              1;
    invoice_coll(idx).EFFECTIVE_DATE :=                ar_history_rec.bill_run_date;
    invoice_coll(idx).ISSUE_DATE :=                    ar_history_rec.bill_run_date;
    invoice_coll(idx).ORIGINAL_PAYMENT_DUE_DATE :=     ar_history_rec.payment_due_date;
    invoice_coll(idx).PAYMENT_DUE_DATE :=              ar_history_rec.payment_due_date;
    invoice_coll(idx).EARLY_PAYMENT_DATE :=            ar_history_rec.payment_due_date;
    invoice_coll(idx).INVOICE_AMOUNT :=                ar_history_rec.invoice_amount;
    invoice_coll(idx).BALANCE_FORWARD :=               a_sv_balance;
    invoice_coll(idx).ACCOUNT_BALANCE :=               a_sv_balance + ar_history_rec.invoice_amount;
    invoice_coll(idx).ACCOUNT_INITIAL_DUE :=           ar_history_rec.invoice_amount - total_adj_amount;
    invoice_coll(idx).CURRENT_DUE :=                   ar_history_rec.current_due - total_adj_current_due;
    invoice_coll(idx).TOTAL_PAYMENTS :=                total_payments;
    invoice_coll(idx).TOTAL_ADJUSTMENTS :=             total_adj_amount * -1;
    invoice_coll(idx).UNBILLED_AMOUNT :=               0;
    invoice_coll(idx).GENERAL_6 :=                     to_char(ar_history_rec.eff_date, 'dd-mm-yyyy hh24:mi:ss');
    invoice_coll(idx).GENERAL_9 :=                     to_char(ar_history_rec.payment_due_date, 'dd-mm-yyyy hh24:mi:ss');
    invoice_coll(idx).GENERAL_5 :=                     validate_pay_stop_string(ar_history_rec.pay_dunning_stop_details);
    --
    -- SV9 adaptions
    --
    invoice_coll(idx).DISPUTED_AMOUNT :=               0;
    
    
    -- create inv_report_accounts_t entry..
    idx := inv_report_accounts_t_coll.count + 1;
    inv_report_accounts_t_coll(idx).bill_run_id :=      a_sv_bill_run_id;
    inv_report_accounts_t_coll(idx).customer_node_id := a_sv_cust_node_id;
    inv_report_accounts_t_coll(idx).invoice_seqnr :=    1;
    inv_report_accounts_t_coll(idx).seqnr :=            0;
    inv_report_accounts_t_coll(idx).account_id :=       a_sv_account_id;
    
    -- update account history...
    new_account_history(
        a_sv_account_id,
        ar_history_rec.eff_date,
        a_sv_invoice_id,
        null,
        null,
        a_sv_balance, 
        a_sv_balance + ar_history_rec.invoice_amount,
        a_sv_seqnr,
        ar_history_rec
    );
        
    -- update balance with current AR record..
    a_sv_balance := a_sv_balance + ar_history_rec.invoice_amount;
    
    -- No need to remap invoice number for payment targets (as this is a "real" SV invoice)
    inv_id_remap(a_sv_invoice_id) := a_sv_invoice_id;

    -- (Possibly) insert entry for direct debit payment..
    new_h3g_direct_debit(
        a_sv_invoice_id
    );
    
    
    uninv_adjs.DELETE;
    
END insert_invoice;






PROCEDURE insert_adjustment(
    a_sv_account_id number,
    a_sv_cust_node_id number,
    ar_hist ar_history_cur%ROWTYPE,
    a_sv_balance in out number,
    a_sv_seqnr in out number
    , uninv_adjs in out uninv_adjs_type
    
) IS
    a_sv_adjustment_id  number;
    next_inv_receipt_nr number;
    
    idx number;
BEGIN
    BEGIN
        select SEQ_ADJUSTMENT.nextval INTO a_sv_adjustment_id FROM DUAL;
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20021, 'Could not get adjustment id from sequence.');
    END;

    idx := a_sv_adjustment_id;
    adjustment_coll(idx).ADJUSTMENT_ID :=                a_sv_adjustment_id;
    adjustment_coll(idx).LAST_MODIFIED :=                sysdate;
    adjustment_coll(idx).ADJUSTMENT_NR :=                'A' || ar_hist.INVOICE_RECEIPT_NR;
    adjustment_coll(idx).ADJUSTMENT_TYPE_ID :=           H3AT_CDMMIG.DUNNING_FEE_ADJ_TYPE_ID;
    adjustment_coll(idx).ADJUSTMENT_STATUS_CODE :=       1;
    adjustment_coll(idx).DESCRIPTION :=                  'Mahnspesen';
    adjustment_coll(idx).ADJUSTMENT_TEXT :=              
            'Mahnspesen f' || chr(252) || 'r Zahlungserinnerung vom ' || to_char(ar_hist.EFF_DATE, 'dd.mm.yyyy');
    adjustment_coll(idx).TO_ACCOUNT_ID :=                a_sv_account_id;
    adjustment_coll(idx).TO_GL_CODE_ID :=                H3AT_CDMMIG.DUMMY_GL_CODE_ID;
    adjustment_coll(idx).ADJUSTMENT_INVOICE_CODE :=      2; -- CREDIT
    adjustment_coll(idx).ATLANTA_OPERATOR_ID :=          4100086;
    adjustment_coll(idx).CURRENCY_ID :=                  1021;
    adjustment_coll(idx).CURRENCY_CONVERSION_DATE :=     ar_hist.EFF_DATE;
    adjustment_coll(idx).AMOUNT :=                       ar_hist.INVOICE_AMOUNT *-1;
    adjustment_coll(idx).TO_AMOUNT :=                    ar_hist.INVOICE_AMOUNT *-1;
    adjustment_coll(idx).TO_ADJUST_AMOUNT :=             0;
    adjustment_coll(idx).TO_TOTAL_AMOUNT :=              ar_hist.INVOICE_AMOUNT *-1;
    adjustment_coll(idx).ADJUSTMENT_DATE :=              ar_hist.EFF_DATE;
    adjustment_coll(idx).ADJUSTMENT_RECEIVED_DATE :=     ar_hist.EFF_DATE;
    adjustment_coll(idx).GENERAL_4 :=                    '1002';
    adjustment_coll(idx).INVOICE_ID :=                   null; -- gets populated when next invoice gets inserted..


    idx := a_sv_adjustment_id;
    adjustment_invoice_coll(idx).ACCOUNT_ID := a_sv_account_id;
    adjustment_invoice_coll(idx).ADJUSTMENT_ID := a_sv_adjustment_id;
    adjustment_invoice_coll(idx).AMOUNT := ar_hist.INVOICE_AMOUNT * -1;
    adjustment_invoice_coll(idx).INVOICE_ID := null; -- gets populated when next invoice gets inserted..
    adjustment_invoice_coll(idx).LAST_MODIFIED := sysdate; 
    adjustment_invoice_coll(idx).SEQNR := 1;
    
    -- update account history...
    new_account_history(
        a_sv_account_id,
        ar_hist.EFF_DATE,
        null,
        null,
        a_sv_adjustment_id,
        a_sv_balance, 
        a_sv_balance + ar_hist.INVOICE_AMOUNT,
        a_sv_seqnr,
        ar_hist
    );
        
    -- update balance with current AR record..
    a_sv_balance := a_sv_balance + ar_hist.INVOICE_AMOUNT;

    -- Add adjustment to list of currently uninvoiced..
    uninv_adjs(a_sv_adjustment_id) := ar_hist;
    
    
END insert_adjustment;



PROCEDURE insert_payment(
    a_sv_account_id number,
    a_sv_cust_node_id number,
    ar_hist ar_history_cur%ROWTYPE,
    a_sv_balance in out number,
    a_sv_seqnr in out number
    , uninv_adjs in out uninv_adjs_type
) IS
    a_sv_payment_id  number;
    a_payment_invoice_code number;
    a_sv_invoice_id  number;
    inv_allocs_count number;
    inv_allocs_total number;
    
    idx number;
BEGIN
    dbms_output.put_line('inserting payment ' || a_sv_account_id || ',' || ar_hist.payment_receipt_nr || ' -- ' || ar_hist.amount);

    a_sv_payment_id := geta_sv_payment_id(
        ar_hist.PAYMENT_RECEIPT_NR
    );

    idx := a_sv_payment_id;
    payment_coll(idx).AMOUNT                   :=         ar_hist.AMOUNT;
    payment_coll(idx).ATLANTA_OPERATOR_ID      :=         4100086;
    payment_coll(idx).CURRENCY_CONVERSION_DATE :=         ar_hist.EFF_DATE;
    payment_coll(idx).CURRENCY_ID              :=         1021;
    payment_coll(idx).CUSTOMER_NODE_ID         :=         a_sv_cust_node_id;
    payment_coll(idx).GENERAL_1                :=         16400040;                       -- GENERAL_1 => PAYMENT_BATCH_UD 'CDM - AR Migration'
    payment_coll(idx).GENERAL_3                :=         ar_hist.PAYMENT_COMMENTS;
    payment_coll(idx).GENERAL_5                :=         ar_hist.EFF_DATE;          
    payment_coll(idx).INVOICE_ID               :=         null;
    payment_coll(idx).LAST_MODIFIED            :=         sysdate;
    payment_coll(idx).PAYMENT_DATE             :=         ar_hist.EFF_DATE;
    payment_coll(idx).PAYMENT_ID               :=         a_sv_payment_id;
    payment_coll(idx).PAYMENT_INVOICE_CODE     :=         1;
    payment_coll(idx).PAYMENT_LOCATION_CODE    :=         8;
    payment_coll(idx).PAYMENT_RECEIVED_DATE    :=         ar_hist.EFF_DATE;
    payment_coll(idx).PAYMENT_STATUS_CODE      :=         1;
    payment_coll(idx).PAYMENT_TYPE_ID          :=         ar_hist.PAYMENT_TYPE_ID;
    payment_coll(idx).RECEIPT_NR               :=         ar_hist.PAYMENT_RECEIPT_NR;
    payment_coll(idx).TO_ACCOUNT_ID            :=         a_sv_account_id;
    payment_coll(idx).TO_ADJUST_AMOUNT         :=         0;
    payment_coll(idx).TO_AMOUNT                :=         ar_hist.AMOUNT;
    payment_coll(idx).TO_GL_CODE_ID            :=         H3AT_CDMMIG.DUMMY_GL_CODE_ID;
    payment_coll(idx).TO_TOTAL_AMOUNT          :=         ar_hist.AMOUNT;

    insert_payment_allocations(
        a_sv_account_id,
        a_sv_payment_id,
        ar_hist.ACCOUNT_NUMBER,
        ar_hist.PAYMENT_RECEIPT_NR,
        inv_allocs_count,
        inv_allocs_total,
        a_sv_invoice_id
        , uninv_adjs
        
    );
    
    
    -- set PAYMENT_INVOICE_CODE/INVOICE_ID
    a_payment_invoice_code := 2;
    IF inv_allocs_count = 0 THEN
        set_pay_inv_code(a_sv_payment_id, 2, null);
    ELSIF inv_allocs_count = 1 THEN
        set_pay_inv_code(a_sv_payment_id, 0, a_sv_invoice_id);
    ELSE
        set_pay_inv_code(a_sv_payment_id, 1, null);
    END IF;
    
    -- update account history...
    new_account_history(
        a_sv_account_id,
        ar_hist.EFF_DATE,
        null,
        a_sv_payment_id,
        null,
        a_sv_balance, 
        a_sv_balance - ar_hist.AMOUNT,
        a_sv_seqnr,
        ar_hist
    );
        
    -- update balance with current AR record..
    a_sv_balance := a_sv_balance - ar_hist.AMOUNT;

    
END insert_payment;



PROCEDURE reject_payment(
    a_sv_account_id number,
    a_sv_cust_node_id number,
    ar_hist ar_history_cur%ROWTYPE,
    a_sv_balance in out number,
    a_sv_seqnr in out number
) IS
    a_sv_payment_id  number;
    a_payment_invoice_code number;
    a_sv_invoice_id  number;
    inv_allocs_count number;
    inv_allocs_total number;
BEGIN
    dbms_output.put_line('Rejecting payment with receipt nr >' || ar_hist.payment_receipt_nr || '<');
    
    a_sv_payment_id := geta_sv_payment_id(
        ar_hist.PAYMENT_RECEIPT_NR
    );
    
    IF  
        NOT payment_coll.EXISTS(a_sv_payment_id)
    THEN
        RAISE_APPLICATION_ERROR(-20022, 'Could not find original payment for reject.');
    END IF;

    payment_coll(a_sv_payment_id).GENERAL_3 := ar_hist.PAYMENT_COMMENTS;
    payment_coll(a_sv_payment_id).GENERAL_5 := ar_hist.EFF_DATE;
    payment_coll(a_sv_payment_id).LAST_MODIFIED := sysdate;
    payment_coll(a_sv_payment_id).PAYMENT_STATUS_CODE := 2;
    payment_coll(a_sv_payment_id).REJECTED_DATE := ar_hist.EFF_DATE;
    payment_coll(a_sv_payment_id).PAYMENT_REJECTED_CODE := H3AT_CDMMIG.CDM_STD_REJECT_CODE;
        
    -- update account history...
    new_account_history(
        a_sv_account_id,
        ar_hist.EFF_DATE,
        null,
        a_sv_payment_id,
        null,
        a_sv_balance, 
        a_sv_balance - ar_hist.AMOUNT,
        a_sv_seqnr,
        ar_hist
    );
        
    -- update balance with current AR record..
    a_sv_balance := a_sv_balance - ar_hist.AMOUNT;

    
END reject_payment;


PROCEDURE set_pay_inv_code(
    a_sv_payment_id number,
    a_pay_inv_code number,
    a_sv_invoice_id number
) IS
BEGIN
    payment_coll(a_sv_payment_id).PAYMENT_INVOICE_CODE := a_pay_inv_code;
    payment_coll(a_sv_payment_id).INVOICE_ID := a_sv_invoice_id;
END set_pay_inv_code;




PROCEDURE insert_payment_allocations(
    a_sv_account_id number,
    a_sv_payment_id number,
    a_account_number varchar2,
    a_payment_receipt_nr varchar2,
    inv_allocs_count out number,
    inv_allocs_total out number,
    a_sv_invoice_id out number
    , uninv_adjs in out uninv_adjs_type
    

) IS
    ar_pay2inv_rec ar_pay2inv_cur%ROWTYPE;
    
    a_pay2inv_seqnr number := 1;

BEGIN
    inv_allocs_total := 0;
    inv_allocs_count := 0;

    FOR ar_pay2inv_rec IN ar_pay2inv_cur(a_account_number, a_payment_receipt_nr)
    LOOP
        insert_payment_allocation(
            a_sv_account_id, 
            a_sv_payment_id, 
            ar_pay2inv_rec,
            a_pay2inv_seqnr
            , uninv_adjs
            
        );

        IF ar_pay2inv_rec.INVOICE_RECEIPT_NR IS NOT NULL THEN
            inv_allocs_total := inv_allocs_total + ar_pay2inv_rec.AMOUNT;
            inv_allocs_count := inv_allocs_count + 1;
            
            a_sv_invoice_id := geta_sv_invoice_id(ar_pay2inv_rec.INVOICE_RECEIPT_NR);
        END IF;
    END LOOP;
END insert_payment_allocations;            


PROCEDURE insert_payment_allocation(
    a_sv_account_id number,
    a_sv_payment_id number,
    ar_pay2inv_rec ar_pay2inv_cur%ROWTYPE,
    a_pay2inv_seqnr in out number
    , uninv_adjs in out uninv_adjs_type
    
) IS
    tmp_sv_invoice_id number;
    idx number;
BEGIN
    dbms_output.put_line('getting remapped inv number for ' || ar_pay2inv_rec.INVOICE_RECEIPT_NR);
    
    if ar_pay2inv_rec.INVOICE_RECEIPT_NR is not null then
        tmp_sv_invoice_id := geta_sv_invoice_id(ar_pay2inv_rec.INVOICE_RECEIPT_NR);
    else
        tmp_sv_invoice_id := null;
    end if;
    
    idx := payment_invoice_coll.count +1;
    payment_invoice_coll(idx).ACCOUNT_ID := a_sv_account_id;
    payment_invoice_coll(idx).PAYMENT_ID := a_sv_payment_id;
    payment_invoice_coll(idx).AMOUNT := ar_pay2inv_rec.AMOUNT;
    -- Storing (possibly dummy) sv invoice id! remapping happens in migrate_ar before inserting to db.
    payment_invoice_coll(idx).INVOICE_ID := tmp_sv_invoice_id; 
    payment_invoice_coll(idx).LAST_MODIFIED := sysdate; 
    payment_invoice_coll(idx).SEQNR := a_pay2inv_seqnr;

    a_pay2inv_seqnr := a_pay2inv_seqnr + 1;
    
    
    
END insert_payment_allocation;



PROCEDURE new_account_history(
    a_sv_account_id number,
    a_effective_date date,
    a_sv_invoice_id number,
    a_sv_payment_id number,
    a_sv_adjustment_id number,
    a_sv_old_balance number,
    a_sv_new_balance number,
    a_sv_seqnr in out number,
    ar_history_rec ar_history_cur%ROWTYPE
) IS
    idx number;
BEGIN
    idx := account_history_coll.count +1;
    
    if a_sv_seqnr > 1 then
        account_history_coll(idx -1).EFFECTIVE_END_DATE := GREATEST(
            account_history_coll(idx -1).EFFECTIVE_START_DATE,
            a_effective_date - (1 / 86400)
        );
    end if;

    a_sv_seqnr := a_sv_seqnr + 1;
    account_history_coll(idx).ACCOUNT_ID := a_sv_account_id;
    account_history_coll(idx).SEQNR := a_sv_seqnr;
    account_history_coll(idx).LAST_MODIFIED := sysdate;
    account_history_coll(idx).EFFECTIVE_START_DATE := a_effective_date;
    account_history_coll(idx).EFFECTIVE_END_DATE := to_date('99991230 235959', 'yyyymmdd hh24:mi:ss');
    account_history_coll(idx).PREVIOUS_BALANCE := a_sv_old_balance;
    account_history_coll(idx).CURRENT_BALANCE := a_sv_new_balance;
    account_history_coll(idx).INVOICE_ID := a_sv_invoice_id; 
    account_history_coll(idx).PAYMENT_ID := a_sv_payment_id; 
    account_history_coll(idx).ADJUSTMENT_ID := a_sv_adjustment_id;
    account_history_coll(idx).APPLIED_DATE := a_effective_date;

END new_account_history;



end H3AT_CDMMIG;
/



show errors;

create or replace synonym app_sv_ro.h3at_cdmmig for h3at_cdmmig;
grant execute on h3aT_cdmmig to app_sv_ro;


CREATE OR REPLACE PACKAGE H3AT_CLEANUP IS
-- 
-- Introduced with CSGI HealthCheck in September 2013.
-- 
-- 

PROCEDURE purge_expired_ticket_subtotals(
	effective_date varchar2, 
    ticket_expiry_purge_days number, 
    commit_count number, 
    del_count out number, 
    task_end varchar2
);

PROCEDURE purge_service_allocations(
    DAID number, 
    purgedays number, 
    effective_date varchar2, 
    block_size number, 
    sda_del_count out number,  
    subtotal_del_count out number, 
    run_hours number
);

PROCEDURE purge_customer_allocations(
    DAID number, 
    purgedays number, 
    effective_date varchar2, 
    block_size number, 
    del_count out number, 
    run_hours number
);


PROCEDURE purge_subtotal_rating_delta(
	commit_count number, 
    del_count out number, 
    run_hours number
);


PROCEDURE purge_subtotal_rating_value(
	in_subtotal_id number, 
    effective_date varchar2, 
    max_end_date_purge_days number, 
    commit_count number, 
    del_count out number, 
    task_end varchar2
);

end H3AT_CLEANUP;
/


CREATE OR REPLACE PACKAGE BODY H3AT_CLEANUP IS
-- 
-- Introduced with CSGI HealthCheck in September 2013.
-- 
-- 

PROCEDURE purge_expired_ticket_subtotals(
    effective_date varchar2, 
    ticket_expiry_purge_days number, 
    commit_count number, 
    del_count out number, 
    task_end varchar2
) IS

	real_task_end date;

	-- Fetch subtotal entries for expired subtotals..
	CURSOR subtotal_rows IS
        select 
          srv.subtotal_id, srv.service_id, srv.key_value
        from 
          subtotal_rating_value partition(SUBTOTAL_RATING_VALUE_P999_01) srv
        where 
          1=1 
          and srv.subtotal_id in (
            26000053, /* sS_H3AT_InitiativeDataCounter#{} */
            26000055  /* sS_H3AT_RAT_TicketCounter#{} */
          )
          and length(srv.key_value) > 29
          and 
            /* =>the date to purge tickets expiring before, e.g. '20130730000000'.. */ 
            substr(srv.key_value, 16, 14) < 
                to_char(make_date(effective_date) - ticket_expiry_purge_days, 'yyyymmdd') || '000000' 
        ;

    tmp_count number;
	a_subtotal_id number;
	a_service_id number;
	a_key_value varchar(200);

BEGIN
    del_count := 0;
    tmp_count := 0; 
    real_task_end := to_date(task_end, 'dd-mm-yyyy hh24:mi:ss');

	-- Check if max. task end date reached...
	if(sysdate >= real_task_end) then
		return;
	end if;

    BEGIN
		/* Get rows... */
        OPEN subtotal_rows;
        LOOP
            FETCH subtotal_rows INTO a_subtotal_id, a_service_id, a_key_value;
            EXIT WHEN subtotal_rows%NOTFOUND;

            /* Delete row(s) .. */
        	DELETE FROM subtotal_rating_value
			WHERE subtotal_id = a_subtotal_id
			AND service_id = a_service_id
			AND key_value = a_key_value;
			
			/* Save count of deleted rows... */
            tmp_count := tmp_count + SQL%rowcount;
            del_count := del_count + SQL%rowcount;
            
			/* Check if commit count has been exceeded.. */
            IF (tmp_count >= commit_count) THEN
                COMMIT;
                tmp_count := 0;
                /* Exit is run_hours parameter has been reached.. */
                EXIT WHEN sysdate >= real_task_end;
            END IF;

        END LOOP;
        COMMIT;
    END;

    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            NULL;
        /*WHEN OTHERS THEN
            -- Consider logging the error and then re-raise
            RAISE;*/
END purge_expired_ticket_subtotals;


PROCEDURE purge_service_allocations(
    DAID number, 
    purgedays number, 
    effective_date varchar2, 
    block_size number, 
    sda_del_count out number,  
    subtotal_del_count out number, 
    run_hours number
) IS
    real_date date;
    
    CURSOR c1 IS 
    SELECT /*+  PARALLEL (sda, 8) PARALLEL (sh, 8) */ 
        sda.service_id, 
        sh.customer_node_id,
        sda.DERIVED_ATTRIBUTE_ID, 
        sda.index1_value 
    FROM 
        service_da_array sda, service_history sh
    WHERE
        sda.derived_attribute_id = DAID 
        AND sda.service_id = sh.service_id
        AND sda.effective_start_date BETWEEN sh.effective_start_date AND  sh.effective_end_date
    GROUP BY
        sda.service_id, 
	sh.customer_node_id,
        sda.DERIVED_ATTRIBUTE_ID, 
        sda.index1_value 
    HAVING 
        max(make_date(sda.result4_value)) < real_date - purgedays;
        
    TYPE DeptCurTyp IS REF CURSOR;
    rec DeptCurTyp;  -- declare cursor variable
    iterator number;
    end_date date;
    deleted number;
BEGIN
	end_date := sysdate + (run_hours / 24);
	iterator := 0;
	sda_del_count := 0;
	subtotal_del_count := 0;
	real_date := to_date(effective_date, 'dd-mm-yyyy hh24:mi:ss');
	FOR rec IN c1 LOOP
		/* Delete allocation details... */
        DELETE FROM service_da_array 
        WHERE SERVICE_ID = rec.SERVICE_ID 
        AND DERIVED_ATTRIBUTE_ID = rec.DERIVED_ATTRIBUTE_ID 
		AND make_date(result4_value) < real_date - purgedays;
		-- GS, 2018.09.12, delete per service_id for expired allocations not per allocation_id
		-- SV Mini Health check 2018, performance enhancement
        --AND index1_value = rec.index1_value;

        deleted := SQL%rowcount;
		sda_del_count := sda_del_count + deleted;
        iterator := iterator + deleted;
        
		/* Delete consumption details... */
        DELETE FROM subtotal_rating_value 
		WHERE subtotal_id = 4200125 
		AND customer_node_id = rec.customer_node_id 
		AND key_value = rec.index1_value;
        
        deleted := SQL%rowcount;
        subtotal_del_count := subtotal_del_count + deleted;
		iterator := iterator + deleted;
		--DBMS_OUTPUT.Put_Line('x records deleted');

        if (iterator > block_size) then
		   commit;
           --DBMS_OUTPUT.Put_Line('commit executed');
		   iterator := 0;
		   EXIT WHEN sysdate >= end_date;
		end if;
	END LOOP;
	commit;
    --DBMS_OUTPUT.Put_Line('commit executed');
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       NULL;
END purge_service_allocations;


procedure purge_customer_allocations(
    DAID number, 
    purgedays number, 
    effective_date varchar2, 
    block_size number, 
    del_count out number, 
    run_hours number
) IS
    real_date date;
    CURSOR c1 IS 
        SELECT /*+  PARALLEL (cda, 4) */ 
            cda.CUSTOMER_NODE_ID, cda.DERIVED_ATTRIBUTE_ID, cda.index1_value 
        from 
            customer_node_da_array cda 
        where 
            cda.derived_attribute_id = DAID 
        group by cda.CUSTOMER_NODE_ID, cda.DERIVED_ATTRIBUTE_ID, cda.index1_value 
        having max(make_date(cda.result4_value)) < real_date - purgedays;

    TYPE DeptCurTyp IS REF CURSOR;
    rec DeptCurTyp;  -- declare cursor variable
    iterator number;
    end_date date;
    deleted number;
BEGIN
	end_date := sysdate + (run_hours / 24);
	iterator := 0;
	del_count := 0;
	real_date := to_date(effective_date, 'dd-mm-yyyy hh24:mi:ss');
	FOR rec IN c1 LOOP
		delete from customer_node_da_array where CUSTOMER_NODE_ID = rec.CUSTOMER_NODE_ID and DERIVED_ATTRIBUTE_ID = rec.DERIVED_ATTRIBUTE_ID and
		index1_value = rec.index1_value;
		deleted := SQL%rowcount;
		del_count := del_count + deleted;
        --DBMS_OUTPUT.Put_Line('x records deleted');

		iterator := iterator + deleted;
		if (iterator > block_size) then
		   commit;
           --DBMS_OUTPUT.Put_Line('commit executed');
		   iterator := 0;
		   EXIT WHEN sysdate >= end_date;
		end if;
	END LOOP;
	commit;
    --DBMS_OUTPUT.Put_Line('commit executed');
   EXCEPTION
     WHEN NO_DATA_FOUND THEN
       NULL;
     WHEN OTHERS THEN
       -- Consider logging the error and then re-raise
       RAISE;
END purge_customer_allocations;




PROCEDURE purge_subtotal_rating_delta(
	commit_count number, 
    del_count out number, 
    run_hours number
) IS

	/*
		Find out which normalised_event_ids are not present in the normalised_event table
		and therefore can be deleted from subtotal_rating_delta..
	*/
    CURSOR neIds IS
    SELECT
        s.normalised_event_id
    FROM
        subtotal_rating_delta s
    WHERE
        NOT EXISTS (
            SELECT 1
            FROM normalised_event ne
            WHERE ne.normalised_event_id = s.normalised_event_id
              AND ne.charge_start_date = s.charge_start_date
        )
        and normalised_event_id < ( select min(normalised_event_id)+2000000 from subtotal_rating_delta)
		and rownum < commit_count;

    v_neid subtotal_rating_delta.normalised_event_id%TYPE;

    end_date date;
BEGIN
    end_date := sysdate + (run_hours / 24);
    del_count := 0;

    BEGIN
		/* Loop through blocks (per commit_count) of rows  */
		LOOP

			/* Get NE ids... */
	        OPEN neIds;
            FETCH neIds INTO v_neid;
            /* no more records found? */
            EXIT WHEN neIds%NOTFOUND;

	        LOOP
	            /* Delete row(s) for NE id.. */
	            DELETE FROM subtotal_rating_delta  WHERE normalised_event_id = v_neid;

				/* Save count of deleted rows... */
	            del_count := del_count + SQL%rowcount;

                /* Exit is run_hours parameter has been reached.. */
                EXIT WHEN sysdate >= end_date;

				/* next neId.. */
	            FETCH neIds INTO v_neid;
	            EXIT WHEN neIds%NOTFOUND;
	        END LOOP;

	        COMMIT;

			/* Exit is run_hours parameter has been reached.. */
			EXIT WHEN sysdate >= end_date;

			/* Close cursor */
			CLOSE neIds;

		END LOOP;
    END;

    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            NULL;
        WHEN OTHERS THEN
            -- Consider logging the error and then re-raise
            RAISE;
END purge_subtotal_rating_delta;




PROCEDURE purge_subtotal_rating_value(
	in_subtotal_id number, 
    effective_date varchar2, 
    max_end_date_purge_days number, 
    commit_count number, 
    del_count out number, 
    task_end varchar2
) IS

	real_task_end date;

	-- Fetch subtotal entries for expired subtotals..
	CURSOR subtotal_rows IS
	select 
		s.subtotal_id, s.service_id, s.end_date 
	from 
		subtotal_rating_value s
	where 
		s.subtotal_id = in_subtotal_id
		and s.end_date < (to_date(effective_date, 'dd-mm-yyyy hh24:mi:ss') - max_end_date_purge_days)
	group by 
		s.subtotal_id, s.service_id, s.end_date;

    tmp_count number;
	a_subtotal_id number;
	a_service_id number;
	a_end_date date;

BEGIN
    del_count := 0;
    tmp_count := 0; 
    real_task_end := to_date(task_end, 'dd-mm-yyyy hh24:mi:ss');

	-- Check if max. task end date reached...
	if(sysdate >= real_task_end) then
		return;
	end if;

    BEGIN
		/* Get NE ids... */
        OPEN subtotal_rows;
        LOOP
			FETCH subtotal_rows INTO a_subtotal_id, a_service_id, a_end_date;
            EXIT WHEN subtotal_rows%NOTFOUND;

            /* Delete row(s) .. */
        	DELETE FROM subtotal_rating_value
			WHERE subtotal_id = a_subtotal_id
			AND service_id = a_service_id
			AND end_date = a_end_date;
			
			/* Save count of deleted rows... */
            tmp_count := tmp_count + SQL%rowcount;
            del_count := del_count + SQL%rowcount;
            
			/* Check if commit count has been exceeded.. */
            IF (tmp_count >= commit_count) THEN
                COMMIT;
                tmp_count := 0;
                /* Exit is run_hours parameter has been reached.. */
                EXIT WHEN sysdate >= real_task_end;
            END IF;

        END LOOP;
        COMMIT;
    END;

    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            NULL;
        WHEN OTHERS THEN
            -- Consider logging the error and then re-raise
            RAISE;
END purge_subtotal_rating_value;



end H3AT_CLEANUP;
/
exit;
CREATE OR REPLACE PACKAGE H3AT_ROAMINFO IS
TYPE H3AT_REF_CODE IS RECORD (
      id number,
      descr varchar2(100)
);

PROCEDURE GET_ROAMING_INFO(
    /* IN: */
    MSISDN IN VARCHAR,
    IMSI IN VARCHAR,
    VLRId IN VARCHAR, 
    MCCMNC IN VARCHAR,
    /* OUT: */
    IN_COUNTRY OUT VARCHAR,
    COUNTRY OUT VARCHAR,
    CALLHOME OUT NUMBER,
    CALLIN OUT NUMBER,
    SENDSMS OUT NUMBER,
    SENDMMS OUT NUMBER,
    MBMMS OUT NUMBER,
    RECEIVEMMS OUT NUMBER,
    DATAPRICE OUT NUMBER,
    TemplateID OUT VARCHAR,
    COUNTRY_CODE OUT VARCHAR,
    EMBASSY_INFO OUT VARCHAR2,
    /* D150805 - new output parameter SV COUNTRY_ID */
    COUNTRY_BILLING_ID OUT NUMBER,	
    /* error handling.. */
    status_code OUT NUMBER,
    error_code OUT VARCHAR,
    error_description OUT VARCHAR
	
);

PROCEDURE GET_ROAMING_INFO(
    /* IN: */
    MSISDN IN VARCHAR,
    IMSI IN VARCHAR,
    VLRId IN VARCHAR, 
    MCCMNC IN VARCHAR,
    /* OUT: */
    IN_COUNTRY OUT VARCHAR,
    COUNTRY OUT VARCHAR,
    CALLHOME OUT NUMBER,
    CALLIN OUT NUMBER,
    SENDSMS OUT NUMBER,
    SENDMMS OUT NUMBER,
    MBMMS OUT NUMBER,
    RECEIVEMMS OUT NUMBER,
    DATAPRICE OUT NUMBER,
    TemplateID OUT VARCHAR,
    COUNTRY_CODE OUT VARCHAR,
    EMBASSY_INFO OUT VARCHAR2,
    /* error handling.. */
    status_code OUT NUMBER,
    error_code OUT VARCHAR,
    error_description OUT VARCHAR
	
);

FUNCTION getNumberRegionPlan(
    MSISDN IN VARCHAR,
    IMSI IN VARCHAR
) return H3AT_REF_CODE;
    
FUNCTION getNetworkPrefixDAName(
    numberRegionPlan IN H3AT_REF_CODE
) RETURN VARCHAR;

FUNCTION getCountryForMCCMNC(
    MCCMNC IN VARCHAR,
    networkPrefixDAName IN VARCHAR
) RETURN H3AT_REF_CODE;

PROCEDURE lookupRoamingZones(
    VLRId IN VARCHAR,
    countryId IN H3AT_REF_CODE,
    numberRegionPlan IN H3AT_REF_CODE, /* IN parameters */
    COUNTRY OUT VARCHAR, 
    IN_COUNTRY OUT VARCHAR,
    roamingZoneVoice OUT H3AT_REF_CODE, 
    roamingZoneData OUT H3AT_REF_CODE, 
    threeLikeHomeInd OUT VARCHAR,
    COUNTRY_CODE OUT VARCHAR, /* OUT parameters */
    EMBASSY_INFO OUT VARCHAR2,
    COUNTRY_BILLING_ID OUT NUMBER
);

PROCEDURE lookupRoamingZones(
    VLRId IN VARCHAR,
    countryId IN H3AT_REF_CODE,
    numberRegionPlan IN H3AT_REF_CODE, /* IN parameters */
    COUNTRY OUT VARCHAR, 
    IN_COUNTRY OUT VARCHAR,
    roamingZoneVoice OUT H3AT_REF_CODE, 
    roamingZoneData OUT H3AT_REF_CODE, 
    threeLikeHomeInd OUT VARCHAR,
    COUNTRY_CODE OUT VARCHAR, /* OUT parameters */
    EMBASSY_INFO OUT VARCHAR2
);

PROCEDURE getRatesForVoiceZone(
    numberRegionPlan IN H3AT_REF_CODE,
    roamingZone IN H3AT_REF_CODE,
    threeLikeHomeInd IN VARCHAR, /* IN parameters */
    CALLHOME OUT NUMBER, 
    CALLIN OUT NUMBER, 
    SENDSMS OUT NUMBER, 
    SENDMMS OUT NUMBER, 
    MBMMS OUT NUMBER,
    RECEIVEMMS OUT NUMBER,
    TEMPLATEID OUT VARCHAR /* OUT parameters */
);

PROCEDURE getRatesForDataZone(
    numberRegionPlan IN H3AT_REF_CODE,
    roamingZone IN H3AT_REF_CODE,
    threeLikeHomeInd IN VARCHAR, /* IN parameters */
    DATAPRICE OUT NUMBER /* OUT parameters */
);

end H3AT_ROAMINFO;
/

CREATE OR REPLACE PACKAGE BODY H3AT_ROAMINFO IS
-- 
-- Introduced with "D130262 - Roaming Info 3".
-- 
-- Notes:
--   - sysdate is used when querying historized data
-- 
-- 

-- 
-- Main function to retrieve roaming SMS info from.
-- 
-- Returns 1 on success(also in status_code).
-- 
-- Error handling:
--   o status_code is set to 0.
--   o error_code is set as follows:
--       -20001 => error while getting number region plan for MSISDN/IMSI
--       -20002 => error while getting network prefix roaming DA for number region plan
--       -20003 => error while getting country for MCCMNC
--       -20004 => error while getting roaming zones for VLRId/country
--       -20005 => error while getting voice rate info
--       -20006 => error while getting data rate info
--   o error_description contains verbose information about the root error
-- 
PROCEDURE GET_ROAMING_INFO(
    /* IN: */
    MSISDN IN VARCHAR,
    IMSI IN VARCHAR,
    VLRId IN VARCHAR,
    MCCMNC IN VARCHAR,
    /* OUT: */
    IN_COUNTRY OUT VARCHAR,
    COUNTRY OUT VARCHAR,
    CALLHOME OUT NUMBER,
    CALLIN OUT NUMBER,
    SENDSMS OUT NUMBER,
    SENDMMS OUT NUMBER,
    MBMMS OUT NUMBER,
    RECEIVEMMS OUT NUMBER,
    DATAPRICE OUT NUMBER,
    TemplateID OUT VARCHAR,
    COUNTRY_CODE OUT VARCHAR,
    EMBASSY_INFO OUT VARCHAR2,
    COUNTRY_BILLING_ID OUT NUMBER,
    /* error handling.. */
    status_code OUT NUMBER,
    error_code OUT VARCHAR,
    error_description OUT VARCHAR
)
IS
BEGIN
DECLARE
    numberRegionPlan H3AT_REF_CODE;
    countryId H3AT_REF_CODE;
    roamingZoneVoice H3AT_REF_CODE;
    roamingZoneData H3AT_REF_CODE;
    threeLikeHomeInd VARCHAR(1);
    networkPrefixDAName VARCHAR(100);
    err_num number;
BEGIN

    /* Fetch number region plan for service.. */
    BEGIN
        numberRegionPlan := getNumberRegionPlan(MSISDN, IMSI);
    EXCEPTION
        /* handle all exceptions. */
        WHEN OTHERS THEN BEGIN
            err_num := SQLCODE;
            IF err_num = -20001 THEN
                numberRegionPlan := getNumberRegionPlan(MSISDN, null);
            END IF;
        END;
    END;

    /* lookup roaming zone based on VLRId.. */
    IF ((VLRId IS NOT NULL) AND (VLRId <> '0')) THEN
        lookupRoamingZones(
            VLRId, null, numberRegionPlan, /* IN parameters */
            COUNTRY, IN_COUNTRY, roamingZoneVoice, roamingZoneData, threeLikeHomeInd, COUNTRY_CODE, EMBASSY_INFO, COUNTRY_BILLING_ID  /* OUT parameters */
        );
    ELSE
    /* lookup roaming zone based on MCCMNC.. */
        networkPrefixDAName := getNetworkPrefixDAName(numberRegionPlan);
        countryId := getCountryForMCCMNC(MCCMNC, networkPrefixDAName);

        lookupRoamingZones(
            null, countryId, numberRegionPlan, /* IN parameters */
            COUNTRY, IN_COUNTRY, roamingZoneVoice, roamingZoneData, threeLikeHomeInd, COUNTRY_CODE, EMBASSY_INFO, COUNTRY_BILLING_ID /* OUT parameters */
        );
    END IF;


    /* Get voice/sms rates.. */
    getRatesForVoiceZone(
        numberRegionPlan, roamingZoneVoice, threeLikeHomeInd, /* IN parameters */
        CALLHOME, CALLIN, SENDSMS, SENDMMS, MBMMS, RECEIVEMMS, TemplateID /* OUT parameters */
    );

    /* Get data rates.. */
    getRatesForDataZone(
        numberRegionPlan, roamingZoneData, threeLikeHomeInd, /* IN parameters */
        DATAPRICE /* OUT parameters */
    );

    /* Everything cool, set RETURN values.. */
    status_code := 1;

EXCEPTION
    /* handle all exceptions. */
    WHEN OTHERS THEN BEGIN
        status_code := 0;
        error_code := SQLCODE;
        error_description := SQLERRM;
    END;
END;
END GET_ROAMING_INFO;

PROCEDURE GET_ROAMING_INFO(
    /* IN: */
    MSISDN IN VARCHAR,
    IMSI IN VARCHAR,
    VLRId IN VARCHAR,
    MCCMNC IN VARCHAR,
    /* OUT: */
    IN_COUNTRY OUT VARCHAR,
    COUNTRY OUT VARCHAR,
    CALLHOME OUT NUMBER,
    CALLIN OUT NUMBER,
    SENDSMS OUT NUMBER,
    SENDMMS OUT NUMBER,
    MBMMS OUT NUMBER,
    RECEIVEMMS OUT NUMBER,
    DATAPRICE OUT NUMBER,
    TemplateID OUT VARCHAR,
    COUNTRY_CODE OUT VARCHAR,
    EMBASSY_INFO OUT VARCHAR2,
    /* error handling.. */
    status_code OUT NUMBER,
    error_code OUT VARCHAR,
    error_description OUT VARCHAR
)
IS
BEGIN
DECLARE
    numberRegionPlan H3AT_REF_CODE;
    countryId H3AT_REF_CODE;
    roamingZoneVoice H3AT_REF_CODE;
    roamingZoneData H3AT_REF_CODE;
    threeLikeHomeInd VARCHAR(1);
    networkPrefixDAName VARCHAR(100);
    err_num number;
BEGIN

    /* Fetch number region plan for service.. */
    BEGIN
        numberRegionPlan := getNumberRegionPlan(MSISDN, IMSI);
    EXCEPTION
        /* handle all exceptions. */
        WHEN OTHERS THEN BEGIN
            err_num := SQLCODE;
            IF err_num = -20001 THEN
                numberRegionPlan := getNumberRegionPlan(MSISDN, null);
            END IF;
        END;
    END;

    /* lookup roaming zone based on VLRId.. */
    IF ((VLRId IS NOT NULL) AND (VLRId <> '0')) THEN
        lookupRoamingZones(
            VLRId, null, numberRegionPlan, /* IN parameters */
            COUNTRY, IN_COUNTRY, roamingZoneVoice, roamingZoneData, threeLikeHomeInd, COUNTRY_CODE, EMBASSY_INFO  /* OUT parameters */
        );
    ELSE
    /* lookup roaming zone based on MCCMNC.. */
        networkPrefixDAName := getNetworkPrefixDAName(numberRegionPlan);
        countryId := getCountryForMCCMNC(MCCMNC, networkPrefixDAName);

        lookupRoamingZones(
            null, countryId, numberRegionPlan, /* IN parameters */
            COUNTRY, IN_COUNTRY, roamingZoneVoice, roamingZoneData, threeLikeHomeInd, COUNTRY_CODE, EMBASSY_INFO /* OUT parameters */
        );
    END IF;


    /* Get voice/sms rates.. */
    getRatesForVoiceZone(
        numberRegionPlan, roamingZoneVoice, threeLikeHomeInd, /* IN parameters */
        CALLHOME, CALLIN, SENDSMS, SENDMMS, MBMMS, RECEIVEMMS, TemplateID /* OUT parameters */
    );

    /* Get data rates.. */
    getRatesForDataZone(
        numberRegionPlan, roamingZoneData, threeLikeHomeInd, /* IN parameters */
        DATAPRICE /* OUT parameters */
    );

    /* Everything cool, set RETURN values.. */
    status_code := 1;

EXCEPTION
    /* handle all exceptions. */
    WHEN OTHERS THEN BEGIN
        status_code := 0;
        error_code := SQLCODE;
        error_description := SQLERRM;
    END;
END;
END GET_ROAMING_INFO;


-- 
-- Returns the number region plan for a service from the INDUSTRY_GENERAL_7 field of SERVICE_HISTORY.
-- Defaults to 1 if no number region plan is configured on the service.
-- IMSI is used in favor of MSISDN!
-- 
function getNumberRegionPlan(
    MSISDN IN VARCHAR,
    IMSI IN VARCHAR
) return H3AT_REF_CODE
IS
BEGIN
DECLARE
    serviceWhereClause VARCHAR(200);
    MsisdnOrImsi VARCHAR(200);
    numberRegionPlan H3AT_REF_CODE;
BEGIN
    /* Build where clause to use.. */
    IF IMSI IS NOT NULL THEN
        serviceWhereClause := ':1 is not null and s.network_name = :1';
        MsisdnOrImsi := IMSI;
    ELSE
        serviceWhereClause := ':1 is not null and s.service_name = :1';
        MsisdnOrImsi := MSISDN;
    END IF;

    /* Get service's number region plan id */
    EXECUTE IMMEDIATE
        'SELECT nvl(s.industry_general_7, 1), rc.abbreviation ' ||
        'FROM service_history s, reference_code rc ' ||
        'WHERE 1=1 ' ||
        'AND ' || serviceWhereClause || ' ' ||
        'AND s.service_status_code = 3 ' ||
        'AND sysdate BETWEEN s.effective_start_date AND s.effective_end_date ' ||
        'AND rc.reference_type_id = 26000410 /* H3AT_NUMBER_REGION_PLAN */ ' ||
        'AND rc.reference_code = nvl(s.industry_general_7, 1)'
    INTO numberRegionPlan.ID, numberRegionPlan.DESCR
    USING MsisdnOrImsi, MsisdnOrImsi
    ;

    RETURN numberRegionPlan;

EXCEPTION
    WHEN NO_DATA_FOUND THEN BEGIN
        RAISE_APPLICATION_ERROR(-20001,
                'Could not find service with MSISDN >' || MSISDN || '< and/or IMSI>' || IMSI ||'<.');
    END;
    WHEN OTHERS THEN BEGIN
        RAISE_APPLICATION_ERROR(-20010, 'Other error in getNumberRegionPlan ' ||
                'for service with MSISDN >' || MSISDN || '< and/or IMSI >' || IMSI ||'<. Original error:' || SQLCODE || ',' || SQLERRM);
    END;
END;
END getNumberRegionPlan;




-- 
-- Returns the Network Prefix Roaming DA for a number region plan.
-- dH3AT_NE_NumberRegionMap is queried.
-- 
FUNCTION getNetworkPrefixDAName(
    numberRegionPlan IN H3AT_REF_CODE
) RETURN VARCHAR
IS
BEGIN
DECLARE
    networkPrefixDAName VARCHAR(200);
BEGIN
    /* Get network prefix DA for number region plan id */
    SELECT
        result4_value
    INTO
        networkPrefixDAName
    FROM
        derived_attribute_array
    WHERE
        derived_attribute_id = 27000504 /* dH3AT_NE_NumberRegionMap$ */
        AND index1_value = numberRegionPlan.ID /* NUMBER_REGION_PLAN */
    ;

    RETURN networkPrefixDAName;

EXCEPTION
    WHEN NO_DATA_FOUND THEN BEGIN
        RAISE_APPLICATION_ERROR(-20002,
                'Could not find network prefix roaming DA for numberRegionPlan >' || numberRegionPlan.DESCR || '<.');
    END;
    WHEN OTHERS THEN BEGIN
        RAISE_APPLICATION_ERROR(-20010, 'Other error in getNetworkPrefixDAName ' ||
                'for numberRegionPlan >' || numberRegionPlan.DESCR || '<. Original error:' || SQLCODE || ',' || SQLERRM);
    END;
END;
END getNetworkPrefixDAName;



-- 
-- Returns the id for a country from a network prefix DA based on the given MCCMNC code.
-- A best match lookup is performed.
-- 
FUNCTION getCountryForMCCMNC(
    MCCMNC IN VARCHAR,
    networkPrefixDAName IN VARCHAR
) RETURN H3AT_REF_CODE
IS
BEGIN
DECLARE
    countryId H3AT_REF_CODE;
BEGIN
    SELECT
        result2_value, abbreviation
    INTO
        countryId.ID, countryId.DESCR
    FROM (
        SELECT
            daa.result2_value, rc.abbreviation
        FROM
            derived_attribute_array daa, derived_attribute_history dah, reference_code rc
        WHERE
            daa.derived_attribute_id = dah.derived_attribute_id
            AND sysdate BETWEEN daa.effective_start_date AND daa.effective_end_date
            AND sysdate BETWEEN dah.effective_start_date AND dah.effective_end_date
            AND dah.display_table_name = networkPrefixDAName /* e.g. dH3G_NE_NetworkPrefix_Roaming*/
            AND MCCMNC LIKE concat(daa.index1_value, '%')
            AND daa.result2_value = to_char(rc.reference_code)
            AND rc.reference_type_id = 4100044 /* H3G_NE_COUNTRY */
        ORDER BY result2_value DESC
    )
    WHERE ROWNUM = 1
    ;

    RETURN countryId;

EXCEPTION
    WHEN NO_DATA_FOUND THEN BEGIN
        RAISE_APPLICATION_ERROR(-20003,
                'Could not find country info for MCCMNC >' || MCCMNC || '< in DA >' || networkPrefixDAName || '<.');
    END;
    WHEN OTHERS THEN BEGIN
        RAISE_APPLICATION_ERROR(-20010, 'Other error in getCountryForMCCMNC ' ||
                'for MCCMNC >' || MCCMNC || '< in DA >' || networkPrefixDAName || '<. Original error:' || SQLCODE || ',' || SQLERRM);
    END;
END;
END getCountryForMCCMNC;


-- 
-- Provides textual country information, roaming zones and the 3LikeHome indicator for the given VLRId, countryId and number region plan.
-- The DA "dH3AT_CountryRoamingInfo" is queried.
-- VLRId - if populated - is used in favor of countryId. In this case a best match lookup on the VLRId is done to find eligible rows.
-- If countryId is used (=> VLRId is not populated) to lookup the DA, an exact match lookup is performed.
-- 
-- 
PROCEDURE lookupRoamingZones(
    VLRId IN VARCHAR,
    countryId IN H3AT_REF_CODE,
    numberRegionPlan IN H3AT_REF_CODE, /* IN parameters */
    COUNTRY OUT VARCHAR,
    IN_COUNTRY OUT VARCHAR,
    roamingZoneVoice OUT H3AT_REF_CODE,
    roamingZoneData OUT H3AT_REF_CODE,
    threeLikeHomeInd OUT VARCHAR, 
    COUNTRY_CODE OUT VARCHAR, /* OUT parameters */
    COUNTRY_BILLING_ID OUT NUMBER
)
IS
BEGIN
BEGIN
    if VLRId IS NOT NULL THEN
        SELECT
            index1_value,
			index2_value,
            result1_value, result2_value,
            result3_value, "rzv.abbreviation",
            result4_value, "rzd.abbreviation",
            result5_value
        INTO
            COUNTRY_CODE,
     	    COUNTRY_BILLING_ID,
            COUNTRY, IN_COUNTRY,
            roamingZoneVoice.ID, roamingZoneVoice.DESCR,
            roamingZoneData.ID, roamingZoneData.DESCR,
            threeLikeHomeInd
        FROM (
            SELECT
                daa.index1_value,
				daa.index2_value,
                daa.result1_value, daa.result2_value,
                daa.result3_value, rzv.abbreviation "rzv.abbreviation",
                daa.result4_value, rzd.abbreviation "rzd.abbreviation",
                daa.result5_value
            FROM
                derived_attribute_array daa, reference_code rzv, reference_code rzd
            WHERE
                daa.derived_attribute_id = 27000880 /* dH3AT_CountryRoamingInfo */
                AND sysdate BETWEEN daa.effective_start_date AND daa.effective_end_date
                AND VLRId LIKE concat(daa.index1_value, '%')
                AND daa.index3_value = numberRegionPlan.ID
                AND daa.result3_value = rzv.reference_code
                AND daa.result4_value = rzd.reference_code
                AND rzv.reference_type_id = 8000047 /* H3G_ROAM_ZONE */
                AND rzd.reference_type_id = 8000047 /* H3G_ROAM_ZONE */
            ORDER BY daa.index1_value desc
        )
        WHERE ROWNUM = 1;
    ELSE
        SELECT
            daa.index1_value,
			daa.index2_value,
            daa.result1_value, daa.result2_value,
            daa.result3_value, rzv.abbreviation,
            daa.result4_value, rzd.abbreviation,
            daa.result5_value
        INTO
            COUNTRY_CODE,
			COUNTRY_BILLING_ID,
            COUNTRY, IN_COUNTRY,
            roamingZoneVoice.ID, roamingZoneVoice.DESCR,
            roamingZoneData.ID, roamingZoneData.DESCR,
            threeLikeHomeInd
        FROM
            derived_attribute_array daa, reference_code rzv, reference_code rzd
        WHERE
            daa.derived_attribute_id = 27000880 /* dH3AT_CountryRoamingInfo */
            AND sysdate BETWEEN daa.effective_start_date AND daa.effective_end_date
            AND daa.index2_value = countryId.ID
            AND daa.index3_value = numberRegionPlan.ID
            AND daa.result3_value = rzv.reference_code
            AND daa.result4_value = rzd.reference_code
            AND rzv.reference_type_id = 8000047 /* H3G_ROAM_ZONE */
            AND rzd.reference_type_id = 8000047 /* H3G_ROAM_ZONE */
            AND rownum = 1
        ;
    END IF;

EXCEPTION
    WHEN NO_DATA_FOUND THEN BEGIN
        RAISE_APPLICATION_ERROR(-20004,
                'Could not find roaming zones for VLRId >' || VLRId || '< or Country >' || countryId.DESCR || '< ' ||
                'and number region plan >' || numberRegionPlan.DESCR || '<.');
    END;
    WHEN OTHERS THEN BEGIN
        RAISE_APPLICATION_ERROR(-20010, 'Other error in lookupRoamingZones ' ||
                'for VLRId >' || VLRId || '< or Country >' || countryId.DESCR || '< (' || countryId.ID || ')' ||
                'and number region plan >' || numberRegionPlan.DESCR || '<. Original error:' || SQLCODE || ',' || SQLERRM);
    END;
END;
END lookupRoamingZones;

PROCEDURE lookupRoamingZones(
    VLRId IN VARCHAR,
    countryId IN H3AT_REF_CODE,
    numberRegionPlan IN H3AT_REF_CODE, /* IN parameters */
    COUNTRY OUT VARCHAR,
    IN_COUNTRY OUT VARCHAR,
    roamingZoneVoice OUT H3AT_REF_CODE,
    roamingZoneData OUT H3AT_REF_CODE,
    threeLikeHomeInd OUT VARCHAR, 
    COUNTRY_CODE OUT VARCHAR /* OUT parameters */
)
IS
BEGIN
BEGIN
    if VLRId IS NOT NULL THEN
        SELECT
            index1_value,
            result1_value, result2_value,
            result3_value, "rzv.abbreviation",
            result4_value, "rzd.abbreviation",
            result5_value
        INTO
            COUNTRY_CODE,
            COUNTRY, IN_COUNTRY,
            roamingZoneVoice.ID, roamingZoneVoice.DESCR,
            roamingZoneData.ID, roamingZoneData.DESCR,
            threeLikeHomeInd
        FROM (
            SELECT
                daa.index1_value,
                daa.result1_value, daa.result2_value,
                daa.result3_value, rzv.abbreviation "rzv.abbreviation",
                daa.result4_value, rzd.abbreviation "rzd.abbreviation",
                daa.result5_value
            FROM
                derived_attribute_array daa, reference_code rzv, reference_code rzd
            WHERE
                daa.derived_attribute_id = 27000880 /* dH3AT_CountryRoamingInfo */
                AND sysdate BETWEEN daa.effective_start_date AND daa.effective_end_date
                AND VLRId LIKE concat(daa.index1_value, '%')
                AND daa.index3_value = numberRegionPlan.ID
                AND daa.result3_value = rzv.reference_code
                AND daa.result4_value = rzd.reference_code
                AND rzv.reference_type_id = 8000047 /* H3G_ROAM_ZONE */
                AND rzd.reference_type_id = 8000047 /* H3G_ROAM_ZONE */
            ORDER BY daa.index1_value desc
        )
        WHERE ROWNUM = 1;
    ELSE
        SELECT
            daa.index1_value,
            daa.result1_value, daa.result2_value,
            daa.result3_value, rzv.abbreviation,
            daa.result4_value, rzd.abbreviation,
            daa.result5_value
        INTO
            COUNTRY_CODE,
            COUNTRY, IN_COUNTRY,
            roamingZoneVoice.ID, roamingZoneVoice.DESCR,
            roamingZoneData.ID, roamingZoneData.DESCR,
            threeLikeHomeInd
        FROM
            derived_attribute_array daa, reference_code rzv, reference_code rzd
        WHERE
            daa.derived_attribute_id = 27000880 /* dH3AT_CountryRoamingInfo */
            AND sysdate BETWEEN daa.effective_start_date AND daa.effective_end_date
            AND daa.index2_value = countryId.ID
            AND daa.index3_value = numberRegionPlan.ID
            AND daa.result3_value = rzv.reference_code
            AND daa.result4_value = rzd.reference_code
            AND rzv.reference_type_id = 8000047 /* H3G_ROAM_ZONE */
            AND rzd.reference_type_id = 8000047 /* H3G_ROAM_ZONE */
            AND rownum = 1
        ;
    END IF;

EXCEPTION
    WHEN NO_DATA_FOUND THEN BEGIN
        RAISE_APPLICATION_ERROR(-20004,
                'Could not find roaming zones for VLRId >' || VLRId || '< or Country >' || countryId.DESCR || '< ' ||
                'and number region plan >' || numberRegionPlan.DESCR || '<.');
    END;
    WHEN OTHERS THEN BEGIN
        RAISE_APPLICATION_ERROR(-20010, 'Other error in lookupRoamingZones ' ||
                'for VLRId >' || VLRId || '< or Country >' || countryId.DESCR || '< (' || countryId.ID || ')' ||
                'and number region plan >' || numberRegionPlan.DESCR || '<. Original error:' || SQLCODE || ',' || SQLERRM);
    END;
END;
END lookupRoamingZones;


-- 
-- Provides textual country information, roaming zones and the 3LikeHome indicator for the given VLRId, countryId and number region plan.
-- The DA "dH3AT_CountryRoamingInfo" is queried.
-- VLRId - if populated - is used in favor of countryId. In this case a best match lookup on the VLRId is done to find eligible rows.
-- If countryId is used (=> VLRId is not populated) to lookup the DA, an exact match lookup is performed.
-- 
-- 
PROCEDURE lookupRoamingZones(
    VLRId IN VARCHAR,
    countryId IN H3AT_REF_CODE,
    numberRegionPlan IN H3AT_REF_CODE, /* IN parameters */
    COUNTRY OUT VARCHAR,
    IN_COUNTRY OUT VARCHAR,
    roamingZoneVoice OUT H3AT_REF_CODE,
    roamingZoneData OUT H3AT_REF_CODE,
    threeLikeHomeInd OUT VARCHAR, 
    COUNTRY_CODE OUT VARCHAR, /* OUT parameters */
    EMBASSY_INFO OUT VARCHAR2,
    COUNTRY_BILLING_ID OUT NUMBER
)
IS
BEGIN
BEGIN
    if VLRId IS NOT NULL THEN
        SELECT
            index1_value,
			index2_value,
            result1_value, result2_value,
            result3_value, "rzv.abbreviation",
            result4_value, "rzd.abbreviation",
            result5_value, 
         result6_value
        INTO
            COUNTRY_CODE,
            COUNTRY_BILLING_ID,
            COUNTRY, IN_COUNTRY,
            roamingZoneVoice.ID, roamingZoneVoice.DESCR,
            roamingZoneData.ID, roamingZoneData.DESCR,
            threeLikeHomeInd,
         EMBASSY_INFO
        FROM (
            SELECT
                daa.index1_value,
				daa.index2_value,
                daa.result1_value, daa.result2_value,
                daa.result3_value, rzv.abbreviation "rzv.abbreviation",
                daa.result4_value, rzd.abbreviation "rzd.abbreviation",
                daa.result5_value,
				daa.result6_value
            FROM
                derived_attribute_array daa, reference_code rzv, reference_code rzd
            WHERE
                daa.derived_attribute_id = 27000880 /* dH3AT_CountryRoamingInfo */
                AND sysdate BETWEEN daa.effective_start_date AND daa.effective_end_date
                AND VLRId LIKE concat(daa.index1_value, '%')
                AND daa.index3_value = numberRegionPlan.ID
                AND daa.result3_value = rzv.reference_code
                AND daa.result4_value = rzd.reference_code
                AND rzv.reference_type_id = 8000047 /* H3G_ROAM_ZONE */
                AND rzd.reference_type_id = 8000047 /* H3G_ROAM_ZONE */
            ORDER BY daa.index1_value desc
        )
        WHERE ROWNUM = 1;
    ELSE
        SELECT
            daa.index1_value,
			daa.index2_value,
            daa.result1_value, daa.result2_value,
            daa.result3_value, rzv.abbreviation,
            daa.result4_value, rzd.abbreviation,
            daa.result5_value,
         daa.result6_value
        INTO
            COUNTRY_CODE,
			COUNTRY_BILLING_ID,
            COUNTRY, IN_COUNTRY,
            roamingZoneVoice.ID, roamingZoneVoice.DESCR,
            roamingZoneData.ID, roamingZoneData.DESCR,
            threeLikeHomeInd,
         EMBASSY_INFO
        FROM
            derived_attribute_array daa, reference_code rzv, reference_code rzd
        WHERE
            daa.derived_attribute_id = 27000880 /* dH3AT_CountryRoamingInfo */
            AND sysdate BETWEEN daa.effective_start_date AND daa.effective_end_date
            AND daa.index2_value = countryId.ID
            AND daa.index3_value = numberRegionPlan.ID
            AND daa.result3_value = rzv.reference_code
            AND daa.result4_value = rzd.reference_code
            AND rzv.reference_type_id = 8000047 /* H3G_ROAM_ZONE */
            AND rzd.reference_type_id = 8000047 /* H3G_ROAM_ZONE */
            AND rownum = 1
        ;
    END IF;

EXCEPTION
    WHEN NO_DATA_FOUND THEN BEGIN
        RAISE_APPLICATION_ERROR(-20004,
                'Could not find roaming zones for VLRId >' || VLRId || '< or Country >' || countryId.DESCR || '< ' ||
                'and number region plan >' || numberRegionPlan.DESCR || '<.');
    END;
    WHEN OTHERS THEN BEGIN
        RAISE_APPLICATION_ERROR(-20010, 'Other error in lookupRoamingZones ' ||
                'for VLRId >' || VLRId || '< or Country >' || countryId.DESCR || '< (' || countryId.ID || ')' ||
                'and number region plan >' || numberRegionPlan.DESCR || '<. Original error:' || SQLCODE || ',' || SQLERRM);
    END;
END;
END lookupRoamingZones;

PROCEDURE lookupRoamingZones(
    VLRId IN VARCHAR,
    countryId IN H3AT_REF_CODE,
    numberRegionPlan IN H3AT_REF_CODE, /* IN parameters */
    COUNTRY OUT VARCHAR,
    IN_COUNTRY OUT VARCHAR,
    roamingZoneVoice OUT H3AT_REF_CODE,
    roamingZoneData OUT H3AT_REF_CODE,
    threeLikeHomeInd OUT VARCHAR, 
    COUNTRY_CODE OUT VARCHAR, /* OUT parameters */
    EMBASSY_INFO OUT VARCHAR2
)
IS
BEGIN
BEGIN
    if VLRId IS NOT NULL THEN
        SELECT
            index1_value,
            result1_value, result2_value,
            result3_value, "rzv.abbreviation",
            result4_value, "rzd.abbreviation",
            result5_value, 
         result6_value
        INTO
            COUNTRY_CODE,
            COUNTRY, IN_COUNTRY,
            roamingZoneVoice.ID, roamingZoneVoice.DESCR,
            roamingZoneData.ID, roamingZoneData.DESCR,
            threeLikeHomeInd,
         EMBASSY_INFO
        FROM (
            SELECT
                daa.index1_value,
                daa.result1_value, daa.result2_value,
                daa.result3_value, rzv.abbreviation "rzv.abbreviation",
                daa.result4_value, rzd.abbreviation "rzd.abbreviation",
                daa.result5_value,
				daa.result6_value
            FROM
                derived_attribute_array daa, reference_code rzv, reference_code rzd
            WHERE
                daa.derived_attribute_id = 27000880 /* dH3AT_CountryRoamingInfo */
                AND sysdate BETWEEN daa.effective_start_date AND daa.effective_end_date
                AND VLRId LIKE concat(daa.index1_value, '%')
                AND daa.index3_value = numberRegionPlan.ID
                AND daa.result3_value = rzv.reference_code
                AND daa.result4_value = rzd.reference_code
                AND rzv.reference_type_id = 8000047 /* H3G_ROAM_ZONE */
                AND rzd.reference_type_id = 8000047 /* H3G_ROAM_ZONE */
            ORDER BY daa.index1_value desc
        )
        WHERE ROWNUM = 1;
    ELSE
        SELECT
            daa.index1_value,
            daa.result1_value, daa.result2_value,
            daa.result3_value, rzv.abbreviation,
            daa.result4_value, rzd.abbreviation,
            daa.result5_value,
         daa.result6_value
        INTO
            COUNTRY_CODE,
            COUNTRY, IN_COUNTRY,
            roamingZoneVoice.ID, roamingZoneVoice.DESCR,
            roamingZoneData.ID, roamingZoneData.DESCR,
            threeLikeHomeInd,
         EMBASSY_INFO
        FROM
            derived_attribute_array daa, reference_code rzv, reference_code rzd
        WHERE
            daa.derived_attribute_id = 27000880 /* dH3AT_CountryRoamingInfo */
            AND sysdate BETWEEN daa.effective_start_date AND daa.effective_end_date
            AND daa.index2_value = countryId.ID
            AND daa.index3_value = numberRegionPlan.ID
            AND daa.result3_value = rzv.reference_code
            AND daa.result4_value = rzd.reference_code
            AND rzv.reference_type_id = 8000047 /* H3G_ROAM_ZONE */
            AND rzd.reference_type_id = 8000047 /* H3G_ROAM_ZONE */
            AND rownum = 1
        ;
    END IF;

EXCEPTION
    WHEN NO_DATA_FOUND THEN BEGIN
        RAISE_APPLICATION_ERROR(-20004,
                'Could not find roaming zones for VLRId >' || VLRId || '< or Country >' || countryId.DESCR || '< ' ||
                'and number region plan >' || numberRegionPlan.DESCR || '<.');
    END;
    WHEN OTHERS THEN BEGIN
        RAISE_APPLICATION_ERROR(-20010, 'Other error in lookupRoamingZones ' ||
                'for VLRId >' || VLRId || '< or Country >' || countryId.DESCR || '< (' || countryId.ID || ')' ||
                'and number region plan >' || numberRegionPlan.DESCR || '<. Original error:' || SQLCODE || ',' || SQLERRM);
    END;
END;
END lookupRoamingZones;


-- 
-- Returns non-data rate information and the HANF template id for the roaming SMS to send out.
-- 
PROCEDURE getRatesForVoiceZone(
    numberRegionPlan IN H3AT_REF_CODE,
    roamingZone IN H3AT_REF_CODE,
    threeLikeHomeInd IN VARCHAR, /* IN parameters */
    CALLHOME OUT NUMBER,
    CALLIN OUT NUMBER,
    SENDSMS OUT NUMBER,
    SENDMMS OUT NUMBER,
    MBMMS OUT NUMBER,
    RECEIVEMMS OUT NUMBER,
    TEMPLATEID OUT VARCHAR /* OUT parameters */
)
IS
BEGIN
BEGIN
    SELECT
        to_number(daa.result1_value, '9999999D99999', 'NLS_NUMERIC_CHARACTERS = ''.,'''), 
        to_number(daa.result2_value, '9999999D99999', 'NLS_NUMERIC_CHARACTERS = ''.,'''), 
        to_number(daa.result3_value, '9999999D99999', 'NLS_NUMERIC_CHARACTERS = ''.,'''),
        to_number(daa.result4_value, '9999999D99999', 'NLS_NUMERIC_CHARACTERS = ''.,'''),
        to_number(daa.result5_value, '9999999D99999', 'NLS_NUMERIC_CHARACTERS = ''.,'''), 
        to_number(daa.result6_value, '9999999D99999', 'NLS_NUMERIC_CHARACTERS = ''.,'''),
        hanf_templates.code_label
    INTO
        CALLHOME, CALLIN, SENDSMS, SENDMMS, MBMMS, RECEIVEMMS, TEMPLATEID
    FROM
        derived_attribute_array daa, reference_code hanf_templates
    WHERE
        daa.derived_attribute_id = 27000881 /* dH3AT_RoamingInfo */
        AND sysdate BETWEEN daa.effective_start_date AND daa.effective_end_date
        AND daa.index1_value = numberRegionPlan.ID
        AND daa.index2_value = roamingZone.ID
        AND daa.index3_value = threeLikeHomeInd
        AND daa.result8_value = hanf_templates.reference_code
        AND hanf_templates.reference_type_id = 26000416 /* H3AT_HANF_CONTROL_TEMPLATE */
    ;

EXCEPTION
    WHEN NO_DATA_FOUND THEN BEGIN
        RAISE_APPLICATION_ERROR(-20005,
                'Could not find voice rate info for roamingZone >' || roamingZone.DESCR || '<, number region plan >' || numberRegionPlan.DESCR || '< ' ||
                'and 3LikeHome indicator ' || threeLikeHomeInd || '.');
    END;
    WHEN OTHERS THEN BEGIN
        RAISE_APPLICATION_ERROR(-20010, 'Other error in getRatesForVoiceZone ' ||
                'for roamingZone >' || roamingZone.DESCR || '<, number region plan >' || numberRegionPlan.DESCR || '< ' ||
                'and 3LikeHome indicator ' || threeLikeHomeInd || '. Original error:' || SQLCODE || ',' || SQLERRM);
    END;
END;
END getRatesForVoiceZone;


-- 
-- Returns data rate information for the roaming SMS to send out.
-- 
PROCEDURE getRatesForDataZone(
    numberRegionPlan IN H3AT_REF_CODE,
    roamingZone IN H3AT_REF_CODE,
    threeLikeHomeInd IN VARCHAR, /* IN parameters */
    DATAPRICE OUT NUMBER /* OUT parameters */
)
IS
BEGIN
BEGIN
    SELECT
        to_number(daa.result7_value, '9999999D99999', 'NLS_NUMERIC_CHARACTERS = ''.,''')
    INTO
        DATAPRICE
    FROM
        derived_attribute_array daa
    WHERE
        daa.derived_attribute_id = 27000881 /* dH3AT_RoamingInfo */
        AND sysdate BETWEEN daa.effective_start_date AND daa.effective_end_date
        AND index1_value = numberRegionPlan.ID
        AND index2_value = roamingZone.ID
        AND index3_value = threeLikeHomeInd
    ;

EXCEPTION
    WHEN NO_DATA_FOUND THEN BEGIN
        RAISE_APPLICATION_ERROR(-20005,
                'Could not find data rate info for roamingZone >' || roamingZone.DESCR || '<, number region plan >' || numberRegionPlan.DESCR || '< ' ||
                'and 3LikeHome indicator ' || threeLikeHomeInd || '.');
    END;
    WHEN OTHERS THEN BEGIN
        RAISE_APPLICATION_ERROR(-20010, 'Other error in getRatesForDataZone ' ||
                'for roamingZone >' || roamingZone.DESCR || '<, number region plan >' || numberRegionPlan.DESCR || '< ' ||
                'and 3LikeHome indicator ' || threeLikeHomeInd || '. Original error:' || SQLCODE || ',' || SQLERRM);
    END;
END;
END getRatesForDataZone;




end H3AT_ROAMINFO;
/


grant execute on H3AT_ROAMINFO to TIBCO_RO;
--
--
--       File:           H3AT_REGTEST.sql
--       Created:        2016-06-29
--       Creator:        dieterwo0
--
-- DESCRIPTION:
--
--       Provides utility functions used for regression test execution.
--
-- REVISION HISTORY:
--
--   $Log: atai_H3AT_PLSQL.sql,v $
--   Revision 1.15  2018/09/13 08:49:01  schausge
--   Ticket CF-3084-1809: Performance enhancement for deletion of service_da_array table
--
--   Revision 1.3  2016/07/04 13:09:16  dieterwo
--   Ticket 163019: dbsync errors during installation
--
--   Revision 1.2  2016/06/29 11:46:36  dieterwo
--   Ticket 91544841606: grants missing..
--
--   Revision 1.1  2016/06/29 11:43:42  dieterwo
--   Ticket 91544841606: PLSQL utilities for regression test execution added
--
--
CREATE OR REPLACE PACKAGE H3AT_REGTEST IS
type string_array is table of varchar2(32767);

function splitString(
    str in varchar2, 
    delimiter in char default ','
) return string_array;

function addPrepaidReactivateAddons(
    productsStr in varchar2
) return varchar2;

end H3AT_REGTEST;
/


CREATE OR REPLACE PACKAGE BODY H3AT_REGTEST IS
-- 
-- splits a string into an array
-- 
function splitString(
    str in varchar2, 
    delimiter in char default ','
) return string_array is
    return_value         string_array := string_array();
    split_str            long default str || delimiter;
    i                    number;
begin
    loop
        i := instr(split_str, delimiter);
        exit when nvl(i,0) = 0;
        return_value.extend;
        return_value(return_value.count) := trim(substr(split_str, 1, i-1));
        split_str := substr(split_str, i + length(delimiter));
    end loop;
    return return_value;
end splitString;



-- 
-- takes a comma separated product list and enriches it with products specified as "FU products" in  "dH3AT_PrepaidReactivateAddOn"
-- 
function addPrepaidReactivateAddons(
    productsStr in varchar2
) return varchar2 is
    begin
    declare
        v_split_string string_array;
	FUProdId number;
	addProductsStr varchar2(2000);
    begin
        v_split_string := H3AT_REGTEST.splitString(productsStr);
        if v_split_string.count > 0 then
         for i in 1..v_split_string.count loop
	     if i > 1 then
		addProductsStr := addProductsStr || ',';
             end if;
             addProductsStr := addProductsStr || v_split_string(i);
   
             BEGIN
                 select to_number(daa.result1_value)
                 into FUProdId
                 from derived_attribute_array daa
                 where daa.derived_attribute_id = 26001266
                 and daa.index1_value = v_split_string(i)
		 and sysdate between daa.effective_start_date and daa.effective_end_date
                 ;

            EXCEPTION
                WHEN NO_DATA_FOUND THEN
                    FUProdId := -1;
            END;

	    if FUProdId > -1 then
	        addProductsStr := addProductsStr || ',' || FUProdId;
	    end if;
             

         end loop;
        end if;
	return addProductsStr;
    end;
end addPrepaidReactivateAddons;

end H3AT_REGTEST;
/


GRANT EXECUTE ON H3AT_REGTEST TO ATA_MANAGER;
GRANT EXECUTE ON H3AT_REGTEST TO ATA_CLIENT;

/*
ARGOS Trigger to populate audit table of NORMALISED_EVENT.
During rating revoke events are deleted from NORMALISED_EVENT table and files are deleted from NORMALISED_EVENT_FILE tables, 
deleted events must be also deleted from DWH. Hence we pupulate the deleted events data into NORMALISED_EVENT_A table, so that the DWH 
can read the deleted events and update there reports accordingly.
*/

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