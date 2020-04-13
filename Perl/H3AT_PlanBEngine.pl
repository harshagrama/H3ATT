#!/usr/bin/env perl
#---------------------------------------------------------------------------
#
#   File:       H3AT_PlanBEngine.pl
#   Created:    14-May-2019 17:57:50
#   Creator:    t_keirench  (Christophe Keirens)
#   $Revision: 2.60 $
#   $Id: H3AT_PlanBEngine.pl,v 2.60 2020/03/26 07:33:32 t_keirench Exp $
#   
# ===========================================================================
# COPYRIGHT (C) 2019 CSG SYSTEMS INTERNATIONAL, INC. AND/OR ITS 
# AFFILIATES ("CSG INTERNATIONAL"). ALL RIGHTS RESERVED. THE INFORMATION
# CONTAINED WITHIN THIS DOCUMENT OR APPLICATION IS THE PROPERTY OF CSG 
# INTERNATIONAL, WHICH IS CONFIDENTIAL AND PROTECTED BY INTERNATIONAL 
# COPYRIGHT LAWS AND ANY UNAUTHORISED USE OF THIS DOCUMENT OR APPLICATION 
# OR ITS CONTENTS MAY VIOLATE COPYRIGHT, TRADEMARK, AND OTHER LAWS. NO 
# PART OF THIS DOCUMENT OR APPLICATION MAY BE PHOTOCOPIED, REPRODUCED 
# OR TRANSLATED IN ANY FORM OR BY ANY MEANS, OR STORED IN A RETRIEVAL 
# SYSTEM OR TRANSMITTED ELECTRONICALLYOR OTHERWISE, WITHOUT THE PRIOR 
# WRITTEN CONSENT OF CSG INTERNATIONAL.
# ===========================================================================
#
# USAGE:
#   H3AT_PlanBEngine
#
# DESCRIPTION:
#   Initiated via a schedule to interrogate the business clarify for any changes that to the customer / products
#   that need to be propagated to Singleview as Flexi calls. 
#   Extra supported functionalities: 
#   1. MOCK mode allows for checking of mock Singleview tables instead of remote clarify
#      tables to support unit testing functionality without integration. 
#
# EXIT STATUS:
#   0       - Succeeded
#   1       - Error (+ Description)
#   2       - Usage
#
# REVISION HISTORY:
#   $Log: H3AT_PlanBEngine.pl,v $
#   Revision 2.60  2020/03/26 07:33:32  t_keirench
#   Ticket CF-9238: A number of minor improvements.
#
#   Revision 2.59  2020/03/25 15:19:13  t_keirench
#   Ticket CF-9238: Included fixes to the normal sync mode.
#
#   Revision 2.58  2020/03/25 08:13:40  t_keirench
#   Ticket CF-9238: Added support for encrypted passwords and also redone the testing mode and event processing logic
#
#   Revision 2.57  2020/03/12 14:44:47  t_keirench
#   Ticket CF-8371: Minimum Turnover feature and date handling
#
#   Revision 2.56  2020/03/11 12:16:31  t_keirench
#   Ticket CF-9108: Minor reporting fix
#
#   Revision 2.55  2020/03/11 09:22:36  t_keirench
#   Ticket CF-7965: Improved error reporting
#
#   Revision 2.54  2020/03/11 09:01:30  t_keirench
#   Ticket CF-7965: PCD support added and many other improvements to add Clarify validation errors and handle dates.
#
#   Revision 2.53  2020/03/10 13:43:57  t_keirench
#   Ticket CF-9108: Fixes for date issues on migration
#
#   Revision 2.52  2020/03/09 14:16:54  t_keirench
#   Ticket CF-9102: Yearly charge enhancements.
#
#   Revision 2.51  2020/03/05 14:36:28  schatzth
#   Ticket CF-7703_2003: delegated audit insert error handling to calling subroutine
#
#   Revision 2.50  2020/03/04 12:54:59  t_keirench
#   Ticket CF-9058: Added multiple email support and fix issue with e-bill output
#
#   Revision 2.49  2020/03/03 16:07:01  schatzth
#   Ticket CF-7703_2003: Added date DATE fields to Customer and Products Audit and Mock Tables
#
#   Revision 2.48  2020/03/02 18:20:33  schatzth
#   Ticket CF-7703_2003: logging to audit tables added
#
#   Revision 2.47  2020/03/01 15:48:16  t_keirench
#   Ticket CF-8901: Handle / clear data based on invoice media
#
#   Revision 2.46  2020/02/26 09:07:39  t_keirench
#   Ticket CF-7972: Fixed issue with event processing that creates accounts and products in the same execution.
#
#   Revision 2.44  2020/02/24 15:27:53  t_keirench
#   Ticket CF-8973: default rate plan added
#
#   Revision 2.43  2020/02/24 06:36:15  t_keirench
#   Ticket CF-8461: Many updates to support CRUD functionality
#
#   Revision 2.42  2020/02/19 13:20:37  t_keirench
#   Ticket CF-6609: Fixed Schedule_id issue with string instead of integer
#
#   Revision 2.41  2020/02/18 05:29:47  t_keirench
#   Ticket CF-7969: Number of enhancements
#
#   Revision 2.40  2020/02/12 13:32:50  t_keirench
#   Ticket CF-8924: Added -force_commit feature
#
#   Revision 2.39  2020/02/06 14:53:21  t_keirench
#   Ticket CF-6609: Adjustment support added
#
#   Revision 2.37  2020/01/28 04:58:48  t_keirench
#   Ticket CF-6617: Added support for CRM_REF for ITD
#
#   Revision 2.36  2020/01/27 23:49:37  t_keirench
#   Ticket CF-8816: Fixed mock loading issues
#
#   Revision 2.35  2020/01/24 13:11:00  t_keirench
#   Ticket CF-8777: Issue with OFFICIAL name and phone numbers
#
#   Revision 2.34  2020/01/21 13:11:26  t_keirench
#   Ticket CF-8748: Fix for phone numbers and task_queue_id is mandatory in non testing mode
#
#   Revision 2.33  2020/01/15 12:25:58  t_keirench
#   Ticket CF-6603: Added invoice total discounts
#
#   Revision 2.32  2019/12/30 10:50:34  t_keirench
#   Ticket CF-8645: Enhancements to handle Clarify data
#
#   Revision 2.31  2019/12/30 06:56:25  t_keirench
#   Ticket CF-8645: Clean data before sending to SV by ltrim and rtrim
#
#   Revision 2.30  2019/12/17 10:58:53  t_keirench
#   Ticket CF-7760-2: A number of fixes
#
#   Revision 2.23  2019/11/21 14:10:02  t_keirench
#   Ticket CF-8322: Enhancements
#
#   Revision 2.22  2019/11/12 12:24:40  t_keirench
#   Ticket CF-7760: Changed eligible rateplan
#
#   Revision 2.21  2019/11/05 10:28:30  t_keirench
#   Ticket CF-8198: removed the temporary limitation on DFU calls.
#
#   Revision 2.20  2019/11/01 21:42:12  ghazizra
#   Ticket CF-8198: Latest changes with Clarify data validations and modifications of start_date.
#
#   Revision 2.18  2019/10/25 07:00:51  t_keirench
#   Ticket CF-8198: NRC discount fix
#
#   Revision 2.17  2019/10/25 06:07:55  t_keirench
#   Ticket CF-7761: Generic subscription enhancement
#
#   Revision 2.16  2019/10/22 08:51:17  t_keirench
#   Ticket CF-8168: fixed issue with locales
#
#   Revision 2.15  2019/10/22 07:35:11  t_keirench
#   Ticket CF-8100: Corrected transaction prodlem with NRC on insert of service issue.
#
#   Revision 2.14  2019/10/18 09:29:02  t_keirench
#   Ticket CF-8134: Fix for non-migration mode
#
#   Revision 2.13  2019/10/17 11:15:40  t_keirench
#   Ticket CF-8134: Fixes for rate plan and also more validation and summary info provided. No more diing on Clarify DB error.
#
#   Revision 2.12  2019/10/15 14:07:50  t_keirench
#   Ticket CF-8093: Fixed one-off charge behaviour.
#
#   Revision 2.10  2019/09/30 02:24:15  t_keirench
#   Ticket CF-7675: Minor corrections
#
#   Revision 2.9  2019/09/22 22:43:25  t_keirench
#   Ticket CF-7675: Oneoff charges implemented
#
#   Revision 2.8  2019/09/12 12:29:04  t_keirench
#   Ticket CF-7295: Fixed cut paste error
#
#   Revision 2.7  2019/09/12 06:50:22  t_keirench
#   Ticket CF-7295: code review comments
#
#   Revision 2.6  2019/09/11 09:20:33  t_keirench
#   Ticket CF-7295: Enhancements for the MaintainAccount API call
#
#   Revision 2.5  2019/08/26 04:41:15  t_keirench
#   Ticket CF-6596: Update to support insert accounts via mock tables
#
#   Revision 2.4  2019/08/25 22:12:45  t_keirench
#   Ticket CF-6596: Updates to support Mocking
#
#   Revision 2.3  2019/07/26 06:32:11  t_keirench
#   Ticket CF-6567: Added support for usage rateplan product and number plan
#
#   Revision 2.2  2019/07/24 14:33:10  ghazizra
#   Ticket CF-7325: Renamed 4 parameters.
#
#   Revision 2.1  2019/06/28 05:40:17  t_keirench
#   Ticket CF-6517: Initial version
#
#---------------------------------------------------------------------------

#---------------------------------------------------------------------------
#       CORE PERL LIBRARIES
#---------------------------------------------------------------------------
use strict;
use FileHandle;
use Getopt::Long;
use POSIX;

# Force output after every line (keeps stderr and stdout in sync)
autoflush STDOUT 1;
autoflush STDERR 1;

#---------------------------------------------------------------------------
#       PROGRAM LIBRARIES
#---------------------------------------------------------------------------
use lib '.',
        $ENV{ATAI_LOC_SERVER_LIB},
        $ENV{ATAI_REL_SERVER_LIB},
        $ENV{ATA_REL_SERVER_LIB};
use savstd;
use atailib;
use ataierr qw(:std :SIL_GENERAL);
use ataiTRE;
use Time::HiRes qw( usleep ualarm gettimeofday tv_interval nanosleep clock_gettime clock_getres clock_nanosleep stat );
use Data::Dumper;
use ataiMultiProcess;

require "$ENV{ATA_SERVER_SHARED_LIB}/ora_trace_perl.pl";

#---------------------------------------------------------------------------
#       PRIVATE GLOBAL CONSTANTS
#---------------------------------------------------------------------------

my $MODULE_NAME = "H3AT_PlanBEngine";
my $MAX_DATE = "30-12-9999 23:59:59";
my $CURRENCY_ID = 1021;
my $INV_FORMAT = "H3AT BUS XML";
my $INV_VAS_FORMAT = "H3AT VAS CSV";
my $OPEN_ITEM = 1;
my $TAX_BUS_20 = 6;
my $TAX_BUS_0 = 8;
my $FALSE = 0;
my $YES = 'Yes';
my $NO = 'No';
my $SOURCE = 'CLARIFY';
my $CONTACT_TYPE_ID = 4100000;
my $CONTACT_STATUS_CODE = 2;
my $NON_USAGE_NUMBER_PLAN = 100;
my $FIXED_NUMBER_PLAN = 200;
my $FORMAT_DATETIME_SV = 'DD-MM-YYYY HH24:MI:SS';
my $INPUT_DIR = $ENV{ATA_DATA_SERVER_INPUT};
my $STATUS_NEW          = 1;
my $STATUS_SUCCESS      = 0;
my $STATUS_PROGRESS     = 3;
my $STATUS_ERROR        = 4;
my $STATUS_SKIPPED      = 10;
my $STATUS_NOT_MIGRATED = 11;
my $STATUS_PART_MIGRATED = 12;
my $MIGRATION_TYPE      = 10;
my $PREFIX_PROD         = 'PBP';
my $PREFIX_ACC          = 'PBA';
my $PAY_METHOD_DD       = 'DD';
my $PAY_METHOD_GIRO     = 'BG';
my $ACCOUNT_TYPE_NAME   = 'Postpaid';
my $ONE_OFF_PROD        = 99200;
my $ONE_OFF_ACC         = 99201;
my $ONE_OFF_CREDIT      = 99202;
my $PROD_BCD            = 'Big Customer Discount';
my $AUDIT_TABLE_SUFFIX  = '_A';
my $VOICE_SERV_TYPE_ID  = 4100005;
my $PROD_H3ATT_DSC_EFT  = 31001181;
my $PROD_H3ATT_DSC_INVTOTAL = 31001161;
my $PROD_H3ATT_DSC_PCD  = 31001221;
my $PROD_H3ATT_MTO      = 31001223;



my %zTableColumnNames =(
    CUST         =>     [ "CUSTOMER_NODE_REF", "START_DATE", "CUSTOMER_NODE_NAME", "OFFICIAL_NAME", "TITLE", "GIVEN_NAMES", "FAMILY_NAME", "EMAIL_ADDRESS", "HOME_PHONE_NR", "WORK_PHONE_NR", "MOBILE_PHONE_NR", "FAX_PHONE_NR", 
                        "SITE_LINE_1", "SITE_LINE_2", "SITE_POST_CODE", "SITE_CITY", "SITE_COUNTRY", "SITE_NAME", "SITE_CONTACT_NAME", "POSTAL_LINE_1", "POSTAL_LINE_2", "POSTAL_POST_CODE", "POSTAL_CITY", "POSTAL_COUNTRY", 
                        "BIRTH_DATE", "PAYMENT_METHOD", "BIC", "IBAN", "UID_NUMBER", "CRM_ACCOUNT_ID", "CUSTOMER_SEGMENT", "CUSTOMER_CLASSIFICATION", "CUSTOMER_SUB_CLASSIFICATION", "TAX_CLASS", "SALES_CHANNEL", "PAYMENT_DUE_PERIOD", 
                        "ICT_TAG", "GLOBAL_ONNET", "CALL_MASKING", "DSS_TCS", "INVOICE_LANGUAGE", "INVOICE_MEDIA", "CALL_DETAILS", "INVOICE_NOT_SEND", "AGENT", "REASON", "INVOICE_SORTING", "CHANGE_DATE"],
    CUST_OBJECTS =>     ["CUSTOMER_NODE_REF", "OBJECT_CODE", "START_DATE", "EDI_FORMAT", "NOTIFICATION_LIST", "DELIVERY_LIST", "DELIVERY_METHOD"],
    PROD         =>     ["CUSTOMER_NODE_REF", "PRODUCT_REF", "START_DATE", "CRM_PRODUCT_PARENT", "CRM_PRODUCT_TOP", "DISPLAY_ALWAYS", "EDI_ID", "NOTICE_ON_INVOICE", "ONNET_SERVICE", 
                        "PRODUCT_CLASS", "PRODUCT_GROUP", "PRODUCT_NAME", "PRODUCT_REF_CODE", "RATE_CLASSIFICATION", "RATE_PLAN", "SERVICE_CLASS", "SERVICE_DISPLAY_NAME", "SERVICE_INFO", 
                        "SERVICE_NAME", "TECHNICAL_SITE", "VPN_SERVICE", "CO_CODE", "FI_CODE", "PRODUCT_STATUS", "CANCEL_DATE", "CLFY_SP_OBJID", "CHANGE_DATE", "BIL_PARM_EFF_DATE", "RATE_PARM_EFF_DATE", "IS_SV_PRODUCT"],
    CM           =>     ["PRODUCT_REF", "CM_CODE", "CM_REF", "SV_PARAMETER" ,"SV_VALUE"],
    ADJ          =>     ["CUSTOMER_NODE_REF", "PRODUCT_REF", "CM_REF", "INVOICE_TEXT", "START_DATE", "RATE", "REASON", "CO_CODE", "FI_CODE", "CHARGE_ID", "FIN_PERCENT", "CASE_OBJID"]
);
                
my %excludedCM_CODEs = (
    UOW         =>  1,
    NLL            =>  1,
    USD            =>  1
);


#---------------------------------------------------------------------------
#       PRIVATE GLOBAL VARIABLES
#---------------------------------------------------------------------------

# Singleview and Clarify DB handles. 
use vars qw(
    $zdb 
    $zdb_cly
); 

my $znow = "";  # Current time string
my %zSQLs;      # stores the DB cursors
my %zCSRs;      # Global cursors  
my %zRefData;   # stores the reference type data
my %zAccProdOrder; # Current account product details
my %zChgClass;  
my %zInvSorting;
my %zStats;
my $zParentPid = $$;
my $zNumStreams = 3;
my $zConnectString;
my $zTaskId;
my $zEffDate;
my $zTransactionTime;
my $zBR_Business;
my $zBR_Reseller;
my $zMigrationDate;
my $zBilledDate;
my $zMigSortDate;
my $zMigId;
my $zModeNormal = 0;
my $zAllAccountsRef;
my $zAllProdNameRef;
my $zLogFile;
my $zChildWorkCnt;
my $zErrorFile;
my @zServMandFields;
my @zAdjustments; # Stores adjustments to create. 
my @zAccounts;

#---------------------------------------------------------------------------
#       GETOPT VARIABLES
#---------------------------------------------------------------------------

use vars qw(
    $opt_debug
    $opt_v
    $opt_u
    $opt_d
    $opt_load_mock
    $opt_mock
    $opt_site
    $opt_migration
    $opt_filename
    $opt_child
    $opt_testing
    $opt_objid
    $opt_mock_clean
    $opt_only_mig
    $opt_perf
    $opt_vas
    $opt_cly_connect
    $opt_schedule_id
    $opt_force_commit
    $opt_tran_time
);


#---------------------------------------------------------------------------
#       PRIVATE SUBROUTINES
#---------------------------------------------------------------------------

#***************************************************************************
# NAME:
#   zBuildCursors
#
# DESCRIPTION:
#   Places all the cursors into a hash (%zSQLs) for preparing against the needed DB connection. 
#   cursors are: 
#   BE_UPDATE       - UPDATE cursor of the H3AT_PBE_EVENT_AUDIT fields that are managed to track processing.
#   BE_STATUS       - UPDATE cursor of the ORDER_STATUS field of the H3AT_PBE_EVENT_AUDIT table.
#   CUST_EVENTS     - SELECT the events to be processed at the account level. EVENT_TYPE = 52
#   PROD_EVENTS     - SELECT the events to be processed at the product level. EVENT_TYPE = 15
#   ADJ_EVENTS      - SELECT the events to be processed to create adjustments. EVENT_TYPE = 0
#   CUST            - SELECT customer details from CLARIFY or MOCK tables. 
#   CUST_OBJECTS    - SELECT the customer objects (EBILL, EVA, ELFE)
#   CUST_ADJ        - SELECT the adjustments that need to be applied. 
#   EXISTING_ACCOUNTS - Gets all the existing accounts in the system to know if this is an insert / update. 
#   PROD_LOAD       - PACKAGE function that loads the product details into gloab temporary tables in Clarify to populate product info.
#   PROD            - SELECT to fetch product details
#   PROD_CM         - SEELCT to fetch all the product Charge Mechanism data. 
#   CUST_PROD       - SELECT the customer products for migration process. 
#   ONEOFF_EXISTS   - Get list of all generated one-off charges to limit creating them if already done. 
#
# PARAMETERS:
#
# RETURNS:
#   Describe what the subroutine returns.
#***************************************************************************
sub zBuildCursors
{

    ###########################
    ### BILLING_EVENT cursors
    ###########################
    
    
    $zSQLs{BE_UPDATE} = <<"EOS";
        UPDATE H3AT_PBE_EVENT_AUDIT
           SET ORDER_STATUS = ?,
               PROCESSED_DATE = SYSDATE,
               TASK_QUEUE_ID = ?,
               TIME_MS = ?,
               ERROR_CODE = ?,
               ERROR_MESSAGE = ?
         WHERE OBJID = ?
EOS

    $zSQLs{BE_STATUS} = <<"EOS";
        UPDATE H3AT_PBE_EVENT_AUDIT
           SET ORDER_STATUS = ?
         WHERE OBJID = ?
EOS

    $zSQLs{CUST_EVENTS} = <<"EOS";
        SELECT OBJID,
               X_BILL_SITE_ID,
               TO_CHAR(X_CREATION_TIME,'DD-MM-YYYY') || ' 00:00:00' AS START_DATE
          FROM H3AT_PBE_EVENT_AUDIT
         WHERE ORDER_STATUS = ?
           AND X_OBJECT_TYPE IN (52,20)
           AND PROCESSED_DATE is NULL
           AND X_CREATION_TIME <= to_date(?,'DD-MM-YYYY HH24:MI:SS')
EOS

    $zSQLs{PROD_EVENTS} = <<"EOS";
        SELECT OBJID,
               X_BILL_SITE_ID,
               X_PRODUCT_REF,
               TO_CHAR(X_CREATION_TIME,'DD-MM-YYYY') || ' 00:00:00' AS START_DATE
          FROM H3AT_PBE_EVENT_AUDIT
         WHERE ORDER_STATUS = ?
           AND X_OBJECT_TYPE = 15
           AND PROCESSED_DATE is NULL
           AND X_CREATION_TIME <= to_date(?,'DD-MM-YYYY HH24:MI:SS')
EOS

    $zSQLs{ADJ_EVENTS} = <<"EOS";
        SELECT OBJID,
               X_BILL_SITE_ID,
               X_OBJECT_ID,
               TO_CHAR(X_CREATION_TIME,'DD-MM-YYYY') || ' 00:00:00' AS START_DATE
          FROM H3AT_PBE_EVENT_AUDIT 
         WHERE ORDER_STATUS = ?
           AND X_OBJECT_TYPE = 0
           AND PROCESSED_DATE is NULL
           AND X_CREATION_TIME <= to_date(?,'DD-MM-YYYY HH24:MI:SS')
EOS

    if ($opt_site) {
        $zSQLs{CUST_EVENTS} .= " AND X_BILL_SITE_ID = \'$opt_site\'";
        $zSQLs{PROD_EVENTS} .= " AND X_BILL_SITE_ID = \'$opt_site\'";
        $zSQLs{ADJ_EVENTS}  .= " AND X_BILL_SITE_ID = \'$opt_site\'";
    }

    ########################### CUSTOMER cursors ###########################

    $zSQLs{CUST} = <<"EOS";
    SELECT CUSTOMER_NODE_REF,
           TO_CHAR(GREATEST(TO_DATE('01-01-2019','DD-MM-YYYY'),TRUNC(START_DATE)),'DD-MM-YYYY HH24:MI:SS') as START_DATE,
           CUSTOMER_NODE_NAME,
           OFFICIAL_NAME,
           TITLE,
           GIVEN_NAMES,
           FAMILY_NAME,
           EMAIL_ADDRESS,
           HOME_PHONE_NR,
           WORK_PHONE_NR,
           MOBILE_PHONE_NR,
           FAX_PHONE_NR,
           SITE_LINE_1,
           SITE_LINE_2,
           SITE_POST_CODE,
           SITE_CITY,
           SITE_COUNTRY,
           SITE_NAME,
           SITE_CONTACT_NAME,
           POSTAL_LINE_1,
           POSTAL_LINE_2,
           POSTAL_POST_CODE,
           POSTAL_CITY,
           POSTAL_COUNTRY,
           BIRTH_DATE,
           PAYMENT_METHOD,
           BIC,
           IBAN,
           UID_NUMBER,
           CRM_ACCOUNT_ID,
           CUSTOMER_SEGMENT,
           CUSTOMER_CLASSIFICATION,
           CUSTOMER_SUB_CLASSIFICATION,
           TAX_CLASS,
           SALES_CHANNEL,
           PAYMENT_DUE_PERIOD,
           ICT_TAG,
           GLOBAL_ONNET,
           CALL_MASKING,
           DSS_TCS,
           INVOICE_LANGUAGE,
           INVOICE_MEDIA,
           CALL_DETAILS,
           INVOICE_NOT_SEND,
           AGENT,
           REASON,
           NVL(INVOICE_SORTING,'Product family') AS INVOICE_SORTING,
           TO_CHAR(TRUNC(CHANGE_DATE),'DD-MM-YYYY HH24:MI:SS') as CHANGE_DATE
EOS

    if ($opt_mock) {
        $zSQLs{CUST} .= " 
      FROM H3AT_EITT_CUSTOMER
     WHERE CUSTOMER_NODE_REF = ?
     ";
    } else {
        $zSQLs{CUST} .= " 
      FROM VIEW_EITT_CUSTOMER
     WHERE CUSTOMER_NODE_REF = ?
     ";
    }
    
    # Get Customer Object Data (EVA, ELFE and EBILL). 
    $zSQLs{CUST_OBJECTS} = <<"EOS";
    SELECT CUSTOMER_NODE_REF,
           OBJECT_CODE,
           START_DATE,
           EDI_FORMAT,
           NOTIFICATION_LIST,
           DELIVERY_LIST,
           DELIVERY_METHOD
EOS

    if ($opt_mock) {
        $zSQLs{CUST_OBJECTS} .= " 
      FROM H3AT_EITT_CUSTOMER_OBJECTS
     WHERE CUSTOMER_NODE_REF = ?
     ";
    } else {
        $zSQLs{CUST_OBJECTS} .= " 
      FROM VIEW_EITT_CUSTOMER_OBJECTS
     WHERE CUSTOMER_NODE_REF = ?
     ";
    }
    
    
     # Get Customer Adjustments. 
    $zSQLs{CUST_ADJ} = <<"EOS";
    SELECT CUSTOMER_NODE_REF,
           PRODUCT_REF,
           CM_REF,
           INVOICE_TEXT,
           START_DATE,
           RATE,
           REASON,
           CO_CODE,
           FI_CODE,
           CHARGE_ID,
           FIN_PERCENT,
           CASE_OBJID
EOS

    if ($opt_mock) {
        $zSQLs{CUST_ADJ} .= " 
      FROM H3AT_EITT_CM_ADJ
     WHERE CUSTOMER_NODE_REF = ?
     ";
    } else {
        $zSQLs{CUST_ADJ} .= " 
      FROM VIEW_EITT_CM_ADJ
     WHERE CUSTOMER_NODE_REF = ?
     ";
    }  
    
    # for normal processing, we find specific adjustment based on event table object_id
    if (not $opt_load_mock) {
        $zSQLs{CUST_ADJ} .= " AND CASE_OBJID = ? ";
    }
    
    # Get all the accounts in the system. 
    $zSQLs{EXISTING_ACCOUNTS} = <<"EOS";
        SELECT ACC.ACCOUNT_NAME, 
               ACC.CUSTOMER_NODE_ID, 
               TO_CHAR(TRUNC(CNH.ACTIVE_DATE),'DD-MM-YYYY HH24:MI:SS') as ACTIVE_DATE
          FROM ACCOUNT ACC
          JOIN CUSTOMER_NODE_HISTORY CNH ON ACC.CUSTOMER_NODE_ID = CNH.CUSTOMER_NODE_ID
         WHERE ACC.GENERAL_2 = 'CLARIFY'
           AND SYSDATE BETWEEN CNH.EFFECTIVE_START_DATE AND CNH.EFFECTIVE_END_DATE
EOS

    ########################### PRODUCT cursors ###########################
    
    $zSQLs{PROD_LOAD} = <<"EOS";
    begin
        SA.PKG_X_BILLING.SVPRODUCTCOMP2TABLE(P_INSTANCE_ID => :1);
    end;
EOS

    $zSQLs{PROD} = <<"EOS";
    SELECT CUSTOMER_NODE_REF,
           PRODUCT_REF,
           TO_CHAR(TRUNC(START_DATE),'DD-MM-YYYY HH24:MI:SS') as START_DATE,
           CRM_PRODUCT_PARENT,
           CRM_PRODUCT_TOP,
           DISPLAY_ALWAYS,
           EDI_ID,
           NOTICE_ON_INVOICE,
           ONNET_SERVICE,
           PRODUCT_CLASS,
           PRODUCT_GROUP,
           PRODUCT_NAME,
           PRODUCT_REF_CODE,
           RATE_CLASSIFICATION,
           RATE_PLAN,
           SERVICE_CLASS,
           SERVICE_DISPLAY_NAME,
           SERVICE_INFO,
           SERVICE_NAME,
           TECHNICAL_SITE,
           VPN_SERVICE,
           CO_CODE,
           FI_CODE,
           PRODUCT_STATUS, 
           CANCEL_DATE,
           CLFY_SP_OBJID,
           TO_CHAR(TRUNC(CHANGE_DATE),'DD-MM-YYYY HH24:MI:SS') as CHANGE_DATE,
           TO_CHAR(TRUNC(BIL_PARM_EFF_DATE),'DD-MM-YYYY HH24:MI:SS') as BIL_PARM_EFF_DATE,
           TO_CHAR(TRUNC(RATE_PARM_EFF_DATE),'DD-MM-YYYY HH24:MI:SS') as RATE_PARM_EFF_DATE,
           IS_SV_PRODUCT
EOS
    
    
    if ($opt_mock) {
        $zSQLs{PROD} .= " 
      FROM H3AT_EITT_PRODUCT
     WHERE PRODUCT_REF = ?
     ";
    } else {
        $zSQLs{PROD} .= " 
      FROM SV_PRODUCT_DATA 
     WHERE PRODUCT_REF = ?
     ";
    }

    ########################### CM cursors ###########################    

    $zSQLs{PROD_CM} = <<"EOS";
    SELECT PRODUCT_REF,
           CM_CODE,
           CM_REF,
           SV_PARAMETER,
           SV_VALUE
EOS

    if ($opt_mock) {
        $zSQLs{PROD_CM} .= " 
      FROM H3AT_EITT_PROD_CM
     WHERE PRODUCT_REF = ?
     ";
    } else {
        $zSQLs{PROD_CM} .= " 
      FROM SV_PRODUCT_DETAILS 
     WHERE PRODUCT_REF = ?
     ";
    }
    
    $zSQLs{CUST_PROD} = <<"EOS";
        SELECT BILLSITE_ID,
               INSTANCE_ID,
               PRODUCT_NAME,
               PART_STATUS,
               START_DATE,
               END_DATE
        FROM TABLE(SA.PKG_X_BILLING.GET_BILLSITE_PRODUCTS(P_SITE_ID => ?))
     ORDER BY INSTANCE_ID
EOS
        
    # Checks if the charge has already been created and skips it if this is the case. 
    # The number is the function defn id used to store it. 
    $zSQLs{ONEOFF_EXISTS} = <<"EOS";
        SELECT 1
          FROM H3AT_API_IDEMPOTENT_REF
          WHERE API_TYPE_ID = 4202618
            AND REF_KEY_1 = ?
EOS

    # Check if the companion product if active at the requested cancel date.
    $zSQLs{REC_ACTIVE} = <<"EOS";
        SELECT 1 
          FROM PRODUCT_INSTANCE_HISTORY REC
          JOIN ACCOUNT ACC ON ACC.CUSTOMER_NODE_ID = REC.CUSTOMER_NODE_ID
         WHERE BASE_PRODUCT_INSTANCE_ID IS NOT NULL
           AND ACC.ACCOUNT_NAME = ?
           AND TO_DATE(?,'DD-MM-YYYY') BETWEEN REC.EFFECTIVE_START_DATE AND REC.EFFECTIVE_END_DATE
           AND REC.PRODUCT_INSTANCE_STATUS_CODE = 3
           AND REC.GENERAL_10 = ?
EOS


    ###########################
    ### MIGRATION cursors
    ###########################
    if ($opt_migration) {
        # Used to insert entry for migration support.
        $zSQLs{BE_MIG_INSERT} = <<"EOS";
        INSERT INTO H3AT_PBE_EVENT_AUDIT
            (OBJID,X_OBJECT_TYPE,X_BILL_SITE_ID,X_CREATION_TIME,TASK_QUEUE_ID,CREATION_DATE,ORDER_STATUS,TIME_MS,ERROR_CODE,ERROR_MESSAGE)
        VALUES
            (SEQ_H3AT_PBE_EVENT_SEQ.nextval,10,?,SYSDATE,?,SYSDATE,?,?,?,?)
EOS
    
    }
    
    
    
    ###########################
    ### AUDIT AND MOCK LOADING cursors
    ###########################
    
    my $bindingAdditions;
    my $tableSuffix;
    my $columnAdditions;
    
    if ($opt_load_mock) { 
        $bindingAdditions = '';
        $tableSuffix =  '';
        $columnAdditions = '';
    } else {
        $bindingAdditions = " SYSDATE, ?, ";
        $tableSuffix =  $AUDIT_TABLE_SUFFIX;
        $columnAdditions = "TIME_STAMP, OBJECT_ID, ";
    }
    
 ##MOCK
        $zSQLs{CUST_INSERT} = <<"EOS";
        INSERT INTO H3AT_EITT_CUSTOMER$tableSuffix
            ( @{[ $columnAdditions . join(', ', @{$zTableColumnNames{CUST}}) ]})
        VALUES
            ( $bindingAdditions ?,to_date(?,'DD-MM-YYYY HH24:MI:SS'),?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?,?,?, to_date(?,'DD-MM-YYYY HH24:MI:SS'),?,?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?,?,to_date(?,'DD-MM-YYYY HH24:MI:SS'))
EOS

        ### CUSTOMER Objects insert. TODO fix date format. 
##MOCK
        $zSQLs{CUST_OBJECTS_INSERT} = <<"EOS";
        INSERT INTO H3AT_EITT_CUSTOMER_OBJECTS$tableSuffix
            ( @{[ $columnAdditions . join(', ', @{$zTableColumnNames{CUST_OBJECTS}}) ]} )
        VALUES
            ( $bindingAdditions ?,?,to_date(?,'DD.MM.YYYY'),?,?,?,?)
EOS

##MOCK
        $zSQLs{PROD_INSERT} = <<"EOS";
            INSERT INTO H3AT_EITT_PRODUCT$tableSuffix
                ( @{[ $columnAdditions . join(', ', @{$zTableColumnNames{PROD}}) ]} )
            VALUES
                ( $bindingAdditions ?,?,to_date(?,'DD-MM-YYYY HH24:MI:SS'),?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?, ?,to_date(?,'DD-MM-YYYY HH24:MI:SS'),to_date(?,'DD-MM-YYYY HH24:MI:SS'),to_date(?,'DD-MM-YYYY HH24:MI:SS'), ? )
EOS

##MOCK
        $zSQLs{CM_INSERT} = <<"EOS";
            INSERT INTO H3AT_EITT_PROD_CM$tableSuffix
                ( @{[ $columnAdditions . join(', ', @{$zTableColumnNames{CM}}) ]} )
            VALUES  
                ( $bindingAdditions ?,?,?,?,?)
EOS


##MOCK
        $zSQLs{ADJ_INSERT} = <<"EOS";
            INSERT INTO H3AT_EITT_CM_ADJ$tableSuffix
                ( @{[ $columnAdditions . join(', ', @{$zTableColumnNames{ADJ}}) ]} )
            VALUES  
                ( $bindingAdditions ?,?,?,?,to_date(?,'DD.MM.YYYY'),?,?,?,?,?,?,?)
EOS

    
    
    
    if ($opt_mock_clean) {
        $zSQLs{CUST_DELETE}     = "DELETE FROM H3AT_EITT_CUSTOMER  WHERE CUSTOMER_NODE_REF = \'$opt_site\'";
        $zSQLs{CUST_OBJ_DELETE} = "DELETE FROM H3AT_EITT_CUSTOMER_OBJECTS WHERE CUSTOMER_NODE_REF = \'$opt_site\'";
        $zSQLs{CUST_ADJ_DELETE} = "DELETE FROM H3AT_EITT_CM_ADJ WHERE CUSTOMER_NODE_REF = \'$opt_site\'";
        $zSQLs{PROD_DELETE}     = "DELETE FROM H3AT_EITT_PRODUCT WHERE CUSTOMER_NODE_REF = \'$opt_site\'";
        $zSQLs{CM_DELETE}       = "DELETE FROM H3AT_EITT_PROD_CM WHERE PRODUCT_REF in (SELECT PRODUCT_REF FROM H3AT_EITT_PRODUCT WHERE CUSTOMER_NODE_REF = \'$opt_site\')";
        $zSQLs{EVENT_DELETE}    = "DELETE FROM H3AT_PBE_EVENT_AUDIT WHERE X_BILL_SITE_ID = \'$opt_site\'";
    }


    ### SUPPORT SQL CURSORS
    $zSQLs{PROD_CANCELLED} = <<"EOS";
    SELECT 1 
      FROM SERVICE_HISTORY
     WHERE NETWORK_NAME = ?
       AND SERVICE_STATUS_CODE = 9 -- CANCELLED
       AND ? BETWEEN EFFECTIVE_START_DATE AND EFFECTIVE_END_DATE
EOS

    $zSQLs{DATE_INC} = "SELECT TO_CHAR(TO_DATE(?,?) + ?,?) FROM DUAL";
    
}

#***************************************************************************
# NAME:
#   zInit
#
# DESCRIPTION:
#   Sets up connections and the SQLS to be executed. 
#
# PARAMETERS:
#
# RETURNS:
#   Describe what the subroutine returns.
#***************************************************************************
sub zInit
{
    
    # Validate parameter combinations. 
    # -mock can not be combined with -load_mock
    if ($opt_mock && $opt_load_mock) {
        &zdie($MSG_PASSTHRU, "USAGE: Options -mock and -load_mock are incompatible.");
    }
    
    if ($opt_mock_clean && ( not $opt_site )) {
        &zdie($MSG_PASSTHRU, "USAGE: Options -mock_clean only supported with a -site <bill_site_id> option");
    }
    
    # -migration can not be combined with -load_mock
    if ($opt_migration && $opt_load_mock) {
        &zdie($MSG_PASSTHRU, "USAGE: Options -migration and -load_mock are incompatible.");
    }
    
    # force_commit is only allowed in migration mode. 
    if (not $opt_migration and $opt_force_commit) {
        &zdie($MSG_PASSTHRU, "USAGE: Option -force_commit is only supported with -migration mode.");
    }
    
    if ($opt_filename and $opt_site) {
        &zdie($MSG_PASSTHRU, "USAGE: Either -filename <filename> or -site <site> is provided, but NOT both.");
    }
    
    if ($opt_migration and not $opt_filename and not $opt_site ) {
        &zdie($MSG_PASSTHRU, "USAGE: Options -filename <filename> must be provided in -migration mode.");
    }
    
    if ($opt_testing and not $opt_site ) {
        &zdie($MSG_PASSTHRU, "USAGE: testing mode is only supported on a single account with -site <bill_site_id>.");
    }
    
    # Get parameters passed. 
    $zTaskId = $ARGV[0];
    $zEffDate = $ARGV[1];
    
    if ($opt_mock and length($zEffDate) == 0) {
        $zEffDate = '30-12-9999 23:59:59';
    }
    
    if ( length($zTaskId) == 0) { 
        &zdie($MSG_PASSTHRU, "ERROR: task_queue_id is mandatory");
    }
    
    # Create ChargeClassification mappings. 
    # <AdvancePeriod>:<Pro-rate>:<Frequency>
    $zChgClass{'1:True:Monthly'}{REC} = 'Bus Monthly Adv Pro';
    $zChgClass{'1:True:Monthly'}{DSC} = 'Bus Dsc Monthly Adv Pro';
    $zChgClass{'0:True:Monthly'}{REC} = 'Bus Monthly Arr Pro';
    $zChgClass{'0:True:Monthly'}{DSC} = 'Bus Dsc Monthly Arr Pro';
    $zChgClass{'1:False:Yearly'}{REC} = 'Bus Yearly Adv Non-Pro';
    $zChgClass{'1:False:Yearly'}{DSC} = 'Bus Dsc Yearly Adv Non-Pro';
    
    # Product sorting mappings
    $zInvSorting{'Product family'} = 'Product Class';
    $zInvSorting{'Technical site'} = 'Tech Site';
    
    

    ### DB connections. 
    ### Always connect to Singleview. Clarify is optional depending on mode.
    $zdb = &ataiDbOpen($ENV{"ATADBACONNECT"}) || &zdie($MSG_PASSTHRU, $errstr);

    ### Open TRE connection
    &ataiTREConnect($opt_u) || &zdie($MSG_PASSTHRU, $errstr);
    
    # Get configuration item settings. 
    my $ciRef = biConfigurationItemFetchByType_STR_H('PLANB',1);
    ($errstr) && &zdie($MSG_PASSTHRU, $errstr);
        
    if ($ciRef) {
        $zNumStreams        = $ciRef->{'Child Processes'};
        $zTransactionTime   = $ciRef->{'Max Transaction Time'};
        $zMigrationDate     = substr($ciRef->{'Migration Date'},0,10);
        $zBilledDate        = substr($ciRef->{'Billed Date'},0,10);
        $zBR_Business       = $ciRef->{'Business Bill Cycle'}*1;
        $zBR_Reseller       = $ciRef->{'Reseller Bill Cycle'}*1;
        $zMigrationDate =~ m/^(\d\d).(\d\d).(\d\d\d\d).*/;
        $zMigSortDate = "$3$2$1";
        my $encPassword = $ciRef->{'Password'};
        my $username = $ciRef->{'Username'};
        my $pass = biAttributeValueDecrypt($encPassword);
        ($errstr) && &zdie($MSG_PASSTHRU, "Decryption failed with error string: ".$errstr);
            
        $zConnectString = $ciRef->{'Username'} . '/' . biAttributeValueDecrypt($encPassword) . '@' . $ciRef->{'Clarify SID'};
    }
    
    # override default setting from options if provided. 
    if ($opt_cly_connect) {
        $zConnectString = $opt_cly_connect;
    }
    
    if ($opt_child) {
        $zNumStreams = $opt_child;
    } elsif ($opt_site) { # If opt_site is provided we limit child processed to 1. 
        $zNumStreams = 1;
        print "In site mode we limit child processes to 1.\n";
    }
    
    if ($opt_schedule_id) {
        $zBR_Business = $zBR_Reseller = $opt_schedule_id*1;
    }
    
    if ($opt_tran_time) {
        $zTransactionTime = $opt_tran_time;
    }
    
    # Cache reference type data that might be needed. 
    &zCacheRefData('WRITTEN_LANGUAGE');
    &zCacheRefData('H3AT_NUMBER_REGION_PLAN');
    &zCacheRefData('H3AT_GENERIC_FU_TYPE');
    
    # Get all the available rateplan products. 
    # Determine all the accounts that exist in the system to limit the entries to be processed
    my $prodNameSQL = <<"EOS";
        SELECT PRODUCT_NAME
          FROM PRODUCT_HISTORY
         WHERE PRODUCT_ID IN ( SELECT DISTINCT(INDEX1_VALUE) FROM DERIVED_ATTRIBUTE_ARRAY WHERE DERIVED_ATTRIBUTE_ID = 4200347 and RESULT2_VALUE = 6)
EOS

    $zAllProdNameRef = $zdb->selectall_hashref($prodNameSQL,'PRODUCT_NAME') || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
    
    # mandatory usage service fields
    @zServMandFields = ('CO_CODE','FI_CODE','SERVICE_CLASS');
    
    # disconnect from the TRE.
    &ataiTREDisconnect() || &zdie($MSG_PASSTHRU, $errstr);
    
    # Build all the SQL cursors for use. 
    &zBuildCursors();
    
    # Initialise stats
    $zStats{ACCOUNT_CNT}  = 0;
    $zStats{PRODUCT_CNT}  = 0;
    $zStats{EVENT_CNT}    = 0;
    $zStats{API_ACC_CNT}  = 0;
    $zStats{API_PROD_CNT} = 0;
    $zStats{API_CHG_CNT}  = 0;
    $zStats{PROD_SQL_CNT} = 0;
    $zStats{CM_SQL_CNT}   = 0;
    $zStats{CM_CHG_SKIPPED} = 0;
    $zStats{VALIDATION_CNT} = 0;
    
}

#***************************************************************************
# NAME:
#   zProcessEvents
#
# DESCRIPTION:
#   Determines The OBJID that has been processed to limit the fetched events for processing. 
#   If this is the first time. Then we expect the -objid <number> to be provided as input.  
#
# PARAMETERS:
#
# RETURNS:
#   Describe what the subroutine returns.
#***************************************************************************
sub zProcessEvents
{

    my ($limitByAccounts, $objId, %LimitAccounts, $loadCnt, $objIdsRef, $skippedCnt, $alreadyProcessed);
    
    ### This SQL gets all the objectIds already processed in the current day. 
    my $SQL_EVENTS_PROCESSED = <<"EOS";
        SELECT OBJID 
          FROM H3AT_PBE_EVENT_AUDIT
         WHERE X_OBJECT_TYPE IN ('15','52','2213')
           AND X_CREATION_TIME >= to_date(?,'DD-MM-YYYY')
           AND X_CREATION_TIME < to_date(?,'DD-MM-YYYY HH24:MI:SS')
EOS

    ### This SQL will fetch all the billing events to load into the SV table to be processed.
    my $SQL_BILL_EVENT = <<"EOS";
        SELECT evt.OBJID, 
               evt.X_OBJECT_TYPE,
               evt.X_OBJECT_ID,
               evt.X_BILL_SITE_ID,
               tsp.INSTANCE_ID X_PRODUCT_REF,
               to_char(evt.X_CREATION_TIME,'DD-MM-YYYY HH24:MI:SS') X_CREATION_TIME
          FROM TABLE_X_BILLING_EVENT evt
          LEFT JOIN TABLE_SITE_PART tsp ON evt.X_OBJECT_ID = tsp.OBJID
          WHERE evt.X_CREATION_TIME >= to_date(?,'DD-MM-YYYY')
            AND evt.X_CREATION_TIME < to_date(?,'DD-MM-YYYY HH24:MI:SS')
EOS

    my $SQL_BE_INSERT = <<"EOS";
        INSERT INTO H3AT_PBE_EVENT_AUDIT
            (OBJID,X_OBJECT_TYPE,X_OBJECT_ID,X_BILL_SITE_ID,X_PRODUCT_REF,X_CREATION_TIME,TASK_QUEUE_ID,CREATION_DATE,ORDER_STATUS)
        VALUES
            (?,?,?,?,?,to_date(?,'DD-MM-YYYY HH24:MI:SS'),?,SYSDATE,1)
EOS

    $loadCnt = $skippedCnt = $alreadyProcessed = 0;
    
    # Get a list of objectid's that already exist in our AUDIT table as these will be skipped in the event setup process.
    &zDebug('D',"Preparing SQL - " . $SQL_EVENTS_PROCESSED);
    $objIdsRef = $zdb->selectall_hashref($SQL_EVENTS_PROCESSED,'OBJID',{}, (substr($zEffDate,0,10),$zEffDate));
    &zDebug('D',"Processed objectIDs are: \n" . Dumper($objIdsRef) );
    
    &zDebug('D',"Preparing SQL - " . $SQL_BILL_EVENT);
    my $CSR_BILL_EVENT = $zdb_cly->prepare($SQL_BILL_EVENT) || &zdie(6000, __LINE__, __FILE__, $zdb_cly->errstr);
    
    # Setup insert cursors
    &zDebug('D',"Preparing SQL - " . $SQL_BE_INSERT);
    my $CSR_BE_INSERT = $zdb->prepare($SQL_BE_INSERT) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
    
    # Limit processing to requested bill sites. 
    if (&zAccountsToProcess() == 0) {
        # We need to limit the events to process to be limited to the requested accounts. 
        %LimitAccounts = @zAccounts;
        &zDebug('D',"limiting accounts to be processed to: \n" . Dumper(%LimitAccounts) );
    }
    
    print "Searching for events to process between " . substr($zEffDate,0,10) . " 00:00:00 and $zEffDate \n";
    
    # Fetch the billing events and insert them into the local table to be processed. 
    $CSR_BILL_EVENT->execute(substr($zEffDate,0,10),$zEffDate) || &zdie(6001, __LINE__, __FILE__, $zdb_cly->errstr);
    while (my @evtRow = $CSR_BILL_EVENT->fetchrow_array()) {
        # Only include events for requested sites if there is a limit.
        if ( not %LimitAccounts or (exists $LimitAccounts{$evtRow[3]} )) {
            # We skip the loading of any events that already exist. 
            if ( not $objIdsRef->{$evtRow[0]} ) {
                push(@evtRow,$zTaskId);
                $CSR_BE_INSERT->execute(@evtRow) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
                &zDebug('D',"Loaded event with objid ($evtRow[0]) to be processed");
                $loadCnt++;
            } else {
                $alreadyProcessed++;
            }
        } else {
            $skippedCnt++;
        }
    }
    
    print "Events fetched for processing: $loadCnt\n";
    $opt_v && $skippedCnt && print "Events skipped due to limit by bill sites: $skippedCnt\n";
    $opt_v && $alreadyProcessed && print "Events skipped due to already fetched previously: $alreadyProcessed\n";
    
    # Commit changes if not in testing mode. 
    $opt_testing or $zdb->commit();
    
    return $loadCnt;
    
}

#***************************************************************************
# NAME:
#   zProcessAccounts
#
# DESCRIPTION:
#   Processes all the changes in billing events that require a account update.  
#
# PARAMETERS:
#
# RETURNS:
#   Describe what the subroutine returns.
#***************************************************************************
sub zProcessAccounts
{
    my ($accName, $objid, $startTime, $AccOrderRef, %lProcessedAcc, $startDate, $eventTime, $ms, $ErrorsRef);
    
    ### Process the events that have not been processed yet for account
    &zDebug('D',"In zProcessAccounts executing event fetch for - $accName");
    $zCSRs{CUST_EVENTS}->execute($STATUS_NEW,$zEffDate) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
    
    while ( my $RecRow = $zCSRs{CUST_EVENTS}->fetchrow_hashref("NAME_uc") ) {
        $objid = $RecRow->{OBJID};
        $accName = $RecRow->{X_BILL_SITE_ID};
        $startDate = $RecRow->{START_DATE};
        $zStats{EVENT_CNT} += 1;
        
        $zCSRs{BE_STATUS}->execute($STATUS_PROGRESS,$objid) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
        # Set the event to be in progress, and record the starting timestamp
        $opt_testing or $zdb->commit();
        $eventTime = [gettimeofday];
        
        # 
        if ($lProcessedAcc{$accName}) {
            # already processed this account. no need to update again. just update the record that is processed. 
            &zDebug('V','Skipping as already processed this account');
            $ms = tv_interval($eventTime)*1000;
            $zCSRs{BE_UPDATE}->execute($STATUS_SKIPPED,$zTaskId,int($ms),undef,undef,$objid) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
            $opt_testing or $zdb->commit();
            $zStats{EVENT_SKIPPED} += 1;
            next;
        }
        
        if ($opt_only_mig and not $zAllAccountsRef->{$PREFIX_ACC . $accName}) {
            &zDebug('V','Skipping as this account is not migrated');
            # already processed this account. no need to update again. just update the record that is processed.
            $ms = tv_interval($eventTime)*1000;
            $zCSRs{BE_UPDATE}->execute($STATUS_NOT_MIGRATED,$zTaskId,int($ms),undef,undef,$objid) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
            $opt_testing or $zdb->commit();
            $zStats{ACCOUNT_SKIPPED} += 1;
            next;
        }
        
        
        # process the account
        $lProcessedAcc{$accName} = 1;
        $startTime = [gettimeofday];
        $zCSRs{CUST}->execute($accName) || &zdie(6001, __LINE__, __FILE__, $zdb_cly->errstr);
        $zStats{ACCOUNT_SQL} += tv_interval($startTime);
        $opt_perf && &zPerf('CLARIFY','ACCOUNT_EXEC',$accName, tv_interval($startTime));
        
        # Fetch account info. 
        $startTime = [gettimeofday];
        my $AccRow = $zCSRs{CUST}->fetchrow_hashref("NAME_uc");
        &zDebug('D',"Fetched ACCOUNT DATA: \n" . Dumper($AccRow));
        $opt_perf && &zPerf('CLARIFY','ACCOUNT_FETCH',$accName, tv_interval($startTime));
        
        &zInsertAudit('CUST', $objid, $AccRow, $ErrorsRef);

        $startDate = undef if (not $zAllAccountsRef->{$PREFIX_ACC . $accName});
        ($AccOrderRef, $ErrorsRef) = &zAccountOrderBuild($AccRow);
        my %Packet;
        $Packet{ACCOUNT} = $AccOrderRef;
        
        # Place all orders into the Packet to send to the child process for processing in the billing system. 
        $Packet{ACCOUNT_NAME} = $accName;
        $Packet{OBJID} = $objid;
        &zDebug('V',"Account Orders: \n" . Dumper(%Packet));
    
        if (@{$ErrorsRef}) {
            &zReportErrors($ErrorsRef, $eventTime, $objid);
        } else {
            &zDispatchOrder(\%Packet);
        }
        
    }
    
    ### Completed processing, now wait for the last executing children to return home
    &zWaitForChildren();
    
}

#***************************************************************************
# NAME:
#   zProcessProducts
#
# DESCRIPTION:
#   Processes all the changes in billing events that require a product update.  
#
# PARAMETERS:
#
# RETURNS:
#   Describe what the subroutine returns.
#***************************************************************************
sub zProcessProducts
{

    my ($accName, $objid, $prodRef, $startDate, $startTime, $eventTime, $ms, $ErrorsRef, @ProdOrders, %lProcessedProd);
    my ($prodOrderRef, $OneOffChargesRef);
        
    ### Process the events that have not been processed yet for account
    &zDebug('D','In zProcessProducts executing event fetch.');
    $zCSRs{PROD_EVENTS}->execute($STATUS_NEW,$zEffDate) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
    
    ### Loop through each top level product to build the orders for all the products. 
    while (my $RecRow = $zCSRs{PROD_EVENTS}->fetchrow_hashref("NAME_uc")) {
        undef($prodOrderRef); undef($OneOffChargesRef); undef($ErrorsRef);
        
        $objid = $RecRow->{OBJID};
        $accName = $RecRow->{X_BILL_SITE_ID};
        $prodRef = $RecRow->{X_PRODUCT_REF};
        $startDate = $RecRow->{START_DATE};
        $zStats{EVENT_CNT} += 1;
        $zCSRs{BE_STATUS}->execute($STATUS_PROGRESS,$objid) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
        $opt_testing || $zdb->commit();
        
        $eventTime = [gettimeofday];
        
        # Validation error. 
        if (length($prodRef) == 0) {
            $zCSRs{BE_UPDATE}->execute($STATUS_ERROR,$zTaskId,int($ms),undef,"<E90000> - Corrupt H3AT_PBE_EVENT_AUDIT table. X_PRODUCT_REF is required for X_OPERATION_TYPE = 15" ,$objid) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
            $opt_testing or $zdb->commit();
            $zStats{PRODUCT_FAIL} += 1;
            next;
        }

        if ($lProcessedProd{$prodRef}) {
            # already processed this account. no need to update again. just update the record that is processed.
            &zDebug('V','Skipping as this product is already processed by this task.');
            $ms = tv_interval($eventTime)*1000;
            $zCSRs{BE_UPDATE}->execute($STATUS_SKIPPED,$zTaskId,int($ms),undef,undef,$objid) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
            $opt_testing or $zdb->commit();
            $zStats{EVENT_SKIPPED} += 1;
            next;
        }
        
        if ($opt_only_mig and not $zAllAccountsRef->{$PREFIX_ACC . $accName}) {
            # product on account not migrated. Skip.
            &zDebug('V','Skipping as this account is not migrated');
            $ms = tv_interval($eventTime)*1000;
            $zCSRs{BE_UPDATE}->execute($STATUS_NOT_MIGRATED,$zTaskId,int($ms),undef,undef,$objid) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
            $opt_testing or $zdb->commit();
            $zStats{EVENT_SKIPPED} += 1;
            next;
        }
        
        $startTime = [gettimeofday];
        # If we are not in mock mode, we need to load the product data. 
        if (not $opt_mock) {
            &zDebug('D','Calling Product load SQL with product - ' . $prodRef);
            if (not $zCSRs{PROD_LOAD}->execute($prodRef)) {
                push(@{$ErrorsRef},"VALIDATION ERROR: Skipping product due to error with stored procedure to load product ($prodRef) data and CM data on account ($accName) - \nSQLERROR:" .  $zdb_cly->errstr);
                &zReportErrors($ErrorsRef, $eventTime, $objid);
                next;
            }
        }
        $opt_perf && &zPerf('CLARIFY','PROD_LOAD',$prodRef, tv_interval($startTime));
        
        # Fetch the product data for the provided reference. 
        $startTime = [gettimeofday];
        if (not $zCSRs{PROD}->execute($prodRef)) {
            push(@{$ErrorsRef},"VALIDATION ERROR: Skipping product due to error with product fetch of ($prodRef) data on account ($accName) - \nSQLERROR:" .  $zdb_cly->errstr);
            &zReportErrors($ErrorsRef, $eventTime, $objid);
            next;
        }
        $opt_perf && &zPerf('CLARIFY','PROD_EXEC',$prodRef, tv_interval($startTime));
        
        ### Loop through the products and build the orders including the CM's for each child.
        $startTime = [gettimeofday];
        my $PrdRow = $zCSRs{PROD}->fetchrow_hashref("NAME_uc");
        &zDebug('D',"Fetched PRODUCT DATA for ($prodRef): \n" . Dumper($PrdRow));
        $zStats{PROD_SQL} += tv_interval($startTime);
        $zStats{PROD_SQL_CNT} += 1;
        $opt_perf && &zPerf('CLARIFY','PROD_FETCH',$prodRef, tv_interval($startTime));
        
        &zInsertAudit('PROD', $objid, $PrdRow, $ErrorsRef);

        # Add some filters here to skip products that should NOT be created. 
        
        if ($PrdRow->{PRODUCT_STATUS} eq 'Active') {
            ($prodOrderRef, $OneOffChargesRef, $ErrorsRef) = &zProdOrderBuild($PrdRow);
        } elsif ($PrdRow->{PRODUCT_STATUS} eq 'Terminate') {
            ($prodOrderRef, $ErrorsRef) = &zProdCancelBuild($PrdRow);
        }
        
        &zDebug('D',"Adding product details: \n" . Dumper($prodOrderRef) . "\n");
        $OneOffChargesRef && &zDebug('D',"Adding charges details: \n" . Dumper($OneOffChargesRef) . "\n");
        $ProdOrders[0] = $prodOrderRef;
        
        # Add the account product if there is anything to add.
        if (%zAccProdOrder) {
            # Set the start date the same as the account. 
            push(@ProdOrders,\%zAccProdOrder);
        }
        
        my %Packet;
        $Packet{PRODUCTS} = \@ProdOrders;
        $Packet{CHARGES} = $OneOffChargesRef if ($OneOffChargesRef);
        $Packet{ACCOUNT_NAME} = $accName;
        $Packet{OBJID} = $objid;
        &zDebug('V',"Product Orders: \n" . Dumper(%Packet));
        
        if (@{$ErrorsRef}) {
            &zReportErrors($ErrorsRef, $eventTime, $objid);
        } else {
            &zDispatchOrder(\%Packet);
        }
    }
    
    ### Completed processing, now wait for the last executing children to return home
    &zWaitForChildren();
    
}

#***************************************************************************
# NAME:
#   zProcessAdjustments
#
# DESCRIPTION:
#   Processes all adjustments found not existing in the system. Do this last
#   To ensure the product exists.   
#
# PARAMETERS:
#
# RETURNS:
#   
#***************************************************************************
sub zProcessAdjustments
{
    my ($accName, $servName, $objid, $xObjId, $fields, $eventTime, $prodRef, @Errors);
    
    &zDebug('D',"Preparing SQL to get SERVICE_NAME");
    my $lCsrServName = $zdb->prepare('SELECT SERVICE_NAME FROM SERVICE_HISTORY WHERE NETWORK_NAME = ? ORDER BY EFFECTIVE_START_DATE DESC') || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
    
    ### Process the events that have not been processed yet for account
    
    $zCSRs{ADJ_EVENTS}->execute($STATUS_NEW,$zEffDate) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
    
    while (my $RecRow = $zCSRs{ADJ_EVENTS}->fetchrow_hashref("NAME_uc")) {
        $objid = $RecRow->{OBJID};
        $accName = $RecRow->{X_BILL_SITE_ID};
        $xObjId = $RecRow->{X_OBJECT_ID};
        $zStats{EVENT_CNT} += 1;
        
        &zDebug('D',"In zProcessAdjustments for ACCOUNT ($accName); objid ($objid); AJD_ID ($xObjId) ");
        # Set the event to be in progress, and record the starting timestamp
        $zCSRs{BE_STATUS}->execute($STATUS_PROGRESS,$objid) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
        $opt_testing or $zdb->commit();
        $eventTime = [gettimeofday];
        
        # Get the data for the adjustment
        $zCSRs{CUST_ADJ}->execute($accName,$xObjId) || &zdie(6001, __LINE__, __FILE__, $zdb_cly->errstr);
        my $AdjFields = $zCSRs{CUST_ADJ}->fetchrow_hashref("NAME_uc");
        if (not length($AdjFields->{PRODUCT_REF})) {
            push(@Errors,"ADJUSTMENT details not found for objid ($objid) account ($accName)");
            &zReportErrors(\@Errors, $eventTime, $objid);
            next;
        } else {
        
            $lCsrServName->execute('PBP'.$AdjFields->{PRODUCT_REF}) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
            my @ServRow = $lCsrServName->fetchrow_array();
            if (not length($ServRow[0])) {
                push(@Errors,"ADJUSTMENT - Unable to find service_name for PRODUCT_REF (".$AdjFields->{PRODUCT_REF}.") account ($accName)");
                &zReportErrors(\@Errors, $eventTime, $objid);
                next;
            } else {
                $servName = $ServRow[0];
                &zDebug('D',"Found SERVICE_NAME = $servName");
            }
        }
        
        # Add ADJ to create for account if any found. 
        $fields = &zAccountAdjBuild($AdjFields);
        
        my %Packet;
        
        # Place all orders into the Packet to send to the child process for processing in the billing system. 
        $Packet{ACCOUNT_NAME} = $accName;
        $Packet{SERVICE_NAME} = $servName;
        $Packet{OBJID} = $objid;
        $Packet{ADJUSTMENT} = $fields;
        &zDebug('V',"Adjustment Order: \n" . Dumper(%Packet));
    
        &zDispatchOrder(\%Packet);
        
    }
    
    ### Completed processing, now wait for the last executing children to return home
    &zWaitForChildren();
    
}

#***************************************************************************
# NAME:
#   zWaitForChildren
#
# DESCRIPTION:
#     Wait and handle the child output.  
#
# PARAMETERS:
#   None.
#
# RETURNS:
#   None.
#***************************************************************************
sub zWaitForChildren {
    
    my ($lPid, $lResult);
    
    # We have sent all requests. Now we need to wait for all children to return home 
    while (ataiFindBusyChild()) {
        # No free child processes - do blocking wait
        ($lPid, $lResult) = ataiWaitForBusyChild();
        &zProcessResult($lPid, $lResult);
    }

}

#***************************************************************************
# NAME:
#   zInitChild
#
# DESCRIPTION:
#     - Create TRE connection for the child process. 
#
# PARAMETERS:
#   None.
#
# RETURNS:
#   None.
#***************************************************************************
sub zInitChild {

    $znow = localtime;
    ### Open TRE connection
    &ataiTREConnect($opt_u) || &zdie($MSG_PASSTHRU, $errstr);
    $zChildWorkCnt = 0;
    
    # Open log file for logging debug and errors with data and processing.
    # Get the current hour, minute, second.
    if ($opt_debug or $opt_v) {
        my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime;
        my ($yyyymmddhhmiss) = (sprintf("%04d%02d%02d%02d%02d%02d", $year, $mon, $mday, $hour, $min, $sec));
        my $lFilename = $MODULE_NAME . "_".$$."_" . $yyyymmddhhmiss;
        $zLogFile = $ENV{ATA_DATA_SERVER_LOG} . '/' . $lFilename . '.log';
        open(LOGFILE,">",$zLogFile) or zdie($MSG_PASSTHRU,"Cannot open file $zLogFile"."'$errstr'");
    }
  
}

#***************************************************************************
# NAME:
#   zChildExit
#
# DESCRIPTION:
#   Cleanly shuts down the child Perl process.
#
# PARAMETERS:
#   None.
#
# RETURNS:
#   None.
#***************************************************************************
sub zChildExit {
    my ($lKey);

    $znow = localtime;
    # disconnect from the TRE.
    &ataiTREDisconnect() || &zdie($MSG_PASSTHRU, $errstr);
    if ($zChildWorkCnt == 0) {
        # Nothing done, so delete the log file. 
        unlink($zLogFile);
    } else {
        print "CHILD_".$$." shutdown with debug/verbose info written to file: $zLogFile\n";
    }

} # end zChildExit

#***************************************************************************
# NAME:
#   zSignalHandler
#
# DESCRIPTION:
#   Signal handler.
#
# PARAMETERS:
#   $l_Sig  - Signal name minus the SIG appendix (e.g. SIGKILL --> KILL)
#
# RETURNS:
#   None.
#***************************************************************************
sub zSignalHandler {
    my ($l_Sig) = @_;

    # Parent process receives a SIGTERM or SIGINT
    $opt_debug && print "Signal handler received the $l_Sig signal.\n";
    if (($l_Sig eq 'TERM') || ($l_Sig eq 'INT'))  {

        # Ignore SIGCHLD's from the shutting down of all other children as they are taken care of next
        $SIG{'CHLD'} = 'IGNORE';

        # Ignore SIGQUIT from TLM escalation for stopped tasks
        $SIG{'QUIT'} = 'IGNORE';

        # Kill off all remaining child processes
        ataiKillChildren('INT');

        exit (1);
    }
    elsif ($l_Sig eq 'CHLD')  {

        # Parent process receives a SIGTERM or SIGINT
        $opt_debug && print "Parent $$ received $l_Sig signal causing abnormal exit.\n";

        # Ignore signals while shutting down
        $SIG{'INT'}  = 'IGNORE';
        $SIG{'TERM'} = 'IGNORE';

        exit (1);
    }

} # end zSignalHandler

#***************************************************************************
# NAME:
#   zChildWork
#
# DESCRIPTION:
#   The child process sends orders to Singleview to be processed.  
#
# PARAMETERS:
#
# RETURNS:
#   Statistics hash for the parent to collate and report on. 
#***************************************************************************
sub zChildWork
{
    my $PacketRef = shift;
    
    &zDebug('D',"Processing Packet: \n" . Dumper($PacketRef));
    
    my (%lResponse, $fail, %result, $startTime, $childTime, $startDate, $cmRef, $lServName, $ms);
    my $AccOrderRef     = $PacketRef->{ACCOUNT};
    my $ProdOrdersRef   = $PacketRef->{PRODUCTS};
    my $AdjOrderRef     = $PacketRef->{ADJUSTMENT};
    my $accName         = $PacketRef->{ACCOUNT_NAME};
    my $servName        = $PacketRef->{SERVICE_NAME};
    my $ChargesRef      = $PacketRef->{CHARGES};
    $startDate = $PacketRef->{START_DATE} if ($PacketRef->{START_DATE});
    $zChildWorkCnt++;
    # Populate needed data to be sent back to parent about this child job
    $result{ACCOUNT_NAME} = $accName;
    $result{OBJID} = $PacketRef->{OBJID} if ($PacketRef->{OBJID});
    
    
    # All orders will be performed in a single transaction. 
    # The provided array should provide them in the order they are to be processed. 
    # Note that in normal processing only an account or a service request is sent, not all as per migrations. 
    
    # Start a transaction
    treBegin($zTransactionTime) || &zdie($MSG_PASSTHRU, "Failed to start TRE transaction with error: $errstr"); 
    $childTime = [gettimeofday];
    if ($AccOrderRef) {
        $result{ACTIVE_DATE} = $AccOrderRef->{'ACCOUNT_HISTORY'}[0]{'FromDate'};
        $result{STATS}{ACCOUNT_CNT} = 1;
        &zDebug('V',"fH3AT_CRM_MaintainAccount_RW Started for ($accName)");
        &zDebug('V',"fH3AT_CRM_MaintainAccount_RW called with: \n" . Dumper($AccOrderRef));
        $startTime = [gettimeofday];
        fH3AT_CRM_MaintainAccount_RW($AccOrderRef, \%lResponse);
        $opt_perf && &zPerf('SINGLEVIEW','API_ACCOUNT',$accName, tv_interval($startTime));
        $result{STATS}{API_ACC_SECS} += tv_interval($startTime);
        $result{STATS}{API_ACC_CNT} += 1;
        
        # handle tuxedo errors, by exit processing this account. 
        if ($errstr) {
            print "fH3AT_CRM_MaintainAccount_RW for ($accName) FAILED with error: $errstr with order: \n" . Dumper($AccOrderRef) . "\n";
            $result{ERROR_MESSAGE} = $errstr;
            undef($errstr);
            $fail = 1;
        } elsif ($opt_force_commit) {
            if ( not $opt_testing) {
                # Commit and create new transaction. 
                treCommit() || &zdie($MSG_PASSTHRU, "Failed to commit TRE transaction with error: $errstr");
                treBegin($zTransactionTime) || &zdie($MSG_PASSTHRU, "Failed to start TRE transaction with error: $errstr"); 
            }
        }
    }
    
    # Create any adjustments in the order.
    if (not $fail and defined($AdjOrderRef)) {
        # Send the creation of each adjustment to be created for this account to Singleview. 
        my $chgId;
        $startTime = [gettimeofday];
        &zDebug('V',"Calling fH3AT_CreateOneOffChg with: \n" . Dumper($AdjOrderRef));
        fH3AT_CreateOneOffChg($PREFIX_ACC.$accName, $servName, $AdjOrderRef, \$chgId);
        $result{STATS}{API_CHG_SECS} += tv_interval($startTime);
        $result{STATS}{API_CHG_CNT} += 1;
        $opt_perf && &zPerf('SINGLEVIEW','API_ADJUSTMENT',$chgId, tv_interval($startTime));
        
        # handle tuxedo errors, by exit processing this product. 
        if ($errstr) {
            print "API call to fH3AT_CreateOneOffChg for ($accName) failed with error: $errstr ";
            $fail = 1;
            $result{ERROR_MESSAGE} = $errstr;
            undef($errstr);
        }
    
    }
    
    # Maintain Service Flexi calls
    if (not $fail and defined($ProdOrdersRef)) {
    
        my @ProdOrders = @$ProdOrdersRef;
        for my $prodOrderRef (@ProdOrders) {
            next if (not defined($prodOrderRef));
            # Loop the product creations API calls now.
            $result{STATS}{PRODUCT_CNT} += 1;
            &zDebug('V',"fH3AT_CRM_MaintainServiceFlexi Started for (" . $prodOrderRef->{SERVICE_HISTORY}[0]{SERVICE}{SERVICE_NAME} . ')');
            &zDebug('V',"fH3AT_CRM_MaintainServiceFlexi called with: \n" . Dumper($prodOrderRef));
            $startTime = [gettimeofday];
            undef(%lResponse);
            fH3AT_CRM_MaintainServiceFlexi_RW($prodOrderRef, \%lResponse);
            $result{STATS}{API_PROD_SECS} += tv_interval($startTime);
            $result{STATS}{API_PROD_CNT} += 1;
            $opt_perf && &zPerf('SINGLEVIEW','API_SERVICE',$accName, tv_interval($startTime));
            
            # handle tuxedo errors, by exit processing this product. 
            if ($errstr) {
                print "API call to fH3AT_CRM_MaintainServiceFlexi for ($accName) failed with error: $errstr with order: \n" . Dumper($prodOrderRef) . "\n";
                $fail = 1;
                $result{ERROR_MESSAGE} = $errstr;
                undef($errstr);
                last;
            } elsif ($opt_force_commit) {
                if ( not $opt_testing) {
                    # Commit and create new transaction. 
                    treCommit() || &zdie($MSG_PASSTHRU, "Failed to commit TRE transaction with error: $errstr");
                    treBegin($zTransactionTime) || &zdie($MSG_PASSTHRU, "Failed to start TRE transaction with error: $errstr"); 
                }
            }
        }
    }
    
    
    
    # On error report it and rollback transaction
    if ($fail) {
        &zDebug('V',"ERROR: Rolling back changes made due to error.");
        treRollback() || &zdie($MSG_PASSTHRU, "Failed to rollback TRE transaction with error: $errstr"); 
        $result{STATUS} = 'ERROR';
        if ($opt_force_commit) {
            print("ERROR during migration of account: $accName. Customer is partially commited and should be purged before new attempt!. \n");
        }
    } else { 
        
    
        # debug mode we make no changes to DB
        if ($opt_testing) {
            # rollback in debug mode
            &zDebug('D',"Rolling back transaction in testing mode. ");
            treRollback() || &zdie($MSG_PASSTHRU, "Failed to rollback TRE transaction with error: $errstr");

        } else {  
            # Commit the changes in normal mode. 
            treCommit() || &zdie($MSG_PASSTHRU, "Failed to commit TRE transaction with error: $errstr");
        }
        
        
        # One-off charge createion calls. 
        if (defined($ChargesRef)) {
        
            # Now we create one off charges. 
            # Note in testing mode, don't make product changes and one-off charges to rely on those changes as it will be rolled back. 
            # Start a transaction
            treBegin($zTransactionTime) || &zdie($MSG_PASSTHRU, "Failed to start TRE transaction with error: $errstr"); 
        
            &zDebug('V',"One off charge for called with: \n" . Dumper($ChargesRef) . "\n");
            my %Charges = %$ChargesRef;
            foreach $cmRef (keys %Charges) {
            
                # Loop the product creations API calls now.
                
                $startTime = [gettimeofday];
                undef(%lResponse);
                my $chgId;
                $lServName = $ChargesRef->{$cmRef}->{SERVICE_NAME};
                $accName = $ChargesRef->{$cmRef}->{ACCOUNT_NAME};
                fH3AT_CreateOneOffChg($accName, $lServName, $ChargesRef->{$cmRef}->{OneOffCharges}, \$chgId);
                $result{STATS}{API_CHG_SECS} += tv_interval($startTime);
                $result{STATS}{API_CHG_CNT} += 1;
                &zDebug('D',"Response from API call: \n " . Dumper(%lResponse));
                $opt_perf && &zPerf('SINGLEVIEW','API_ONEOFF',$chgId, tv_interval($startTime));
                
                # handle tuxedo errors, by exit processing this product. 
                if ($errstr) {
                    print "API call to fH3AT_CreateOneOffChg for ($accName) failed with error: $errstr ";
                    $fail = 1;
                    $result{ERROR_MESSAGE} = $errstr;
                    undef($errstr);
                    last;
                }
            }
            # Handle oneoff results. 
            if ($fail) {
                &zDebug('V',"Rolling back changes made due to error.");
                treRollback() || &zdie($MSG_PASSTHRU, "Failed to rollback TRE transaction with error: $errstr"); 
                $result{STATUS} = 'ERROR';
            # debug mode we make no changes to DB
            } elsif ($opt_testing) {
                # rollback in debug mode
                &zDebug('D',"Rolling back transaction in testing mode. ");
                treRollback() || &zdie($MSG_PASSTHRU, "Failed to rollback TRE transaction with error: $errstr");

            } else {  
                # Commit the changes in normal mode. 
                treCommit() || &zdie($MSG_PASSTHRU, "Failed to commit TRE transaction with error: $errstr");
            }
        }
    }
    $ms = tv_interval($childTime) * 1000;
    $result{TIME_MS} = int($ms);
    return \%result;
    
}

#***************************************************************************
# NAME:
#   zDebug
#
# DESCRIPTION:
#   Prints debug messages
#
# PARAMETERS:
#
# RETURNS:
#   
#***************************************************************************
sub zDebug
{
    my $mode = shift;
    my $msg = shift;
    my $now = localtime;
    
    if ($mode eq 'D') {
        return 0 if (not $opt_debug); # Debug Mode
        $msg = 'DEBUG: '.$msg;
    } elsif ($mode eq 'V') {
        return 0 if (not $opt_v); # Verbose Mode
    }
    
    print LOGFILE "$now - $msg\n";
    
    return 1;
    
}

#***************************************************************************
# NAME:
#   zPerf
#
# DESCRIPTION:
#   Prints performance info messages to find issues. 
#
# PARAMETERS:
#
# RETURNS:
#   
#***************************************************************************
sub zPerf
{
    my $system = shift;
    my $desc = shift;
    my $instId = shift;
    my $secs = shift;
    my $now;
    my $ms_limit = ($system eq 'SINGLEVIEW') ? 3000 : $opt_perf;
       
    if ($secs*1000 > $ms_limit) {
        $now = localtime;
        printf("%s - %15s - %15s - (%s) - %.0f ms \n",($now, $system,$desc,$instId, $secs*1000));
    }
    
}

#***************************************************************************
# NAME:
#   zToSvDate
#
# DESCRIPTION:
#   With a date, converts it to a SV Date. assume input DD.MM.YYYY -> DD-MM-YYYY HH24:MI:SS
#
# PARAMETERS:
#   $date 
#   $dayIncrement - Number od days to move the date. negative goes backwards. 
#   $cancelFlag if 1 then we add 23:59:59 to date before return. 
#
# RETURNS:
#   0 on success
#***************************************************************************
sub zToSvDate
{
    my $date = shift;
    my $incDays = shift;
    my $cancelFlag = shift;
    
    my $timeStr = ' 00:00:00';
    
    if ($cancelFlag == 1) {
        $timeStr = ' 23:59:59';
    }
    
    # replace . with - for singleview. 
    $date =~ s/\./-/g;
    
    # Increase / decrease the provided date if needed.
    if ($incDays) {
        my $format = ( length($date == 10) ) ? 'DD-MM-YYYY' : 'DD-MM-YYYY HH24:MI:SS';
        $zCSRs{DATE_INC}->execute($date,$format, $incDays,$format ) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
        my @data = $zCSRs{DATE_INC}->fetchrow_array();
        $date = $data[0];
    }
    
    # Only return dates => migration date. 
    $date =~ m/^(\d\d).(\d\d).(\d\d\d\d).*/;
    if ($zMigSortDate*1 gt "$3$2$1"*1) {
        return $zMigrationDate.$timeStr;
    }
    
    return substr($date,0,10) . $timeStr;
    
}

#***************************************************************************
# NAME:
#   zCompareDates
#
# DESCRIPTION:
#   deterimines if first date is before (-1), same (0) or later (1) than the second date. Both need to be in the same format as provided. 
#   Both dates should be in the same format. DD-MM-YYYY or DD-MM-YYYY HH24:MI:SS as supported. 
#
# PARAMETERS:
#   $date1 - first date
#   $date2 - second date
#
# RETURNS:
#   -1 first date < second date
#   0 dates are the same
#   1 first date > second date
#***************************************************************************
sub zCompareDates
{
    my $date1 = shift;
    my $date2 = shift;
    
    my ($sortDate1, $sortDate2);
    
    if (length($date1) == 10) {
        $date1 =~ m/^(\d\d).(\d\d).(\d\d\d\d).*/;
        $sortDate1 = "$3$2$1";
        $date2 =~ m/^(\d\d).(\d\d).(\d\d\d\d).*/;
        $sortDate2 = "$3$2$1";
    } elsif (length($date1) == 19) {
        $date1 =~ m/^(\d\d).(\d\d).(\d\d\d\d).(\d\d).(\d\d).(\d\d).*/;
        $sortDate1 = "$3$2$1$4$5$6";
        $date2 =~ m/^(\d\d).(\d\d).(\d\d\d\d).(\d\d).(\d\d).(\d\d).*/;
        $sortDate2 = "$3$2$1$4$5$6";
    }
    
    &zDebug('D',"Comparing dates - $date1 ($sortDate1) with $date2 ($sortDate2)");
    
    if ($sortDate1 < $sortDate2) {
        return -1;
    } elsif ($sortDate1 > $sortDate2) {
        return 1;
    }
    return 0;
}


#***************************************************************************
# NAME:
#   zCacheRefData
#
# DESCRIPTION:
#   Stores reference type data needed into global hash structure. 
#
# PARAMETERS:
#   <RT_LABEL_NAME> - The reference type label name that is to be cached
#
# RETURNS:
#   Describe what the subroutine returns.
#***************************************************************************
sub zCacheRefData
{
    my $l_labelName = shift;
    my $now = localtime;
    
    # Fetch and store all legal entities. 
    my (@lFieldValues, $lrow);
    my @fieldNames = ("REFERENCE_CODE","ABBREVIATION");
    
    &biReferenceCodeFetchByName($l_labelName,\@fieldNames, \@lFieldValues);
    zdie($MSG_PASSTHRU, $errstr) if ($errstr);
    
  
    foreach $lrow (@lFieldValues) {
        $zRefData{$l_labelName}{$lrow->[0]} = $lrow->[1];
        $zRefData{$l_labelName}{CODES}{$lrow->[1]} = $lrow->[0];
    }  
    
    #$opt_debug && print("Loaded Referendce Type: $l_labelName \n");
    
}

#***************************************************************************
# NAME:
#   zAccountOrderBuild
#
# DESCRIPTION:
#   Builds the order to send to MaintainAccount for Create/Update
#
# PARAMETERS:
#   $lAccRef - Account selected data
#
# RETURNS:
#   AccountOrder Hash
#***************************************************************************
sub zAccountOrderBuild
{
    my $lAccRef  = shift;
    
    my (%lAccountOrder, %lAccountDetails, %lContact, %lResponse, %lAddress, %lSiteAddress, %lInvFrmt, @Errors);
    
    &zDebug('D',"Building Account order with data: \n" . Dumper($lAccRef) . "\n");
    my $accName = $lAccRef->{CUSTOMER_NODE_REF};
    
    # ltrim and rtrim every account field. 
    foreach my $key (keys %$lAccRef) {
        $lAccRef->{$key} =~ s/^\s+|\s+$//g;
    }
    
    # Build the account maintain Order.
    if ($opt_migration) {
        $lAccountOrder{MIGRATION_DATE}                  = $zMigrationDate; # The date the account has been billed up to. 
    }
    $lAccountOrder{SOURCE_SYSTEM}                       = $SOURCE;
    $lAccountOrder{ExternalAccountNumber}               = $PREFIX_ACC . $lAccRef->{CUSTOMER_NODE_REF};
    #$lAccountOrder{ACCOUNT_HISTORY}[0]{DeactivateFlag}  = $FALSE;
    if (not $zAllAccountsRef->{$PREFIX_ACC . $accName}) {
        $lAccountOrder{ACCOUNT_HISTORY}[0]{FromDate}        = &zToSvDate($lAccRef->{START_DATE});
    } else {
        $lAccountOrder{ACCOUNT_HISTORY}[0]{FromDate}        = &zToSvDate($lAccRef->{CHANGE_DATE});
    }
    $lAccountOrder{ACCOUNT_HISTORY}[0]{ToDate}          = $MAX_DATE;
    
    
    # Build account history structure
    $lAccountDetails{ACCOUNTING_METHOD}             = $OPEN_ITEM;
    $lAccountDetails{ACCOUNT_NAME}                  = $lAccRef->{CUSTOMER_NODE_NAME}; 
    $lAccountDetails{ACCOUNT_STATUS_NAME}           = 'Active'; 
    $lAccountDetails{EXTERNAL_CUSTOMER_ID}          = $lAccRef->{CRM_ACCOUNT_ID};
    $lAccountDetails{ACCOUNT_TYPE_NAME}             = $ACCOUNT_TYPE_NAME;
    $lAccountDetails{BILL_CYCLE_ID}                 = ($lAccRef->{CUSTOMER_CLASSIFICATION} eq 'RDL') ? $zBR_Reseller : $zBR_Business;
    $lAccountDetails{CREDIT_LIMIT}                  = 100000; #TODO
    $lAccountDetails{CURRENCY_ID}                   = $CURRENCY_ID;
    $lAccountDetails{CUSTOMER_CLASSIFICATION_NAME}  = ($lAccRef->{CUSTOMER_CLASSIFICATION}) ? $lAccRef->{CUSTOMER_CLASSIFICATION} : 'Standard';
    $lAccountDetails{INVOICE_FORMAT_NAME}           = $INV_FORMAT;
    $lAccountDetails{PAYMENT_METHOD_NAME}           = ($lAccRef->{PAYMENT_METHOD} eq 'Direct Debit') ? $PAY_METHOD_DD : $PAY_METHOD_GIRO;
    $lAccountDetails{REPORT_LEVEL_NAME}             = "Invoice"; 
    $lAccountDetails{SHADOW_CREDIT_LIMIT}           = 50000;
    $lAccountDetails{TAX_CLASS_CODE}                = ($lAccRef->{TAX_CLASS} eq "Taxable") ? $TAX_BUS_20 : $TAX_BUS_0; 
    $lAccountDetails{SALES_CHANNEL}                 = $lAccRef->{SALES_CHANNEL};
    
    
    ### Placed on the new Account Information list
    $lAccountDetails{CUSTOMER_SEGMENT}             = $lAccRef->{CUSTOMER_SEGMENT}               if length($lAccRef->{CUSTOMER_SEGMENT});
    $lAccountDetails{CUSTOMER_SUB_CLASSIFICATION}  = $lAccRef->{CUSTOMER_SUB_CLASSIFICATION}    if length($lAccRef->{CUSTOMER_SUB_CLASSIFICATION});
    $lAccountDetails{ICT_TAG}                      = $lAccRef->{ICT_TAG}                        if length($lAccRef->{ICT_TAG});
    $lAccountDetails{GLOBAL_ONNET_FLAG}            = ( $lAccRef->{GLOBAL_ONNET} == 1 ) ? $YES : $NO; 
    $lAccountDetails{PAYMENT_DUE_PERIOD}           = $lAccRef->{PAYMENT_DUE_PERIOD}             if length($lAccRef->{PAYMENT_DUE_PERIOD});
    $lAccountDetails{UID_NUMBER}                   = $lAccRef->{UID_NUMBER}                     if length($lAccRef->{UID_NUMBER});
    $lAccountDetails{SITE_NAME}                    = $lAccRef->{SITE_NAME}                      if length($lAccRef->{SITE_NAME});
    $lAccountDetails{SITE_CONTACT_NAME}            = $lAccRef->{SITE_CONTACT_NAME}              if length($lAccRef->{SITE_CONTACT_NAME});
    $lAccountDetails{BANK_ACCOUNT_NUMBER}          = $lAccRef->{IBAN}                           if length($lAccRef->{IBAN});
    $lAccountDetails{BANK_CODE}                    = $lAccRef->{BIC}                            if length($lAccRef->{BIC});
    $lAccountDetails{TITLE_STRING}                 = $lAccRef->{TITLE} if ($lAccRef->{TITLE});
    $lAccountDetails{CALL_MASKING} = ($lAccRef->{CALL_MASKING} eq 'Masked') ? 'Masked' : 'UnMasked'  if ($lAccRef->{CALL_MASKING});
    
    
    ### Add customer objects data. 
    $zCSRs{CUST_OBJECTS}->execute($accName);
    if ($zdb_cly->errstr) { 
        print "ERROR: Skipping account due to error with view EITT_CUSTOMER_OBJECTS on account ($accName) \nSQLERROR: " . $zdb_cly->errstr . "\n";
        undef($zdb_cly->errstr);
        return undef;
    }
    
    # can be multiple entries, handle these. 
    while (my $RecRow = $zCSRs{CUST_OBJECTS}->fetchrow_hashref("NAME_uc")) {
        my ($prefix, $setNull);
        $setNull = 0;
        
        &zInsertAudit('CUST_OBJECTS', $zTaskId, $RecRow, \@Errors);
        
        # Check the type of customer object. 
        if (substr($RecRow->{OBJECT_CODE},0,3) eq 'EVA') {
            $prefix = 'EVA_';
        } elsif ($RecRow->{OBJECT_CODE} eq 'ELFE') {
            $setNull = 1 if ($lAccRef->{INVOICE_MEDIA} eq 'Paper');
            $prefix = 'ELFE_';
        } elsif ($RecRow->{OBJECT_CODE} eq 'EBILL') {
            $setNull = 1 if ($lAccRef->{INVOICE_MEDIA} eq 'Paper');
            $prefix = 'EBILL_';
        } else {
            print "ERROR: Unkown CUSTOMER_OBJECT CODE - " . $RecRow->{OBJECT_CODE} . "\n";
        }
        &zDebug('D',"Processing Customer Object - " . $RecRow->{OBJECT_CODE});
        if (defined($prefix)) {
            if ($setNull) {
                $lAccountDetails{$prefix.'EDI_FORMAT'}             = '';
                $lAccountDetails{$prefix.'NOTIFICATION_LIST'}     = '';
                $lAccountDetails{$prefix.'DELIVERY_LIST'}         = '';
                $lAccountDetails{$prefix.'DELIVERY_METHOD'}     = '';
            } else {
                $lAccountDetails{$prefix.'EDI_FORMAT'}             = $RecRow->{EDI_FORMAT};
                $lAccountDetails{$prefix.'NOTIFICATION_LIST'}     .= (length($lAccountDetails{$prefix.'NOTIFICATION_LIST'}) > 0) ? ',' . $RecRow->{NOTIFICATION_LIST} : $RecRow->{NOTIFICATION_LIST};
                $lAccountDetails{$prefix.'DELIVERY_LIST'}         .= (length($lAccountDetails{$prefix.'DELIVERY_LIST'}) > 0) ? ',' . $RecRow->{DELIVERY_LIST} : $RecRow->{DELIVERY_LIST};
                $lAccountDetails{$prefix.'DELIVERY_METHOD'}     = $RecRow->{DELIVERY_METHOD};
            }
        }
        
    }
    
    # Site Address: 
    undef(%lSiteAddress);
    $lSiteAddress{LINE1} = $lAccRef->{SITE_LINE_1};
    $lSiteAddress{LINE2} = $lAccRef->{SITE_LINE_2} if ($lAccRef->{SITE_LINE_2});
    $lSiteAddress{LINE3} = $lAccRef->{SITE_CITY};
    $lSiteAddress{LINE6} = $lAccRef->{SITE_COUNTRY};
    $lSiteAddress{LINE7} = $lAccRef->{SITE_POST_CODE};
    $lAccountDetails{POSTAL_ADDRESS} = \%lSiteAddress;
    
    # Invoice Format details
    $lInvFrmt{NOT_SEND_FLAG}        = $lAccRef->{INVOICE_NOT_SEND} if ($lAccRef->{INVOICE_NOT_SEND});
    $lInvFrmt{NOT_SEND_AGENT}       = $lAccRef->{AGENT} if ($lAccRef->{AGENT});
    $lInvFrmt{NOT_SEND_REASON}      = $lAccRef->{REASON} if ($lAccRef->{REASON});
    $lInvFrmt{INVOICE_SORTING}      = $zInvSorting{$lAccRef->{INVOICE_SORTING}};
    $lInvFrmt{DSS_TCS}              = ( $lAccRef->{DSS_TCS} eq 'True') ? $YES : $NO;
    $lInvFrmt{CALL_DETAIL_OPTION}   = $lAccRef->{CALL_DETAILS};
    $lInvFrmt{INVOICE_MEDIA}        = $lAccRef->{INVOICE_MEDIA};
    $lAccountDetails{CUST_INV_FORMAT} = \%lInvFrmt;
    
    # Contact details
    $lAddress{LINE1} = $lAccRef->{POSTAL_LINE_1};
    $lAddress{LINE2} = $lAccRef->{POSTAL_LINE_2} if ($lAccRef->{POSTAL_LINE_2});
    $lAddress{LINE3} = $lAccRef->{POSTAL_CITY};
    $lAddress{LINE6} = $lAccRef->{POSTAL_COUNTRY};
    $lAddress{LINE7} = $lAccRef->{POSTAL_POST_CODE};
    $lContact{POSTAL_ADDRESS} = \%lAddress;
    $lContact{CONTACT_STATUS_CODE}      = $CONTACT_STATUS_CODE; 
    $lContact{CONTACT_TYPE_ID}          = $CONTACT_TYPE_ID;
    
    $lContact{FAMILY_NAME}              = $lAccRef->{FAMILY_NAME} if ($lAccRef->{FAMILY_NAME}); 
    $lContact{GIVEN_NAMES}              = $lAccRef->{GIVEN_NAMES} if ($lAccRef->{GIVEN_NAMES});
    $lContact{OFFICIAL_NAME}            = $lAccRef->{OFFICIAL_NAME};
    $lContact{WRITTEN_LANGUAGE_CODE}    = $zRefData{'WRITTEN_LANGUAGE'}{CODES}{length($lAccRef->{INVOICE_LANGUAGE}) > 0 ? $lAccRef->{INVOICE_LANGUAGE} : 'German'}*1;
    $lContact{EMAIL_ADDRESS}            = $lAccRef->{EMAIL_ADDRESS} if ($lAccRef->{EMAIL_ADDRESS});
    $lContact{PREFERRED_CONTACT_METHOD} = 21;
    
    # Handle phone numbers. If any number is more than 30 characters, then we set home number to UNKNOWN. also in case of no numbers are provided. 
    if ( (length($lAccRef->{HOME_PHONE_NR}) <= 30 and length($lAccRef->{MOBILE_PHONE_NR}) <= 30 and length($lAccRef->{FAX_PHONE_NR}) <= 30) and 
          (length($lAccRef->{HOME_PHONE_NR}) + length($lAccRef->{MOBILE_PHONE_NR}) + length($lAccRef->{FAX_PHONE_NR}) != 0 ) ) {
        $lContact{PHONE_NR}         = $lAccRef->{HOME_PHONE_NR};
        $lContact{MOBILE_PHONE_NR}  = $lAccRef->{MOBILE_PHONE_NR};
        $lContact{FAX_PHONE_NR}     = $lAccRef->{FAX_PHONE_NR}; 
    } else {
        $lContact{PHONE_NR}         = 'UNKNOWN';
        $lContact{MOBILE_PHONE_NR}  = '';
        $lContact{FAX_PHONE_NR}     = '';
    }
    if ( length($lAccRef->{FAMILY_NAME}.$lAccRef->{GIVEN_NAMES}) == 0) {
        $lContact{OFFICIAL_NAME}    = 'NA';
        $lContact{PHONE_NR}         = 'NA';
    }
    
    $lAccountOrder{ACCOUNT_HISTORY}[0]{ACCOUNTDETAILS} = \%lAccountDetails;
    $lAccountOrder{ACCOUNT_HISTORY}[0]{PRIMARYCONTACT} = \%lContact;
    
    # return the order to be used by the requester.
    return (\%lAccountOrder, \@Errors);
    
}

#***************************************************************************
# NAME:
#   zProdOrderBuild
#
# DESCRIPTION:
#   Builds the order for the provided product ref. 
#
# PARAMETERS:
#
# RETURNS:
#   Order that can be se sent to MaintainFlexiService API
#***************************************************************************
sub zProdOrderBuild
{
    my $l_PrdData = shift;
    my $l_ChargesRef = shift;
    my (%lProductOrder, %OfferDetails, %OneOffCharges, %Products, %RecCharges, %RecDiscounts, %ServHistory, %FlexiProducts, %CmData, @Errors);
    my ($prodRef, $startTime, $accName, $cmType, $prdSortDate, $field, $errStr);
    my $lNoProduct = 0;
    
    # output the provided data
    $accName = $l_PrdData->{CUSTOMER_NODE_REF}.''; 
    $prodRef = $l_PrdData->{PRODUCT_REF}.'';
    
    # Build create structure: 
    $lProductOrder{SOURCE_SYSTEM}         = $SOURCE;
    $lProductOrder{ExternalAccountNumber} = $PREFIX_ACC.$accName; 
    $lProductOrder{ExternalServiceNumber} = $PREFIX_PROD . $prodRef;
    
    # Offer
    $OfferDetails{FlexiOfferIdentifier}         = $PREFIX_PROD . $prodRef;
    $OfferDetails{FlexiPackageClassification}   = 'H3AT_pGenericBusPackage';
    $OfferDetails{FlexiTariffNameOnInvoice}     = $l_PrdData->{PRODUCT_NAME};
    if ($opt_migration) {
        $OfferDetails{StartDate}                = &zToSvDate($l_PrdData->{START_DATE});
    } else {
        # if the Eff BIL date TODO
        $OfferDetails{StartDate}                = &zToSvDate($l_PrdData->{START_DATE});
    }
    
    if ($opt_migration) {
        # Only return dates => migration date. 
        my $prdStartDate = $l_PrdData->{START_DATE};
        $prdStartDate =~ m/^(\d\d).(\d\d).(\d\d\d\d).*/;
        $prdSortDate = "$3$2$1"*1;
    }
    
    # Other Business product fields
    $OfferDetails{ParentOfferId}            = $PREFIX_PROD.$l_PrdData->{CRM_PRODUCT_PARENT}  if ($l_PrdData->{CRM_PRODUCT_PARENT});
    $OfferDetails{ProductGroup}             = $l_PrdData->{PRODUCT_GROUP}               if ($l_PrdData->{PRODUCT_GROUP});
    $OfferDetails{ProductRefCode}           = $l_PrdData->{PRODUCT_REF_CODE}            if ($l_PrdData->{PRODUCT_REF_CODE});
    $OfferDetails{ProductName}              = $l_PrdData->{PRODUCT_NAME}                if ($l_PrdData->{PRODUCT_NAME});
    $OfferDetails{ServiceInfo}              = $l_PrdData->{SERVICE_INFO}                if ($l_PrdData->{SERVICE_INFO});
    $OfferDetails{NoticeOnInvoice}          = $l_PrdData->{NOTICE_ON_INVOICE}           if ($l_PrdData->{NOTICE_ON_INVOICE});
    $OfferDetails{ProductClass}             = $l_PrdData->{PRODUCT_CLASS}               if ($l_PrdData->{PRODUCT_CLASS});
    $OfferDetails{TechSite}                 = $l_PrdData->{TECHNICAL_SITE}              if ($l_PrdData->{TECHNICAL_SITE});
    $OfferDetails{ServiceDisplayName}       = $l_PrdData->{SERVICE_DISPLAY_NAME}        if ($l_PrdData->{SERVICE_DISPLAY_NAME});
    $OfferDetails{EdiId}                    = $l_PrdData->{EDI_ID}                      if ($l_PrdData->{EDI_ID});
    $OfferDetails{AlwaysDisplay}            = $l_PrdData->{DISPLAY_ALWAYS}              if ($l_PrdData->{DISPLAY_ALWAYS});      # No / Yes
    
    # Service details.
    $lProductOrder{SERVICE_HISTORY}[0]{FromDate} = &zToSvDate($l_PrdData->{START_DATE});
    $lProductOrder{SERVICE_HISTORY}[0]{SERVICE}{NETWORK_NAME}   = $PREFIX_PROD.$prodRef; # Use networkd name with CRM_REF as fast way to find it. 
    
    # Add the rating product if this is a usage service. The product is the rate plan version. 
    if ($l_PrdData->{SERVICE_NAME} and $l_PrdData->{SERVICE_NAME} ne $prodRef) {
        my $servName = $l_PrdData->{SERVICE_NAME};
        $servName =~ s/\///g; # Remove / from the provided SERVICE_NAME
        # This is a usage service and hence validate all mandatory parameters are provided for this. 
        foreach my $field (@zServMandFields) {
            if (not length($l_PrdData->{$field})) {
                push(@Errors,"Usage Service is missing mandatory field ($field) for product ($prodRef) on account ($accName)");
            }
        }
        
        # Default the rate plan if not provided. 
        $l_PrdData->{RATE_PLAN} = 'dTT_RAT_RP_BUSINESS_DIRECT_DIAL_TONE' if (length($l_PrdData->{RATE_PLAN}) == 0);
        
        $lProductOrder{SERVICE_HISTORY}[0]{SERVICE}{SERVICE_CLASS}  = $l_PrdData->{SERVICE_CLASS};
        $OfferDetails{OnnetFlag}                = ($l_PrdData->{ONNET_SERVICE}) ? $l_PrdData->{ONNET_SERVICE} : $NO;
        $OfferDetails{VPNFlag}                  = ($l_PrdData->{VPN_SERVICE}) ? $l_PrdData->{VPN_SERVICE} : $NO;
        $OfferDetails{RateClassification}       = $l_PrdData->{RATE_CLASSIFICATION} if (length($l_PrdData->{RATE_CLASSIFICATION}));
        $OfferDetails{FinanceCode}              = $l_PrdData->{CO_CODE}.':'.$l_PrdData->{FI_CODE};
        
        # Number region plans are the rate plans in ARGOS with the prefix 'dTT_RAT_RP_' removed. 
        $lProductOrder{SERVICE_HISTORY}[0]{SERVICE}{SERVICE_NAME} = $servName;
        $lProductOrder{SERVICE_HISTORY}[0]{SERVICE}{NUMBER_REGION_TYPE} = $FIXED_NUMBER_PLAN;
        $lProductOrder{SERVICE_HISTORY}[0]{SERVICE}{BUSINESS_RATEPLAN} = substr($l_PrdData->{RATE_PLAN},length('dTT_RAT_RP_'));
        # Add the rateplan product if it exists. 
        my $prodName = 'H3ATT_RP_' . substr($l_PrdData->{RATE_PLAN},length('dTT_RAT_RP_'));
        
        if ($zAllProdNameRef->{$prodName}) {
            $FlexiProducts{ProductName} = $prodName;
            $FlexiProducts{StartDate} = &zToSvDate($l_PrdData->{START_DATE});
            $lProductOrder{FlexibleAttributes}{FlexiProducts}[0] = \%FlexiProducts;
        } else {
            &zDebug('V',"RP - $prodName is not configured, skipping creation");
        }
    } else {
        $lProductOrder{SERVICE_HISTORY}[0]{SERVICE}{NUMBER_REGION_TYPE} = $NON_USAGE_NUMBER_PLAN;
        $lProductOrder{SERVICE_HISTORY}[0]{SERVICE}{SERVICE_NAME} = $PREFIX_PROD.$prodRef;
    }
    
    $lProductOrder{FlexibleAttributes}{FlexiOfferDetails}[0] = \%OfferDetails;
    
    # Add into the order any recurring charges and discounts.
    $startTime = [gettimeofday];
    &zDebug('D','Calling RCD SQL with product ref - ' . $prodRef);
    
    $zCSRs{PROD_CM}->execute($prodRef);
    if ($zdb_cly->errstr) { 
        print "ERROR: Skipping account due to error with stored proceedure to load Product / CM data for Product ref - ($prodRef) on account ($accName) \nSQLERROR: " . $zdb_cly->errstr . "\n";
        undef($zdb_cly->errstr);
        return undef;
    }
    $opt_perf && &zPerf('CLARIFY','PROD_CM_EXEC',$prodRef, tv_interval($startTime));
    $zStats{CM_SQL} += tv_interval($startTime);
    $zStats{CM_SQL_CNT} += 1;
    
    ### Store all CM details into a complex hash structure:
    $startTime = [gettimeofday];
    while (my $RecRow = $zCSRs{PROD_CM}->fetchrow_hashref("NAME_uc")) {
        
        
        if ( defined $RecRow->{CM_CODE} and not(exists($excludedCM_CODEs{$RecRow->{CM_CODE}}))) {
            &zInsertAudit('CM', $zTaskId , $RecRow , \@Errors);
        }
        
        $CmData{$RecRow->{CM_CODE}}{$RecRow->{CM_REF}}{$RecRow->{SV_PARAMETER}} = $RecRow->{SV_VALUE};
        
        if ($opt_migration) {
            # Ensure the child date is not before the parent date. 
            if ($RecRow->{SV_PARAMETER} eq 'START_DATE') {
                my $childStart = $RecRow->{SV_VALUE};
                $childStart =~ m/^(\d\d).(\d\d).(\d\d\d\d).*/;
                my $childSortDate = "$3$2$1"*1;
                if ($prdSortDate > $childSortDate) {
                    $prdSortDate = $childSortDate;
                    $lProductOrder{SERVICE_HISTORY}[0]{FromDate} = &zToSvDate($RecRow->{SV_VALUE});
                    $lProductOrder{FlexibleAttributes}{FlexiOfferDetails}[0]{StartDate} = &zToSvDate($RecRow->{SV_VALUE});
                    if ( exists($lProductOrder{FlexibleAttributes}{FlexiProducts}) ) {
                        $lProductOrder{FlexibleAttributes}{FlexiProducts}[0]{StartDate} = &zToSvDate($RecRow->{SV_VALUE});
                    }
                }
            }
        }
    }
    $opt_perf && &zPerf('CLARIFY','PROD_CM_FETCH',$prodRef, tv_interval($startTime));
    
    # Send all 
    my ($cmRef, $entCnt, %lFields);
    &zDebug('D',"Received CM DATA: \n" . Dumper(%CmData));
    
    # Process every type of charge mechanism. 
    foreach $cmType (keys %CmData) {
        undef(%lFields);
        if ($cmType eq 'RCD') {
        
            # process each recurring charge on the product. Only service level supported. 
            foreach $cmRef (keys %{$CmData{$cmType}}) {
                # Validation of date: 
                if ( $CmData{$cmType}{$cmRef}{START_DATE} eq '01.01.1753' ) {
                    $errStr = "VALIDATION ERROR - RCD ($cmRef) has invalid date ". $CmData{$cmType}{$cmRef}{START_DATE} ." for Product ($prodRef) on account ($accName)";
                    push(@Errors,$errStr);
                    &zDebug('D',$errStr);
                    next;
                }
                my ($ltype, $recChargeRef, $recDscRef) = &zCM_RCD($cmRef,\%{$CmData{$cmType}{$cmRef}},$accName);
                # Add the details to the approriate product structure. 
                if ($ltype == 0) {
                    &zDebug('V',"Adding RCD product ");
                    $entCnt = defined($lProductOrder{FlexibleAttributes}{FlexiRecurringCharges}) ? @{ $lProductOrder{FlexibleAttributes}{FlexiRecurringCharges} } : 0;
                    $lProductOrder{FlexibleAttributes}{FlexiRecurringCharges}[$entCnt] = $recChargeRef;
                    
                    if ( $recDscRef->{CRMOfferId}) {
                        $entCnt = defined($lProductOrder{FlexibleAttributes}{FlexiRecurringDiscounts}) ? @{ $lProductOrder{FlexibleAttributes}{FlexiRecurringDiscounts} } : 0;
                        $lProductOrder{FlexibleAttributes}{FlexiRecurringDiscounts}[$entCnt] = $recDscRef;
                    }
                }
            }
        }
        
        # Process each one-off charge record (NRCD). 
        elsif ($cmType eq 'NRCD' and not $opt_migration) {
        
            foreach $cmRef (keys %{$CmData{$cmType}}) {
                my ($ltype, $chargeRef, $discountRef) = &zCM_NRCD($cmRef,\%{$CmData{$cmType}{$cmRef}});
                if ($ltype == 0) {
                    &zDebug('V',"Adding Product level NRCD product ");
                    # product level
                    my %NewCharges;
                    $OneOffCharges{$cmRef}{OneOffCharges} = $chargeRef;
                    $OneOffCharges{$cmRef}{ACCOUNT_NAME} = $lProductOrder{ExternalAccountNumber};
                    $OneOffCharges{$cmRef}{SERVICE_NAME} = $lProductOrder{SERVICE_HISTORY}[0]{SERVICE}{SERVICE_NAME};
                    
                    if ($discountRef->{ONE_OFF_CHARGE_REFERENCE}) {
                        # There was a discount on the charge also.
                        $OneOffCharges{$discountRef->{ONE_OFF_CHARGE_REFERENCE}}{OneOffCharges} = $discountRef;
                        $OneOffCharges{$discountRef->{ONE_OFF_CHARGE_REFERENCE}}{ACCOUNT_NAME} = $lProductOrder{ExternalAccountNumber};
                        $OneOffCharges{$discountRef->{ONE_OFF_CHARGE_REFERENCE}}{SERVICE_NAME} = $lProductOrder{SERVICE_HISTORY}[0]{SERVICE}{SERVICE_NAME};
                    }
                } else {
                    &zDebug('V',"Adding Account level NRCD product ");
                    # TODO the account level charges
                    1;
                }
            }  
        }
        
        # Process each one-off charge record (NRCD). 
        elsif ($cmType eq 'DFU') {
    
            foreach $cmRef (keys %{$CmData{$cmType}}) {
                my ($ltype, $fieldRef) = &zCM_DFU($cmRef,\%{$CmData{$cmType}{$cmRef}});
                # Perform validation of mandatory fields. 
                if ($ltype <= 1 and ( length($fieldRef->{InvoiceText}) == 0 ) ) {
                    push(@Errors,"CM - DFU ($cmRef) is missing mandatory parameter (INVOICE_TEXT) on account ($accName)");
                    &zDebug('D',"CM - DFU ($cmRef) is missing mandatory parameter (INVOICE_TEXT) on account ($accName)");
                    next;
                }
                
                
                # Add the details to the approriate product structure. 
                if ($ltype == 0) {
                    &zDebug('V',"Adding Product level DFU product ");
                    # DFU on product level. 
                    $entCnt = defined($lProductOrder{FlexibleAttributes}{FlexiRecurringFreeUnits}) ? @{ $lProductOrder{FlexibleAttributes}{FlexiRecurringFreeUnits} } : 0;
                    $lProductOrder{FlexibleAttributes}{FlexiRecurringFreeUnits}[$entCnt] = $fieldRef;            
                } elsif ($ltype == 1) {
                    # DFU on account level.
                    &zDebug('V',"Adding Account level DFU product ");
                    %zAccProdOrder = &zAccProdBuild($accName) if ( not %zAccProdOrder);
                    $entCnt = defined($zAccProdOrder{FlexibleAttributes}{FlexiRecurringFreeUnits}) ? @{ $zAccProdOrder{FlexibleAttributes}{FlexiRecurringFreeUnits} } : 0;
                    $zAccProdOrder{FlexibleAttributes}{FlexiRecurringFreeUnits}[$entCnt] = $fieldRef;
                    
                } else {
                    &zDebug('V',"SKIPPED - DFU with fields - " . $CmData{$cmType}{$cmRef}{'FU_PLAN'}); 
                }
            }
        }
        
        # Process each one-off charge record (AFU). 
        elsif ($cmType eq 'AFU') {
    
            foreach $cmRef (keys %{$CmData{$cmType}}) {
                my ($ltype, $fieldRef) = &zCM_AFU($cmRef,\%{$CmData{$cmType}{$cmRef}});
                # Perform validation of mandatory fields. 
                if ( length($fieldRef->{InvoiceText}) == 0 ) {
                    push(@Errors,"CM - AFU ($cmRef) is missing mandatory parameter (INVOICE_TEXT) on account ($accName)");
                    &zDebug('D',"CM - DFU ($cmRef) is missing mandatory parameter (INVOICE_TEXT) on account ($accName)");
                    next;
                }
                
                
                # Add the details to the approriate product structure. 
                if ($ltype == 0) {
                    &zDebug('V',"Adding Product level AFU product ");
                    # AFU on product level. 
                    $entCnt = defined($lProductOrder{FlexibleAttributes}{FlexiRecurringUsageDiscounts}) ? @{ $lProductOrder{FlexibleAttributes}{FlexiRecurringUsageDiscounts} } : 0;
                    $lProductOrder{FlexibleAttributes}{FlexiRecurringUsageDiscounts}[$entCnt] = $fieldRef;            
                } elsif ($ltype == 1) {
                    # AFU on account level.
                    &zDebug('V',"Adding Account level AFU product ");
                    %zAccProdOrder = &zAccProdBuild($accName) if ( not %zAccProdOrder);
                    $entCnt = defined($zAccProdOrder{FlexibleAttributes}{FlexiRecurringUsageDiscounts}) ? @{ $zAccProdOrder{FlexibleAttributes}{FlexiRecurringUsageDiscounts} } : 0;
                    $zAccProdOrder{FlexibleAttributes}{FlexiRecurringUsageDiscounts}[$entCnt] = $fieldRef;
                    
                } else {
                    &zDebug('V',"SKIPPED - DFU with fields - " . $CmData{$cmType}{$cmRef}{'FU_PLAN'}); 
                }
            }
        }
        
        # Process Account level entities 
        elsif ( $cmType eq 'ITD' ) {
            foreach $cmRef (keys %{$CmData{$cmType}}) {
                # Add the details to the approriate product structure. 
                $lFields{PERCENTAGE} = $CmData{$cmType}{$cmRef}{'DISCOUNT_PERCENT'};
                &zAccProdAdd($accName, $l_PrdData->{CLARIFY_PRODUCT_NAME}, $cmType, $cmRef, $CmData{$cmType}{$cmRef}{'START_DATE'}, \%lFields);
            }
            # for these products, we do not create the Clarify product in Singleview. 
            $lNoProduct = 1;
        }
        
        elsif ( $cmType eq 'PCD' ) {
            $lFields{PERCENTAGE} = $CmData{''}{''}{'DISCOUNT.VALUE'};
            &zAccProdAdd($accName, $l_PrdData->{CLARIFY_PRODUCT_NAME}, $cmType, $prodRef, undef, \%lFields);
            # for these products, we do not create the Clarify product in Singleview. 
            $lNoProduct = 1;
        }
        
        elsif ( $cmType eq 'MTO' ) {
            # Add the details to the approriate product structure.
            foreach $cmRef (keys %{$CmData{$cmType}}) {
                &zAccProdAdd($accName, $l_PrdData->{CLARIFY_PRODUCT_NAME}, $cmType, $prodRef, $CmData{$cmType}{$cmRef}{'START_DATE'}, \%{$CmData{$cmType}{$cmRef}});
            }
            # for these products, we do not create the Clarify product in Singleview. 
            $lNoProduct = 1;
        }
        
        elsif ($cmType eq 'CUD') {
            # Add the details to the approriate product structure. 
            &zAccProdAdd($accName, $l_PrdData->{CLARIFY_PRODUCT_NAME}, $cmType, $prodRef, undef, \%lFields);
            # for these products, we do not create the Clarify product in Singleview. 
            $lNoProduct = 1;
        
        } else {
            &zDebug('V',"Found $cmType Entry ");
        }
        
    }
    
    # Validation errors. 
    if ($lProductOrder{SERVICE_HISTORY}[0]{SERVICE}{NUMBER_REGION_TYPE} == 0) {
        push(@Errors,"PRODUCT - RATE_PLAN (".$l_PrdData->{RATE_PLAN}.") is not known for product ($prodRef) on account ($accName)");
        &zDebug('D',"PRODUCT - RATE_PLAN (".$l_PrdData->{RATE_PLAN}.") is not known for product ($prodRef) on account ($accName)");
    }
    if (not $lProductOrder{SERVICE_HISTORY}[0]{SERVICE}{SERVICE_NAME}) {
        push(@Errors,"PRODUCT - SERVICE_NAME is Mandatory for product ($prodRef) on account ($accName)");
        &zDebug('D',"PRODUCT - SERVICE_NAME is Mandatory for product ($prodRef) on account ($accName)");
    }
    
    # If we should not create this product, then undefine it.
    if ($lNoProduct) {
      return (undef, \%OneOffCharges, \@Errors);
    }
    
 
    return (\%lProductOrder, \%OneOffCharges, \@Errors);
    
}

#***************************************************************************
# NAME:
#   zProdCancelBuild
#
# DESCRIPTION:
#   Builds the order for cancellation of a product.  
#
# PARAMETERS:
#
# RETURNS:
#   Order that can be se sent to MaintainFlexiService API
#***************************************************************************
sub zProdCancelBuild
{
    my $l_PrdData = shift;
    my (%lProductOrder, %OfferDetails,  @Errors);
    
    my $cancelDate = &zToSvDate($l_PrdData->{CANCEL_DATE},1);
    # Determine the date the product should be canceled from. 
    $zCSRs{PROD_CANCELLED}->execute($PREFIX_PROD.$l_PrdData->{PRODUCT_REF},$cancelDate) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
    my @RecRow = $zCSRs{PROD_CANCELLED}->fetchrow_array();
    
    # Already cancelled. Nothing to do. 
    if ($RecRow[0] == 1) {
        return (\%lProductOrder, \@Errors);
    }
    
    # Create cancel order if needed. 
    my $accName = $l_PrdData->{CUSTOMER_NODE_REF}.''; 
    my $prodRef = $l_PrdData->{PRODUCT_REF}.'';
    
    # Build create structure: 
    $lProductOrder{SOURCE_SYSTEM}         = $SOURCE;
    $lProductOrder{ExternalAccountNumber} = $PREFIX_ACC.$accName; 
    $lProductOrder{ExternalServiceNumber} = $PREFIX_PROD.$prodRef;
    $lProductOrder{SERVICE_HISTORY}[0]{FromDate} = $cancelDate;
    $lProductOrder{SERVICE_HISTORY}[0]{DeactivateFlag} = 1;
        
    return (\%lProductOrder, \@Errors);
}

#***************************************************************************
# NAME:
#   &zAccProdAdd
#
# DESCRIPTION:
#   Adds charge mechanism to the account product. 
# PARAMETERS:
#   $accName - Account Name (CUSTOMER_NODE_REF)
#   $l_type - Type of data provide in the next parameter for update / add. Supported: 
#       * DFU
#       * ITD
#   $l_fieldRef - Configured field values to pass to the account product. 
#
# RETURNS:
#   Level (0 - product, 1 - account, 2 - error / skipped)
#   Recurring charge hash reference
#   Recurring discount hash reference. 
#***************************************************************************
sub zAccProdAdd
{
    my $accName = shift;
    my $l_clarifyProdName = shift;
    my $l_type = shift;
    my $l_cmRef = shift;
    my $l_StartDate = shift;
    my $l_fieldRef = shift;
    
    my ($entCnt, $dfuData, %FlexiProducts);
    
    &zDebug('D',"zAccProdAdd called with ($l_cmRef)");
    &zDebug('V','zAccProdAdd called for Clarify product -  ' . $l_clarifyProdName . ' for type - ' . $l_type);
    
    
    # Build the account service / product structure if not built yet.
    %zAccProdOrder = &zAccProdBuild($accName) if ( not %zAccProdOrder);
    
    
    # Set common values. 
    
    $FlexiProducts{StartDate} = $zAccProdOrder{SERVICE_HISTORY}[0]{FromDate};
    
    if ($l_type eq 'ITD') {
        if ( $l_clarifyProdName eq 'EFT Discount' ) {
            &zDebug('V',"Adding Account level ETF product ");
            $FlexiProducts{ProductName} = 'H3ATT_DSC_EFT';
            $FlexiProducts{StartDate}   = &zToSvDate($l_StartDate) if (defined $l_StartDate);
            $FlexiProducts{CRMRef}      = $l_cmRef;
            $FlexiProducts{PRODUCT_ATTRIBUTES}{CRMOfferId}  = $l_cmRef;
            $entCnt = defined($FlexiProducts{PREFERENCES}{BUS_DSC_INFO}) ? @{ $FlexiProducts{PREFERENCES}{BUS_DSC_INFO} } : 0;
            $FlexiProducts{PREFERENCES}{BUS_DSC_INFO}[$entCnt]{PRODUCT_ID} = $PROD_H3ATT_DSC_EFT;
            $FlexiProducts{PREFERENCES}{BUS_DSC_INFO}[$entCnt]{DISCOUNT_PERCENT} = $l_fieldRef->{PERCENTAGE};
            $FlexiProducts{PREFERENCES}{BUS_DSC_INFO}[$entCnt]{CRM_REF} = $l_cmRef;
        } else {
            &zDebug('V',"Adding Account level Big_Customer_Discount product ");
            $FlexiProducts{ProductName} = 'H3ATT_DSC_InvTotal';
            $FlexiProducts{StartDate}   = &zToSvDate($l_StartDate) if (defined $l_StartDate);
            $FlexiProducts{CRMRef}      = $l_cmRef;
            $FlexiProducts{PRODUCT_ATTRIBUTES}{CRMOfferId}  = $l_cmRef;
            $entCnt = defined($FlexiProducts{PREFERENCES}{BUS_DSC_INFO}) ? @{ $FlexiProducts{PREFERENCES}{BUS_DSC_INFO} } : 0;
            $FlexiProducts{PREFERENCES}{BUS_DSC_INFO}[$entCnt]{PRODUCT_ID} = $PROD_H3ATT_DSC_INVTOTAL;
            $FlexiProducts{PREFERENCES}{BUS_DSC_INFO}[$entCnt]{DISCOUNT_PERCENT} = $l_fieldRef->{PERCENTAGE};
            $FlexiProducts{PREFERENCES}{BUS_DSC_INFO}[$entCnt]{CRM_REF} = $l_cmRef;
        }
    }
    
    if ($l_type eq 'MTO') {
        $FlexiProducts{ProductName} = 'H3ATT_BIL_MinimumTurnover';
        $FlexiProducts{StartDate}   = &zToSvDate($l_StartDate) if (defined $l_StartDate);
        $FlexiProducts{CRMRef}      = $l_cmRef;
        $FlexiProducts{PRODUCT_ATTRIBUTES}{CRMOfferId}  = $l_cmRef;
        $FlexiProducts{PREFERENCES}{BUS_MTO_INFO}[0]{PRODUCT_ID}    = $PROD_H3ATT_MTO;
        $FlexiProducts{PREFERENCES}{BUS_MTO_INFO}[0]{CRM_REF}       = $l_cmRef;
        $FlexiProducts{PREFERENCES}{BUS_MTO_INFO}[0]{INVOICE_TEXT}  = $l_fieldRef->{INVOICE_TEXT};
        $FlexiProducts{PREFERENCES}{BUS_MTO_INFO}[0]{TURNOVER_AMOUNT}  = $l_fieldRef->{TURNOVER}*1.0;
        $FlexiProducts{PREFERENCES}{BUS_MTO_INFO}[0]{FINANCE_CODE}  = $l_fieldRef->{CO_CODE}.':'.$l_fieldRef->{FI_CODE};
        $FlexiProducts{PREFERENCES}{BUS_MTO_INFO}[0]{CHARGE_ID}     = $l_fieldRef->{CHARGE_ID};
    }
    
    if ($l_type eq 'CUD') {
        $FlexiProducts{ProductName} = 'H3ATT_DSC_CustomerUsage';
        $FlexiProducts{StartDate}   = &zToSvDate($l_StartDate) if (defined $l_StartDate);
        $FlexiProducts{CRMRef}      = $l_cmRef;
        $FlexiProducts{PRODUCT_ATTRIBUTES}{CRMOfferId}  = $l_cmRef;
    }
    
    if ($l_type eq 'PCD') {
        $FlexiProducts{ProductName} = 'H3ATT_DSC_ProductClass';
        $FlexiProducts{StartDate}   = &zToSvDate($l_StartDate) if (defined $l_StartDate);
        $FlexiProducts{CRMRef}      = $l_cmRef;
        $FlexiProducts{PRODUCT_ATTRIBUTES}{CRMOfferId}  = $l_cmRef;
        $entCnt = defined($FlexiProducts{PREFERENCES}{BUS_DSC_INFO}) ? @{ $FlexiProducts{PREFERENCES}{BUS_DSC_INFO} } : 0;
        $FlexiProducts{PREFERENCES}{BUS_DSC_INFO}[$entCnt]{PRODUCT_ID} = $PROD_H3ATT_DSC_PCD;
        $FlexiProducts{PREFERENCES}{BUS_DSC_INFO}[$entCnt]{DISCOUNT_PERCENT} = $l_fieldRef->{PERCENTAGE};
        $FlexiProducts{PREFERENCES}{BUS_DSC_INFO}[$entCnt]{CRM_REF} = $l_cmRef;
    }
    $entCnt = defined($zAccProdOrder{FlexibleAttributes}{FlexiProducts}) ? @{ $zAccProdOrder{FlexibleAttributes}{FlexiProducts} } : 0;
    $zAccProdOrder{FlexibleAttributes}{FlexiProducts}[$entCnt] = \%FlexiProducts;
    
    return 0;
}

#***************************************************************************
# NAME:
#   &zAccProdBuild
#
# DESCRIPTION:
#   Builds the Flexi order for the account product
# PARAMETERS:
#   $accName - Account Name (CUSTOMER_NODE_REF)
#
# RETURNS:
#   Level (0 - product, 1 - account, 2 - error / skipped)
#   Recurring charge hash reference
#   Recurring discount hash reference. 
#***************************************************************************
sub zAccProdBuild
{
    my $accName = shift;
    my $effDate = shift;
    my (%accProdOrder, %OfferDetails);
    
    # Build create structure: 
    $accProdOrder{SOURCE_SYSTEM}         = $SOURCE;
    $accProdOrder{ExternalAccountNumber} = $PREFIX_ACC.$accName; 
    $accProdOrder{ExternalServiceNumber} = $PREFIX_ACC.$accName;
    
    # Service details.
    $accProdOrder{SERVICE_HISTORY}[0]{SERVICE}{SERVICE_NAME}        = $PREFIX_ACC.$accName;
    $accProdOrder{SERVICE_HISTORY}[0]{SERVICE}{NETWORK_NAME}        = $PREFIX_ACC.$accName;
    $accProdOrder{SERVICE_HISTORY}[0]{SERVICE}{SERVICE_TYPE_ID}     = $VOICE_SERV_TYPE_ID;
    $accProdOrder{SERVICE_HISTORY}[0]{SERVICE}{NUMBER_REGION_TYPE}  = $NON_USAGE_NUMBER_PLAN;
    $accProdOrder{SERVICE_HISTORY}[0]{PACKAGE}{PACKAGE_NAME}        = 'H3ATT_pGenericAccPackage';
    if (exists $zAllAccountsRef->{$PREFIX_ACC . $accName}) {
        $accProdOrder{SERVICE_HISTORY}[0]{FromDate}                 = $zAllAccountsRef->{$PREFIX_ACC . $accName}{ACTIVE_DATE}; # Always set to account active date. 
    } else {
        $accProdOrder{SERVICE_HISTORY}[0]{FromDate}                 = &zToSvDate($effDate);
    }
    return %accProdOrder; 
}

#***************************************************************************
# NAME:
#   zCM_RCD
#
# DESCRIPTION:
#   Adds recurring charge / discount to a product order. 
#   Cancels 
#
# PARAMETERS:
#   $l_cmRef - unique CRM_REF for this CM
#   $l_RecData - Record data to transform
#
# RETURNS:
#   Level (0 - product, 1 - account, 2 - error / skipped)
#   Recurring charge hash reference
#   Recurring discount hash reference. 
#***************************************************************************
sub zCM_RCD
{
    my $l_cmRef = shift;
    my $l_RecData = shift;
    my $accName = shift;
    
    my (%RecCharges, %RecDiscounts, $cancelDate);
    # Skip the record if the integrity of the entry is not valid, but instead print it out for analysis in the error log. 
    if ( ($l_RecData->{CM_STATUS} eq 'Cancelled') ) {
        $cancelDate = $l_RecData->{CANCEL_DATE};
        $cancelDate =~ s/\./-/g;
        if ($opt_migration) {
            if ( ( $l_RecData->{FREQUENCY} ne 'Yearly' and (&zCompareDates($l_RecData->{CANCEL_DATE},$zBilledDate) == -1) ) or
                ( $l_RecData->{FREQUENCY} eq 'Yearly' and (&zCompareDates($l_RecData->{CANCEL_DATE},&zToSvDate($zMigrationDate,365)) == -1) ) )
            {
                &zDebug('D',"RCD ($l_cmRef) skipped because it's cancelled");
                return (2, \%RecCharges, \%RecDiscounts);
            }
            if (&zCompareDates($l_RecData->{CANCEL_DATE},$l_RecData->{START_DATE}) == 0) {
                &zDebug('D',"RCD ($l_cmRef) skipped because it's cancelled on start_date");
                return (2, \%RecCharges, \%RecDiscounts);
            }
            
        } else {
            # In normal mode, we check to see if product is already cancelled from the requested date. If not cancelled, then we cancel it.
           
            $zCSRs{REC_ACTIVE}->execute($PREFIX_ACC.$accName,$cancelDate,$l_cmRef) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
            my @arr = $zCSRs{REC_ACTIVE}->fetchrow_array();
            if ($arr[0] != 1) {
                &zDebug('D',"RCD ($l_cmRef) skipped because it's cancelled");
                return (2, \%RecCharges, \%RecDiscounts);
            }
        }
    }
    
    # Build chargeClassification string. 
    my $chgClass = $l_RecData->{ADVANCE_PERIOD}.':'.$l_RecData->{PRO_RATE}.':'.$l_RecData->{FREQUENCY};    
    if (not $zChgClass{$chgClass} ) {
        &zdie($MSG_PASSTHRU,"Recurring parameter cannot map to a product. Fields with DATA: \n" . Dumper($l_RecData));
    }
    
    
    # Handle amounts
    $l_RecData->{RATE} =~ s/,/./; 
    $l_RecData->{DISCOUNT_VALUE} =~ s/,/./; 
    
    # Recurring charges
    $RecCharges{CRMOfferId}             = $l_cmRef;
    $RecCharges{Charge}                 = strtod($l_RecData->{RATE});
    $RecCharges{ChargeClassification}   = $zChgClass{$chgClass}{REC};
    $RecCharges{InvoiceText}            = $l_RecData->{INVOICE_TEXT};
    
    # Need to handle StartDate differently for migration mode for yearly charges. 
    # We need to have it created on the same day of the year as it was created after the migration date. 
    if ( ($l_RecData->{FREQUENCY} eq 'Yearly') and (&zCompareDates($l_RecData->{START_DATE},$zMigrationDate) == -1) ) {
        my $yearlyStartDate = substr($l_RecData->{START_DATE},0,6) . substr($zMigrationDate,6,4);
        print("testing compare with $yearlyStartDate \n");
        while (&zCompareDates($yearlyStartDate,$zMigrationDate) == -1) {
            print("testing compare with $yearlyStartDate \n");
            $yearlyStartDate = substr($l_RecData->{START_DATE},0,6) . (substr($zMigrationDate,6,4)+1);                            
        }
        $RecCharges{StartDate}              = &zToSvDate($yearlyStartDate);
    } else {
        $RecCharges{StartDate}              = &zToSvDate($l_RecData->{START_DATE});
    }
    # Build the finance code. <CO>:<FI>
    if ( defined($l_RecData->{CO_CODE}) ) {
        $RecCharges{FinanceCode} = $l_RecData->{CO_CODE} . ':' . $l_RecData->{FI_CODE};
    }
    $RecCharges{ExternalChargeId}   = $l_RecData->{CHARGE_ID};
    $RecCharges{ClarifyChargeClass} = $l_RecData->{CHARGE_CLASS};
    
    # Only in migration mode do we add 
    if ($opt_migration and $l_RecData->{CM_STATUS} eq 'Cancelled') {
        $RecCharges{EndDate} = &zToSvDate($l_RecData->{CANCEL_DATE}, 0, 1);
    }
    
    # Recurring discounts
    # Add if there is a discount defined for product. 
    if (defined($l_RecData->{DISCOUNT_TYPE})) {
        $RecDiscounts{CRMOfferId}           = $l_cmRef;
        $RecDiscounts{CRMDiscountId}        = $l_cmRef.'_DSC';
        if ( $l_RecData->{DISCOUNT_TYPE} eq 'Percent' ) {
            $RecDiscounts{CreditPercentage} = strtod($l_RecData->{DISCOUNT_VALUE});
        } else {
            $RecDiscounts{CreditAmount}     = strtod($l_RecData->{DISCOUNT_VALUE});
        }
        $RecDiscounts{DiscountInvoiceText}  = $l_RecData->{DISCOUNT_TEXT};
        $RecDiscounts{DiscountPriority}     = 1;
        $RecDiscounts{StartDate}            = $RecCharges{StartDate};
        $RecDiscounts{EndDate}              = $RecCharges{EndDate} if ($RecCharges{EndDate});
    }
    
    return (0, \%RecCharges, \%RecDiscounts);
}

#***************************************************************************
# NAME:
#   zCM_NRCD
#
# DESCRIPTION:
#   Creates the Non-recurring charge if it has not been created yet.  
#
# PARAMETERS:
#   $l_cmRef - unique CRM_REF for this CM
#   $l_RecData - Record data to transform
#
# RETURNS:
#   Level (0 - product, 1 - account, 2 - error / skipped)
#   One-off charge reference
#   One-off discount reference
#***************************************************************************
sub zCM_NRCD
{
    my $l_cmRef = shift;
    my $l_Data = shift;
    
    my (%chargeFields, %dscFields, $dscAmt);
    &zDebug('D',"IN zCM_NRCD ($l_cmRef) with data: \n" . Dumper($l_Data) . "\n");
    
    # check if the charge has been created before, and skip if this is the case. 
    $zCSRs{ONEOFF_EXISTS}->execute($l_cmRef) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
    my @arr = $zCSRs{ONEOFF_EXISTS}->fetchrow_array();
    if ($arr[0] == 1) {
        &zDebug('V',"NRCD ($l_cmRef) skipped because it is already processed\n");
        $zStats{CM_CHG_SKIPPED} += 1;
        return (2, \%chargeFields, \%dscFields);
    }
    
    # replace , for .
    $l_Data->{RATE} =~ s/,/./;
    $l_Data->{DISCOUNT_VALUE} =~ s/,/./;
    
    # One-off charge
    $chargeFields{CHARGE_TYPE_CODE}         = $ONE_OFF_PROD;
    $chargeFields{CHARGE}                   = strtod($l_Data->{RATE}); 
    $chargeFields{INVOICE_TEXT}             = $l_Data->{INVOICE_TEXT};
    $chargeFields{CHARGE_DATE}              = &zToSvDate($l_Data->{START_DATE});
    $chargeFields{ONE_OFF_CHARGE_REFERENCE} = $l_cmRef;
    $chargeFields{EXT_CHARGE_CLASS}         = $l_Data->{CHARGE_CLASS};
    $chargeFields{EXT_CHARGE_ID}            = $l_Data->{CHARGE_ID};
    $chargeFields{EXT_FINANCE_CODES}        = $l_Data->{CO_CODE} . ':' . $l_Data->{FI_CODE};
    
    &zDebug('D',"IN zCM_NRCD with charge data: \n" . Dumper(%chargeFields) . "\n");
    
    # One-off discount
    # Add if there is a discount defined 
    if (defined($l_Data->{DISCOUNT_TYPE})) {
        my $dscCrmRef = $l_cmRef.'_DSC';
        if ($l_Data->{DISCOUNT_TYPE} eq 'Percent') {
            $dscAmt = sprintf("%.6f",(-1.0 * $l_Data->{RATE} * $l_Data->{DISCOUNT_VALUE} / 100.0));
        } else {
            $dscAmt = -1.0 * $l_Data->{DISCOUNT_VALUE};
        }
        $dscFields{CHARGE_TYPE_CODE}         = $ONE_OFF_PROD;
        $dscFields{CHARGE}                   = $dscAmt; 
        $dscFields{INVOICE_TEXT}             = $l_Data->{DISCOUNT_TEXT};
        $dscFields{CHARGE_DATE}              = &zToSvDate($l_Data->{START_DATE});
        $dscFields{ONE_OFF_CHARGE_REFERENCE} = $dscCrmRef;
        $dscFields{EXT_CHARGE_CLASS}         = $l_Data->{CHARGE_CLASS};
        $dscFields{EXT_CHARGE_ID}            = $l_Data->{CHARGE_ID};
        $dscFields{EXT_FINANCE_CODES}        = $l_Data->{CO_CODE} . ':' . $l_Data->{FI_CODE};
    }
    
    &zDebug('D',"IN zCM_NRCD with discount data: \n" . Dumper(%dscFields) . "\n");
    
    return (0, \%chargeFields, \%dscFields);
}

#***************************************************************************
# NAME:
#   zAccountAdjBuild
#
# DESCRIPTION:
#   Determine if there are any AJDUSTMENTS need to be created
#
# PARAMETERS:
#   $lAccRef - Account selected data
#
# RETURNS:
#   Array of adjustment fields. 
#***************************************************************************
sub zAccountAdjBuild
{
    my $lRecRow = shift;
    
    my (%adjFields);
    
    # Check if this adjustment has been created. Skip if found
    $zCSRs{ONEOFF_EXISTS}->execute($lRecRow->{CM_REF}) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);;
    my @arr = $zCSRs{ONEOFF_EXISTS}->fetchrow_array();
    if ($arr[0] == 1) {
        &zDebug('V',"ADJ (" . $lRecRow->{CM_REF} . ") skipped because it is already processed\n");
        next;
    }

    # replace , for .
    $lRecRow->{RATE} =~ s/,/./;
    
    # One-off charge
    $adjFields{CHARGE_TYPE_CODE}         = $ONE_OFF_CREDIT;
    $adjFields{CHARGE}                   = strtod($lRecRow->{RATE}) * -1.0; # reverse the adjustment amount. 
    $adjFields{INVOICE_TEXT}             = $lRecRow->{INVOICE_TEXT};
    $adjFields{CHARGE_DATE}              = &zToSvDate($lRecRow->{START_DATE});
    $adjFields{ONE_OFF_CHARGE_REFERENCE} = $lRecRow->{CM_REF};
    $adjFields{EXT_CHARGE_ID}            = $lRecRow->{CHARGE_ID};
    $adjFields{EXT_FINANCE_CODES}        = $lRecRow->{CO_CODE} . ':' . $lRecRow->{FI_CODE};
    $adjFields{EXT_REASON}               = $lRecRow->{REASON};
    
    &zDebug('D',"IN zAccountAdjBuild with charge data: \n" . Dumper(%adjFields) . "\n");
    
    return \%adjFields;
        
    
    
}

#***************************************************************************
# NAME:
#   zCM_DFU
#
# DESCRIPTION:
#   Places DFU fields into supported KIM fields
#
# PARAMETERS:
#   $l_cmRef - unique CRM_REF for this CM
#   $l_RecData - Record data to transform
#
# RETURNS:
#   Level (0 - product, 1 - account, 2 - error / skipped)
#   Field hash reference
#***************************************************************************
sub zCM_DFU
{
    my $l_cmRef = shift;
    my $l_RecData = shift;
    my (%Fields, $level);
    
    # Only include if this is a valid FU_PLAN
    if ($zRefData{'H3AT_GENERIC_FU_TYPE'}{CODES}{$l_RecData->{FU_PLAN}} ) {
        
        # DFU fields
        $Fields{CRMOfferId}     = $l_cmRef;
        $Fields{FUAmount}       = $l_RecData->{VALUE} * 1;
        $Fields{FUTypeName}     = $l_RecData->{FU_PLAN};
        $Fields{Prorate}        = $l_RecData->{PRO_RATE} eq 'True' ? 1 : 0;
        $Fields{InvoiceText}    = $l_RecData->{INVOICE_TEXT};
        $Fields{StartDate}      = &zToSvDate($l_RecData->{START_DATE});
        if ($l_RecData->{PRODUCT_REF}) {
            $level = 0;
        } else {
            $level = 1;
            $Fields{AccountLevel} = 1;
        }
    } else { 
        return (2);
    }
    
    return ($level, \%Fields);
}

#***************************************************************************
# NAME:
#   zCM_AFU
#
# DESCRIPTION:
#   Places AFU fields into supported KIM fields
#
# PARAMETERS:
#   $l_cmRef - unique CRM_REF for this CM
#   $l_RecData - Record data to transform
#
# RETURNS:
#   Level (0 - product, 1 - account, 2 - error / skipped)
#   Field hash reference
#***************************************************************************
sub zCM_AFU
{
    my $l_cmRef = shift;
    my $l_RecData = shift;
    my (%Fields, $level);
    
    # DFU fields
    $Fields{CRM_REF}        = $l_cmRef;
    $Fields{Amount}         = $l_RecData->{AMOUNT} * 1.0;
    $Fields{DiscountType}   = '6000';
    $Fields{InvoiceText}    = $l_RecData->{INVOICE_TEXT};
    $Fields{StartDate}      = &zToSvDate($l_RecData->{START_DATE});
    if ($l_RecData->{PRODUCT_REF}) {
        $level = 0;
    } else {
        $level = 1;
        $Fields{AccountLevel} = 1;
    }
    
    return ($level, \%Fields);
}

#***************************************************************************
# NAME:
#   zPrepareSvCursors
#
# DESCRIPTION:
#   Prepare the SV SQL's   
#
# PARAMETERS:
#
# RETURNS:
#   Describe what the subroutine returns.
#***************************************************************************
sub zPrepareSvCursors
{
    if ( not $opt_migration) {
        &zDebug('D',"Preparing SQL - " . $zSQLs{BE_UPDATE});
        $zCSRs{BE_UPDATE} = $zdb->prepare($zSQLs{BE_UPDATE}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
        &zDebug('D',"Preparing SQL - " . $zSQLs{BE_STATUS});
        $zCSRs{BE_STATUS} = $zdb->prepare($zSQLs{BE_STATUS}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
        &zDebug('D',"Preparing SQL - " . $zSQLs{CUST_EVENTS});
        $zCSRs{CUST_EVENTS} = $zdb->prepare($zSQLs{CUST_EVENTS}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
        &zDebug('D',"Preparing SQL - " . $zSQLs{PROD_EVENTS});
        $zCSRs{PROD_EVENTS} = $zdb->prepare($zSQLs{PROD_EVENTS}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
        &zDebug('D',"Preparing SQL - " . $zSQLs{ADJ_EVENTS});
        $zCSRs{ADJ_EVENTS} = $zdb->prepare($zSQLs{ADJ_EVENTS}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
        &zDebug('D',"Preparing SQL - " . $zSQLs{PROD_CANCELLED});
        $zCSRs{PROD_CANCELLED} = $zdb->prepare($zSQLs{PROD_CANCELLED}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
    }

    &zDebug('D',"Preparing SQL - " . $zSQLs{CUST_INSERT});
    $zCSRs{CUST_INSERT} = $zdb->prepare($zSQLs{CUST_INSERT}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
    &zDebug('D',"Preparing SQL - " . $zSQLs{CUST_OBJECTS_INSERT});
    $zCSRs{CUST_OBJECTS_INSERT} = $zdb->prepare($zSQLs{CUST_OBJECTS_INSERT}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
    &zDebug('D',"Preparing SQL - " . $zSQLs{PROD_INSERT});
    $zCSRs{PROD_INSERT} = $zdb->prepare($zSQLs{PROD_INSERT}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
    &zDebug('D',"Preparing SQL - " . $zSQLs{CM_INSERT});
    $zCSRs{CM_INSERT} = $zdb->prepare($zSQLs{CM_INSERT}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
    &zDebug('D',"Preparing SQL - " . $zSQLs{ADJ_INSERT});
    $zCSRs{ADJ_INSERT} = $zdb->prepare($zSQLs{ADJ_INSERT}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);

    &zDebug('D',"Preparing SQL - " . $zSQLs{DATE_INC});
    $zCSRs{DATE_INC} = $zdb->prepare($zSQLs{DATE_INC}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
    &zDebug('D',"Preparing SQL - " . $zSQLs{EXISTING_ACCOUNTS});
    $zCSRs{EXISTING_ACCOUNTS} = $zdb->prepare($zSQLs{EXISTING_ACCOUNTS}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
    &zDebug('D',"Preparing SQL - " . $zSQLs{ONEOFF_EXISTS});
    $zCSRs{ONEOFF_EXISTS} = $zdb->prepare($zSQLs{ONEOFF_EXISTS}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
    &zDebug('D',"Preparing SQL - " . $zSQLs{REC_ACTIVE});
    $zCSRs{REC_ACTIVE} = $zdb->prepare($zSQLs{REC_ACTIVE}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
    
}

#***************************************************************************
# NAME:
#   zSetupClyConnection
#
# DESCRIPTION:
#   Sets up the extraction SQL's   
#
# PARAMETERS:
#
# RETURNS:
#   Describe what the subroutine returns.
#***************************************************************************
sub zSetupClyConnection
{

    if (not $opt_mock) {
        $zdb_cly->do('ALTER SESSION set NLS_DATE_FORMAT = \'DD.MM.YYYY\'') or die $zdb_cly->errstr;
        $zdb_cly->do('ALTER SESSION set NLS_TIMESTAMP_FORMAT = \'DD.MM.RR HH24:MI:SSXFF\'') or die $zdb_cly->errstr;
        $zdb_cly->do('ALTER SESSION set NLS_NUMERIC_CHARACTERS = \',.\'') or die $zdb_cly->errstr; 
    }
    
    &zDebug('D',"Preparing SQL - " . $zSQLs{CUST});
    $zCSRs{CUST} = $zdb_cly->prepare($zSQLs{CUST}) || &zdie(6000, __LINE__, __FILE__, $zdb_cly->errstr);
    
    &zDebug('D',"Preparing SQL - " . $zSQLs{CUST_OBJECTS});
    $zCSRs{CUST_OBJECTS} = $zdb_cly->prepare($zSQLs{CUST_OBJECTS}) || &zdie(6000, __LINE__, __FILE__, $zdb_cly->errstr);
    
            
    if (not $opt_mock) {
        if ($opt_migration or $opt_load_mock) {
            &zDebug('D',"Preparing SQL - " . $zSQLs{CUST_PROD});
            $zCSRs{CUST_PROD} = $zdb_cly->prepare($zSQLs{CUST_PROD}) || &zdie(6000, __LINE__, __FILE__, $zdb_cly->errstr);
        }
        &zDebug('D',"Preparing SQL - " . $zSQLs{PROD_LOAD});
        $zCSRs{PROD_LOAD} = $zdb_cly->prepare($zSQLs{PROD_LOAD}) || &zdie(6000, __LINE__, __FILE__, $zdb_cly->errstr);
    }
    &zDebug('D',"Preparing SQL - " . $zSQLs{PROD});
    $zCSRs{PROD}     = $zdb_cly->prepare($zSQLs{PROD}) || &zdie(6000, __LINE__, __FILE__, $zdb_cly->errstr);
    &zDebug('D',"Preparing SQL - " . $zSQLs{PROD_CM});
    $zCSRs{PROD_CM}  = $zdb_cly->prepare($zSQLs{PROD_CM}) || &zdie(6000, __LINE__, __FILE__, $zdb_cly->errstr);
    
    # Cursors only when in normal mode. 
    if (not $opt_migration) {
        &zDebug('D',"Preparing SQL - " . $zSQLs{CUST_ADJ});
        $zCSRs{CUST_ADJ} = $zdb_cly->prepare($zSQLs{CUST_ADJ}) || &zdie(6000, __LINE__, __FILE__, $zdb_cly->errstr);
    }
    
}

#***************************************************************************
# NAME:
#   zAccountsToProcess
#
# DESCRIPTION:
#   Builds a list of account names to proces. 
#
# PARAMETERS:
#
# RETURNS:
#   0 if there is a restriction to requested accounts. 
#   1 full mode, all accounts 
#***************************************************************************
sub zAccountsToProcess
{
    my (@lAccounts, $filePath);
    
    # Determine the list of accounts to migrate.
    # Ignore the file if -site is provided. 
    if ($opt_site) {
        push(@zAccounts,$opt_site);
    # First do a test if we are given a file or a site_id directly. 
    } elsif ($opt_filename) {
        my $lFilename = $opt_filename;
        # Try to find the file to use in the following locations in this order. If not found then error. 
        # 1. Look in $ATA_DATA_SERVER_INPUT
        # 2. Look in the current directory of execution to support unit testing.
        &zDebug('D',"Looking for file in dir - $INPUT_DIR/$lFilename");        
        if (-e "$INPUT_DIR/$lFilename") {
            $filePath = "$INPUT_DIR/$lFilename";
        } elsif (-e $lFilename) {
            $filePath = $lFilename;
        } else {
            &zdie($MSG_PASSTHRU,"No file with $lFilename found in $INPUT_DIR or " . `pwd` . ".Exiting.....\n");
        }
        
        # File exists, so assume its a file and get all the accounts from it and store in 
        open (my $fh, "<", $filePath) || &zdie($ERR_GENERROR,"Cannot open $filePath \n");
        while (my $accName = <$fh>) {
            $accName =~ s/\s+$//;
            push @zAccounts, $accName;
        }
    } else {
        return 1;
    }
    
    return 0;
    
}

#***************************************************************************
# NAME:
#   zFetchAccounts
#
# DESCRIPTION:
#   Fetch all existing accounts in Singleview and needed details. 
#
# PARAMETERS:
#
# RETURNS:
#   Describe what the subroutine returns.
#***************************************************************************
sub zFetchAccounts
{
    my (%AccInfo);
    
    $zCSRs{EXISTING_ACCOUNTS}->execute() || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
    
    while (my $RecRow = $zCSRs{EXISTING_ACCOUNTS}->fetchrow_hashref("NAME_uc")) {
        $AccInfo{$RecRow->{ACCOUNT_NAME}}{ACTIVE_DATE} = $RecRow->{ACTIVE_DATE};
        $AccInfo{$RecRow->{ACCOUNT_NAME}}{CUSTOMER_NODE_ID} = $RecRow->{CUSTOMER_NODE_ID};
    }
    
    $zAllAccountsRef = \%AccInfo;

}

#***************************************************************************
# NAME:
#   zMigrateAccounts
#
# DESCRIPTION:
#   Processes all the changes in billing events that require a account update.  
#
# PARAMETERS:
#
# RETURNS:
#   Describe what the subroutine returns.
#***************************************************************************
sub zMigrateAccounts
{
    my ($rc, $lPid, $lResult, $now);
    
    &zDebug('V',"Start zMigrateAccounts");
    
    &zSetupClyConnection();
    
    # Prepare cursors only for migration: 
    # Setup insert cursors
    my @row_ary = $zdb->selectrow_array('SELECT MAX(OBJID) FROM H3AT_PBE_EVENT_AUDIT WHERE X_OBJECT_TYPE = ' . $MIGRATION_TYPE);
    if ($zdb->errstr) {
        &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
    }
    $zMigId = (@row_ary) ? $row_ary[0] : 0;
    &zDebug('V',"Migration ID - " . $zMigId);
    &zDebug('D',"Preparing SQL - " . $zSQLs{BE_MIG_INSERT});
    $zCSRs{BE_MIG_INSERT} = $zdb->prepare($zSQLs{BE_MIG_INSERT}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);

    &zAccountsToProcess() && &zdie($MSG_PASSTHRU, "USAGE: -load_mock requres -site or -filename option to limit accounts to process");
    $now = localtime;
    print "$now Provided with " . scalar @zAccounts . " accounts to migrate.\n";
    
    
    # process the account
    for my $accName (@zAccounts) {
        chomp $accName;
        last if (not $accName);
        $now = localtime;
        if (exists $zAllAccountsRef->{$PREFIX_ACC . $accName}) {
            print "$now Skipping account: $accName. Already migrated\n";
        } else {
            print "$now Processing account: $accName\n";
            $rc = &zMigrateAccount($accName);
        }
        
        if ($rc == 1) {
            # Error in fetching stage. Update stats
            $zStats{ACCOUNT_FAIL} += 1;
            $zStats{ACCOUNT_CNT} += 1;
            my $now = localtime;
            print "$now FAILED Account: $accName\n";
            $rc = 0;
        }
    }
    
    return 0;
    
}

#***************************************************************************
# NAME:
#   zMigrateAccount
#
# DESCRIPTION:
#   Migrates a single account. 
#
# PARAMETERS:
#
# RETURNS:
#   Describe what the subroutine returns.
#***************************************************************************
sub zMigrateAccount
{
    my $accName = shift;
    
    my (@ProdOrders,  @Errors, %Packet);
    my ($startTime, $migrateStart, $AccOrderRef, $prodOrderRef, $OneOffChargesRef, $lResult, $lPid, $ErrorsRef, $secs, $now);
    
    undef(%zAccProdOrder);
    
    ### Account fetch details
    # Fetch account data for this account
    $migrateStart = $startTime = [gettimeofday];
    if (not $zCSRs{CUST}->execute($accName)) {
        push(@Errors,"ACCOUNT - Unable to execute cursor - CUST with error: " . $zdb_cly->errstr);
        &zReportErrors(\@Errors, $migrateStart, $accName);
        return 0;
    }
    $zStats{ACCOUNT_SQL} += tv_interval($startTime);
    $opt_perf && &zPerf('CLARIFY','ACCOUNT_EXEC',$accName, tv_interval($startTime));
    
    $startTime = [gettimeofday];
    my $AccRow = $zCSRs{CUST}->fetchrow_hashref("NAME_uc");
    if (not defined($AccRow)) {
        print("WARNING: No account for requested Account ($accName) \n");
        return 0;
    }
    
    $opt_perf && &zPerf('CLARIFY','ACCOUNT_FETCH',$accName, tv_interval($startTime));
   
    &zInsertAudit('CUST', $zTaskId , $AccRow, \@Errors);

    ($AccOrderRef, $ErrorsRef) = &zAccountOrderBuild($AccRow);
    # On new account create the account product. 
    %zAccProdOrder = &zAccProdBuild($accName,$AccRow->{START_DATE});
    
    push(@Errors, @{$ErrorsRef}) if $ErrorsRef;
    $Packet{ACCOUNT} = $AccOrderRef;
    
    ### Get all top level products for this account
    $startTime = [gettimeofday];
    if ($zCSRs{CUST_PROD}->execute($accName)) { 
        $opt_perf && &zPerf('CLARIFY','CUST_PROD_EXEC',$accName, tv_interval($startTime));
        $zStats{CUST_PROD_SQL} += tv_interval($startTime);
        
        ### Loop through each top level product to build the orders for all the products. 
        while (my $prdData = $zCSRs{CUST_PROD}->fetchrow_hashref("NAME_uc")) {
            ### Get all the products orders to send after the account
            # Fetch all products for the account.
            $now = localtime;
            my $prodRef = $prdData->{INSTANCE_ID};
            next if ($prdData->{PART_STATUS} eq 'Terminated');
            if ($prdData->{PRODUCT_NAME} eq 'Service Discount'  or 
                $prdData->{PRODUCT_NAME} eq 'OnNet Tarif') {
                &zDebug('V',"Processing account product " . $prdData->{PRODUCT_NAME} . " ($prodRef)");                
            } elsif ($prdData->{PRODUCT_NAME} eq 'Landline discount SingleView' or 
                $prdData->{PRODUCT_NAME} eq 'Inter company tariff Validity' or
                $prdData->{PRODUCT_NAME} eq 'Landline Discount' or 
                $prdData->{PRODUCT_NAME} eq 'EVA Elfe') {
                &zDebug('V',"Skipping product " . $prdData->{PRODUCT_NAME} . " ($prodRef) as not supported yet!");
                next;
            } else {
                &zDebug('V',"Processing product " . $prdData->{PRODUCT_NAME} . " ($prodRef)");
            }
            
            $startTime = [gettimeofday];
            &zDebug('D','Calling Products SQL with product - ' . $prodRef);
            if (not $zCSRs{PROD_LOAD}->execute($prodRef)) {
                push(@Errors,"PRODUCT - ($prodRef) Unable to execute cursor - PROD_LOAD with error: " . $zdb_cly->errstr);
                next;
            }
            $opt_perf && &zPerf('CLARIFY','PROD_LOAD',$prodRef, tv_interval($startTime));
            $zStats{PROD_SQL} += tv_interval($startTime);
            $zStats{PROD_SQL_CNT} += 1;
            
            $startTime = [gettimeofday];
            if (not $zCSRs{PROD}->execute($prodRef)) {
                push(@Errors,"PRODUCT - ($prodRef) Unable to execute cursor - PROD with error: " . $zdb_cly->errstr);
                next;
            }
            $opt_perf && &zPerf('CLARIFY','PROD_DETAILS',$prodRef, tv_interval($startTime));
            ### Loop through the products and build the orders including the CM's for each child. 
            while (my $PrdRow = $zCSRs{PROD}->fetchrow_hashref("NAME_uc")) {
                &zDebug('D',"Fetched PRODUCT DATA: \n" . Dumper($PrdRow));
                $PrdRow->{CLARIFY_PRODUCT_NAME} = $prdData->{PRODUCT_NAME};
                
                &zInsertAudit('PROD', $zTaskId , $PrdRow, $ErrorsRef);

                # Add some filters here to skip products that should NOT be created. 
                if ( ($PrdRow->{PRODUCT_STATUS} eq 'Active') and defined($PrdRow->{CUSTOMER_NODE_REF}) ) {
                
                    if ($PrdRow->{IS_SV_PRODUCT} == 1) {
                    
                        ($prodOrderRef, $OneOffChargesRef, $ErrorsRef) = &zProdOrderBuild($PrdRow);
                        push(@Errors, @{$ErrorsRef}) if ($ErrorsRef);
                    } else {
                        # Handle special account products. 
                        1;
                        
                    }
                    
                    if ( defined($prodOrderRef)) {
                        &zDebug('D',"Adding product details: \n" . Dumper($prodOrderRef) . "\n");
                        push(@ProdOrders,$prodOrderRef);
                    } else {
                        &zDebug('D',"No product to add - Account product only\n");
                    }
                }
            }
        }
        # Add the account product if there is anything to add.
        if (%zAccProdOrder) {
            # Set the start date the same as the account. 
            push(@ProdOrders,\%zAccProdOrder);
        }
        
        # Place all orders into the Packet to send to the child process for processing in the billing system. 
        $Packet{PRODUCTS} = \@ProdOrders;
        $Packet{ACCOUNT_NAME} = $accName;
        
        
    } else {
        push(@Errors,"PRODUCT - Unable to execute cursor - CUST_PROD with error: " . $zdb_cly->errstr);
    }
    
    if (@Errors) {
        &zReportErrors(\@Errors, $migrateStart, $accName);
    } else {
        &zDispatchOrder(\%Packet);
    }
    
    return 0;
}

#***************************************************************************
# NAME:
#   zDispatchOrder
#
# DESCRIPTION:
#   Sends the order to a child process to be sent to SV for processing. 
#
# PARAMETERS:
#   OrderPacket reference. 
#
# RETURNS:
#   
#***************************************************************************
sub zDispatchOrder
{
    
    my $packetRef = shift;
    my ($lResult);
    my $accName = $packetRef->{ACCOUNT_NAME};
        
    my $lPid = ataiWaitForFreeChild(0);
    if ($lPid)  {
        $opt_perf && &zDebug("Child process $lPid to process $accName order.");
        # Send the work to child
        &ataiDispatchRequest($lPid, $packetRef);
        
    } else {
        # No free child processes - do blocking wait
        $opt_perf && &zDebug("Waiting for child to be free to accept order");
        ($lPid, $lResult) = ataiWaitForFreeChild(1);
        &zProcessResult($lPid, $lResult);
        # Exit if No Child processes
        last if ( $lPid == $FALSE);

        # Send the work to child
        &ataiDispatchRequest($lPid, $packetRef);
        $znow = localtime;
    }
}

#***************************************************************************
# NAME:
#   zProcessResult
#
# DESCRIPTION:
#   Process the child result
#
# PARAMETERS:
#   Process id of the child
#   result reference
#
# RETURNS:
#   
#***************************************************************************
sub zProcessResult
{   
    my ($status);
    
    my $lPid = shift;
    my $lResult = shift;
    my $accName = $lResult->{ACCOUNT_NAME};
    my $now = localtime;
    my $objId = $lResult->{OBJID};
    my $errStr = $lResult->{ERROR_MESSAGE};
    my $errId;
    
    if ($lResult->{STATUS} eq 'ERROR') {
    
        if ($errStr =~ /<E(\d*)>/) {
             $errId = $1*1;
             print("Found error code: $errId \n");
        } else {
            $errId = 90000;
        }
        if ($opt_migration) {
            print "$now FAILED account: $accName \n";
        } else {
            print "$now FAILED processing event objid = $objId\n";
        }
    } else {
        if ($opt_migration) {
            print "$now Completed account: $accName \n";
        } else {
            print "$now Completed processing event objid = $objId\n";
        }
        # add the account into the existing account hash if it does not exist there. 
        if (not exists $zAllAccountsRef->{$PREFIX_ACC . $accName} and $lResult->{ACTIVE_DATE}) {
            $zAllAccountsRef->{$PREFIX_ACC . $accName}{ACTIVE_DATE} = $lResult->{ACTIVE_DATE};
        }
    }
    
    &zDebug('V',"Child process $lPid completed order for account: ($accName) and objectid: ($objId) ");
    &zDebug('D',"Result from $lPid:" . Dumper($lResult));
    
    &zMergeStats($lResult);        
    
    # In migration mode, add entry in event table to store migration.
    if (not $opt_testing) {
        if ($opt_migration) {
            if ($lResult->{STATUS} eq 'ERROR') {
                $status = ($lResult->{PARTIAL_COMMIT}) ? $STATUS_PART_MIGRATED : $STATUS_ERROR;
                $zCSRs{BE_MIG_INSERT}->execute($accName,$zTaskId,$status,$lResult->{TIME_MS},$errId,substr($errStr,0,3999) ) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
            } else {
                $zCSRs{BE_MIG_INSERT}->execute($accName,$zTaskId,$STATUS_SUCCESS,$lResult->{TIME_MS},undef,undef ) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
            }
            $zdb->commit();
        } elsif ($zModeNormal) {
            if ($lResult->{STATUS} eq 'ERROR') {
                $zCSRs{BE_UPDATE}->execute($STATUS_ERROR,$zTaskId,$lResult->{TIME_MS},$errId,substr($errStr,0,3999),$objId) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
            } else {
                $zCSRs{BE_UPDATE}->execute($STATUS_SUCCESS,$zTaskId,$lResult->{TIME_MS},undef,undef,$objId) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
            }
            $zdb->commit();
        }
    }
}

#***************************************************************************
# NAME:
#   zMockLoad
#
# DESCRIPTION:
#   Extracts data in similar way to the Migration process, 
#   but loads the extracted data into the mock tables. 
#   Supports the same way to select customers as per the migration process. 
#   Also performs the following GDPS masking: 
#   1. Address values are dummied. 
#   2. names use the account name. 
#
# PARAMETERS:
#
# RETURNS:
#   Describe what the subroutine returns.
#***************************************************************************
sub zMockLoad
{
    my ($rc, $CmCnt);
    
    &zSetupClyConnection();
    
    # Setup insert cursors
    &zDebug('D',"Preparing SQL - " . $zSQLs{CUST_INSERT});
    $zCSRs{CUST_INSERT} = $zdb->prepare($zSQLs{CUST_INSERT}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
    &zDebug('D',"Preparing SQL - " . $zSQLs{CUST_OBJECTS_INSERT});
    $zCSRs{CUST_OBJECTS_INSERT} = $zdb->prepare($zSQLs{CUST_OBJECTS_INSERT}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
    &zDebug('D',"Preparing SQL - " . $zSQLs{PROD_INSERT});
    $zCSRs{PROD_INSERT} = $zdb->prepare($zSQLs{PROD_INSERT}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
    &zDebug('D',"Preparing SQL - " . $zSQLs{CM_INSERT});
    $zCSRs{CM_INSERT} = $zdb->prepare($zSQLs{CM_INSERT}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
    &zDebug('D',"Preparing SQL - " . $zSQLs{ADJ_INSERT});
    $zCSRs{ADJ_INSERT} = $zdb->prepare($zSQLs{ADJ_INSERT}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
    
    &zAccountsToProcess() && &zdie($MSG_PASSTHRU, "USAGE: -load_mock requres -site or -filename option to limit accounts to process");
    my $now = localtime;
    print "$now Provided with " . scalar @zAccounts . " accounts to migrate.\n";
    
    # process the account
    for my $accName (@zAccounts) {
        chomp $accName;
        last if (not $accName);
        $now = localtime;
        
        # Load the account
        $zCSRs{CUST}->execute($accName) || &zdie(6001, __LINE__, __FILE__, $zdb_cly->errstr);
        
        my @AccRow = $zCSRs{CUST}->fetchrow_array;
        &zDebug('D',"Fetched ACCOUNT DATA: \n" . Dumper(@AccRow));
        
        # Load fetched account into table, remove the last CHANGE_DATE parameter that is not inserted. 
        
        $zCSRs{CUST_INSERT}->execute(@AccRow) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
        $opt_v && print("Loaded account data - $accName\n");
        
        ### CUSTOMER OBJECTS
        # Load the account
        $zCSRs{CUST_OBJECTS}->execute($accName) || &zdie(6001, __LINE__, __FILE__, $zdb_cly->errstr);
        
        while (my @AccObjRow = $zCSRs{CUST_OBJECTS}->fetchrow_array) {
            &zDebug('D',"Fetched ACCOUNT OBJECTS DATA: \n" . Dumper(@AccObjRow));
        
            # Load fetched account into table:
            $zCSRs{CUST_OBJECTS_INSERT}->execute(@AccObjRow) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
            $opt_v && print("Loaded customer object data for account - $accName\n");
        }
        
        ### CUSTOMER OBJECTS
        # Load the account
        $zCSRs{CUST_ADJ}->execute($accName) || &zdie(6001, __LINE__, __FILE__, $zdb_cly->errstr);
        
        while (my @AccAdjRow = $zCSRs{CUST_ADJ}->fetchrow_array) {
            &zDebug('D',"Fetched ACCOUNT ADJUSTMENT DATA: \n" . Dumper(@AccAdjRow));
        
            # Load fetched account into table:
            $AccAdjRow[5] =~ s/,/./;
            $zCSRs{ADJ_INSERT}->execute(@AccAdjRow) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
            $opt_v && print("Loaded customer adjustment - $AccAdjRow[2] \n");
        } 
        
        ### PRODUCTS ###
        # Load all the products for the account
        $zCSRs{CUST_PROD}->execute($accName) || &zdie(6001, __LINE__, __FILE__, $zdb_cly->errstr);
        
        ### Loop through each top level product to build the orders for all the products. 
        while (my $prdData = $zCSRs{CUST_PROD}->fetchrow_hashref("NAME_uc")) {
            $CmCnt = 0;
            next if ($prdData->{PART_STATUS} eq 'Terminated');
            
            &zDebug('D',"Product Info: \n" . Dumper($prdData));
            my $prodRef = $prdData->{INSTANCE_ID};
            
            $zCSRs{PROD_LOAD}->execute($prodRef) || &zdie(6001, __LINE__, __FILE__, $zdb_cly->errstr);
            
            $zCSRs{PROD}->execute($prodRef) || &zdie(6001, __LINE__, __FILE__, $zdb_cly->errstr);
        
            ### Loop through the products and build the orders including the CM's for each child. 
            my @PrdRow = $zCSRs{PROD}->fetchrow_array();
            &zDebug('D',"Product Data: \n" . Dumper(@PrdRow));
            # remove the dates from the array. 
            
            $zCSRs{PROD_INSERT}->execute(@PrdRow) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
            $opt_v && print("Loaded customer product - $PrdRow[1] \n");
            
            # Load all CM data. 
            $zCSRs{PROD_CM}->execute($prodRef) || &zdie(6001, __LINE__, __FILE__, $zdb_cly->errstr);
            
            ### Store all CM details into a complex hash structure: 
            while (my @RecRow = $zCSRs{PROD_CM}->fetchrow_array) {
                &zDebug('D',"CM Data: \n" . Dumper(@RecRow));
                next if (not $RecRow[1]);
                # Skip the following until design is handled. 
                next if (exists($excludedCM_CODEs{$RecRow[1]}));
                
                ### Only support RCR and NRCD for now. 
                $zCSRs{CM_INSERT}->execute(@RecRow) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
                $CmCnt++;
            }
            
            $opt_v && print("Loaded $CmCnt CM rows \n");
            
        }
        
        # Commit or rollback the changes
        $opt_testing or $zdb->commit();
        $now = localtime;
        print "$now Loading mock tables for account: $accName Completed\n";

    }
    
}

#***************************************************************************
# NAME:
#   zMockClean
#
# DESCRIPTION:
#   Deletes all data from mock tables. 
#
# PARAMETERS:
#
# RETURNS:
#   
#***************************************************************************
sub zMockClean
{
    if ($opt_site) {
        # Setup insert cursors
        &zDebug('D',"Execution SQL - " . $zSQLs{CM_DELETE});
        $zdb->do($zSQLs{CM_DELETE}) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
        &zDebug('D',"Execution SQL - " . $zSQLs{PROD_DELETE});
        $zdb->do($zSQLs{PROD_DELETE}) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
        &zDebug('D',"Execution SQL - " . $zSQLs{CUST_DELETE});
        $zdb->do($zSQLs{CUST_DELETE}) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
        &zDebug('D',"Execution SQL - " . $zSQLs{CUST_OBJ_DELETE});
        $zdb->do($zSQLs{CUST_OBJ_DELETE}) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
        &zDebug('D',"Execution SQL - " . $zSQLs{CUST_ADJ_DELETE});
        $zdb->do($zSQLs{CUST_ADJ_DELETE}) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
        &zDebug('D',"Execution SQL - " . $zSQLs{EVENT_DELETE});
        $zdb->do($zSQLs{EVENT_DELETE}) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
    }
    
    my $now = localtime;
    $opt_testing or $zdb->commit();
    print "$now Mock tables erased for site ($opt_site)\n";
    
}

#***************************************************************************
# NAME:
#   zReportErrors
#
# DESCRIPTION:
#   print the validation errors 
#
# PARAMETERS:
#
# RETURNS:
#   nothing
#***************************************************************************
sub zReportErrors
{
    my $errRef = shift;
    my $eventTime = shift;
    my $objid = shift;
    my $errorMsg = "ERRORS: \n";
    my ($ms, $errId);
    $zStats{VALIDATION_CNT}++;
    
    foreach my $error (@{$errRef}) {
        $errorMsg .= "$error \n";
        # find the first error code and use this for insert. 
        if (not $errId and ($error =~ /<E(\d*)>/) ) {
            $errId = $1*1;
            print("Found error code: $errId \n");
        }
    }
    
    print($errorMsg . "\n");
    
    if (not $opt_testing) {
        $ms = tv_interval($eventTime)*1000;
        $errId = 90000 if (not $errId);
        if ($opt_migration) {
            my $accName = $objid; # This parameter is account name in this use case. 
            $zCSRs{BE_MIG_INSERT}->execute($accName,$zTaskId,$STATUS_ERROR,int($ms),$errId,substr($errorMsg,0,3999) ) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
        } else {
            $zCSRs{BE_UPDATE}->execute($STATUS_ERROR,$zTaskId,int($ms),$errId,substr($errorMsg,0,3999),$objid) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
        }
        $zdb->commit();
    }
    return 0;
}

#***************************************************************************
# NAME:
#   zInsertAudit
#
# DESCRIPTION:
#   Adds the child process result stats to the global stats hash. 
#
# PARAMETERS:
#
# RETURNS:
#   nothing
#***************************************************************************


sub zInsertAudit {

    my $tableName = shift;
    my $objectId = shift; 
    my $valueHashRef = shift;
    my $errRef = shift;

    &zDebug('D',"Inserting into Audit table: " . $tableName . " Values:" .Dumper($valueHashRef));

    my @bindingValues = ( $objectId );      
    
    foreach my $columnName (@{$zTableColumnNames{$tableName}}) {
        push @bindingValues, $valueHashRef->{$columnName};
    }
    
    $zCSRs{$tableName."_INSERT"}->execute(@bindingValues)  || push (@{$errRef},  "AUDIT ERROR: while inserting object with object ID ($objectId) \nSQLERROR:  ". $zdb->errstr);

}


#***************************************************************************
# NAME:
#   zMergeStats
#
# DESCRIPTION:
#   Adds the child process result stats to the global stats hash. 
#
# PARAMETERS:
#
# RETURNS:
#   nothing
#***************************************************************************
sub zMergeStats
{
    my $lResult = shift;
    
    # First handle if the account was created successfully or failed. 
    if ($lResult->{STATUS} eq 'ERROR') {
        $zStats{ACCOUNT_FAIL} += 1;
    }
    
    foreach my $key (keys($lResult->{STATS})) {
        $zStats{$key} += $lResult->{STATS}{$key};
    }

}


#***************************************************************************
# NAME:
#   zPrintStats
#
# DESCRIPTION:
#   Adds the child process result stats to the global stats hash. 
#
# PARAMETERS:
#
# RETURNS:
#   nothing
#***************************************************************************
sub zPrintStats
{
    if ($zStats{EVENT_CNT} + $zStats{ACCOUNT_CNT} + $zStats{PRODUCT_CNT} + $zStats{VALIDATION_CNT} > 0) {
    
        # Prepare stats: 
        $zStats{TOTAL_TRE_SECS} = $zStats{API_ACC_SECS}+$zStats{API_PROD_SECS};
        $zStats{TOTAL_SQL_SECS} = $zStats{ACCOUNT_SQL}+$zStats{CUST_PROD_SQL}+$zStats{PROD_SQL}+$zStats{CM_SQL};
        print "\n\n\t******** EXECUTION SUMMARY ********\n";
        printf("\tNumber of Event Processed:          %18s\n",$zStats{EVENT_CNT}) if ($zStats{EVENT_CNT});
        printf("\tNumber of CLARIFY validation error: %18s\n",$zStats{VALIDATION_CNT}) if ($zStats{VALIDATION_CNT});
        printf("\tNumber of Event rows Skipped:       %18s\n",$zStats{EVENT_SKIPPED}) if ($zStats{EVENT_SKIPPED});
        printf("\tNumber of Accounts Processed:       %18s\n",$zStats{ACCOUNT_CNT});
        printf("\tNumber of Accounts Failed:          %18s\n",$zStats{ACCOUNT_FAIL}) if ($zStats{ACCOUNT_FAIL});
        printf("\tNumber of Accounts Skipped:         %18s\n",$zStats{ACCOUNT_SKIPPED}) if ($zStats{ACCOUNT_SKIPPED});
        printf("\tNumber of Products Processed:       %18s\n",$zStats{PRODUCT_CNT}) if ($zStats{PRODUCT_CNT});
        printf("\tNumber of Products Failed:          %18s\n",$zStats{PRODUCT_FAIL}) if ($zStats{PRODUCT_FAIL});
        printf("\tNumber of Products Skipped:         %18s\n",$zStats{PRODUCT_SKIPPED}) if ($zStats{PRODUCT_SKIPPED});
        printf("\tTotal execution time:               %18.2f Seconds\n",$zStats{TOTAL_TIME});
        printf("\tTotal CLARIFY time:                 %18.2f Seconds\n",$zStats{TOTAL_SQL_SECS});
        printf("\tTotal Singleview time (Child):      %18.2f Seconds\n",$zStats{TOTAL_TRE_SECS});
        printf("\tLead Singleview time:               %18.2f Seconds\n",$zStats{TOTAL_TIME} - $zStats{TOTAL_SQL_SECS});
        
        print "\n\t******** FETCHING SUMMARY ********\n";
        if ($zStats{ACCOUNT_CNT} > 0) {
            printf("\tAccounts Fetch time:                %18.2f Seconds\n",$zStats{ACCOUNT_SQL});
            printf("\tAccounts Fetch average:             %18.2f Seconds\n",$zStats{ACCOUNT_SQL}/$zStats{ACCOUNT_CNT});
            if (not $zModeNormal) {
                printf("\tTop Level Product Fetch time:       %18.2f Seconds\n",$zStats{CUST_PROD_SQL});
                printf("\tTop Level Product Fetch average:    %18.2f Seconds\n",$zStats{CUST_PROD_SQL}/$zStats{ACCOUNT_CNT});
            }
        }
        
        printf("\tProduct Fetch count:                %18s\n",$zStats{PROD_SQL_CNT});
        if ($zStats{PROD_SQL_CNT} > 0) {
            printf("\tProduct Fetch time:                 %18.2f Seconds\n",$zStats{PROD_SQL});
            printf("\tProduct Fetch average:              %18.2f Seconds\n",$zStats{PROD_SQL}/$zStats{PROD_SQL_CNT});
        }
        printf("\tCharge Mechanism Fetch count:       %18s\n",$zStats{CM_SQL_CNT});
        if ($zStats{CM_SQL_CNT} > 0) {
            printf("\tCharge Mechanism Fetch time:        %18.2f Seconds\n",$zStats{CM_SQL});
            printf("\tCharge Mechanism Fetch average:     %18.2f Seconds\n",$zStats{CM_SQL}/$zStats{CM_SQL_CNT});
        }
        
        print "\n\t******** SINGLEVIEW SUMMARY ********\n";
        printf("\tAccount maintenance API calls:      %18s\n",$zStats{API_ACC_CNT});
        if ($zStats{API_ACC_CNT} > 0) {
            printf("\tAccount maintenance API time:       %18.2f Seconds\n",$zStats{API_ACC_SECS});
            printf("\tAccount maintenance API average:    %18.2f Seconds\n",$zStats{API_ACC_SECS}/$zStats{API_ACC_CNT});
        }
        printf("\tFlexi Service API calls:            %18s\n",$zStats{API_PROD_CNT});
        if ($zStats{API_PROD_CNT} > 0) {
            printf("\tFlexi Service API time:             %18.2f Seconds\n",$zStats{API_PROD_SECS});
            printf("\tFlexi Service API average:          %18.2f Seconds\n",$zStats{API_PROD_SECS}/$zStats{API_PROD_CNT});
        }
        printf("\tOne-off Charge API calls:           %18s\n",$zStats{API_CHG_CNT});
        printf("\tOne-off Charges skipped:            %18s\n",$zStats{CM_CHG_SKIPPED}) if ($zStats{CM_CHG_SKIPPED} > 0);
        if ($zStats{API_CHG_CNT} > 0) {
            printf("\tOne-off charge API time:            %18.2f Seconds\n",$zStats{API_CHG_SECS});
            printf("\tOne-off charge API average:         %18.2f Seconds\n",$zStats{API_CHG_SECS}/$zStats{API_CHG_CNT});
        }
        
        print("\n\t*************************************\n\n");
    } else {
        print("\nNo Accounts processed. Nothing to report\n");
    }
    
    return 0;

}

#***************************************************************************
# NAME:
#   zCreateVAS
#
# DESCRIPTION:
#   Creates the VAS account if it does not exist. 
#
# PARAMETERS:
#
# RETURNS:
#   AccountOrder Hash
#***************************************************************************
sub zCreateVAS
{
    my (%lAccountOrder, %lAccountDetails, %lContact, %lAddress, %lResponse);
    
    # Check if the VAS account does not exits. 
    my @row_ary = $zdb->selectrow_array('SELECT 1 FROM ACCOUNT WHERE ACCOUNT_NAME = \'PBAVAS\'');
    if ($zdb->errstr) {
        &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
    }
    
    if ($row_ary[0] == 1) {
        print("WARNING: VAS Account (PBAVAS) is already created. Nothing to do\n");
        return 0;
    }
    
    # Build the account maintain Order.
    $lAccountOrder{SOURCE_SYSTEM}                       = $SOURCE;
    $lAccountOrder{ExternalAccountNumber}               = 'PBAVAS';
    $lAccountOrder{ACCOUNT_HISTORY}[0]{DeactivateFlag}  = $FALSE;
    $lAccountOrder{ACCOUNT_HISTORY}[0]{FromDate}        = &zToSvDate($zMigrationDate);
    $lAccountOrder{ACCOUNT_HISTORY}[0]{ToDate}          = $MAX_DATE;
    
    # Build account history structure
    $lAccountDetails{ACCOUNTING_METHOD}             = $OPEN_ITEM;
    $lAccountDetails{ACCOUNT_NAME}                  = 'PBAVAS'; 
    $lAccountDetails{ACCOUNT_STATUS_NAME}           = 'Active';
    $lAccountDetails{ACCOUNT_TYPE_NAME}             = $ACCOUNT_TYPE_NAME;
    $lAccountDetails{BILL_CYCLE_ID}                 = $zBR_Reseller;
    $lAccountDetails{CREDIT_LIMIT}                  = 100000; #TODO
    $lAccountDetails{CURRENCY_ID}                   = $CURRENCY_ID;
    $lAccountDetails{CUSTOMER_CLASSIFICATION_NAME}  = 'TODO';
    $lAccountDetails{INVOICE_FORMAT_NAME}           = $INV_VAS_FORMAT;
    #$lAccountDetails{PAYMENT_METHOD_NAME}           = ($lAccRef->{PAYMENT_METHOD} eq 'Direct Debit') ? $PAY_METHOD_DD : $PAY_METHOD_GIRO;
    $lAccountDetails{REPORT_LEVEL_NAME}             = "Invoice"; 
    $lAccountDetails{SHADOW_CREDIT_LIMIT}           = 50000;
    $lAccountDetails{TAX_CLASS_CODE}                = $TAX_BUS_0; 
    
    $lAddress{LINE1} = 'Dummy';
    $lAccountDetails{SITE_ADDRESS} = \%lAddress;
   
    # Contact details
    $lContact{CONTACT_STATUS_CODE}  = $CONTACT_STATUS_CODE;
    $lContact{CONTACT_TYPE_ID}      = $CONTACT_TYPE_ID;
    $lContact{FAMILY_NAME}          = 'VAS';
    $lContact{OFFICIAL_NAME}        = 'VAS';
    $lContact{POSTAL_ADDRESS}       = \%lAddress;
    
    $lContact{WRITTEN_LANGUAGE_CODE}    = $zRefData{'WRITTEN_LANGUAGE'}{CODES}{'German'}*1;
    
    $lAccountOrder{ACCOUNT_HISTORY}[0]{ACCOUNTDETAILS} = \%lAccountDetails;
    $lAccountOrder{ACCOUNT_HISTORY}[0]{PRIMARYCONTACT} = \%lContact;
    
    ### Open TRE connection
    &ataiTREConnect($opt_u) || &zdie($MSG_PASSTHRU, $errstr);
    
    treBegin($zTransactionTime) || &zdie($MSG_PASSTHRU, "Failed to start TRE transaction with error: $errstr"); 
    
    fH3AT_CRM_MaintainAccount_RW(\%lAccountOrder, \%lResponse);
    
    # handle tuxedo errors, by exit processing this account. 
    if ($errstr) {
        print "fH3AT_CRM_MaintainAccount_RW for (PBAVAS) FAILED with error: $errstr with order: \n" . Dumper(%lAccountOrder) . "\n";
        treRollback() || &zdie($MSG_PASSTHRU, "Failed to rollback TRE transaction with error: $errstr");
    } else {
    
        # debug mode we make no changes to DB
        if ($opt_testing) {
            # rollback in debug mode
            &zDebug('D',"Rolling back transaction in testing mode. ");
            treRollback() || &zdie($MSG_PASSTHRU, "Failed to rollback TRE transaction with error: $errstr");

        } else {  
            # Commit the changes in normal mode. 
            treCommit() || &zdie($MSG_PASSTHRU, "Failed to commit TRE transaction with error: $errstr");
        }
    }
    
}

#***************************************************************************
# NAME:
#   END
#
# DESCRIPTION:
#   Executed when the script exits.
#
#   Rolls back the current database transaction if we are exiting without
#   having explicitly disconnected from the database.
#
# PARAMETERS:
#   None
#
# RETURNS:
#   Nothing
#***************************************************************************
END {
    $zdb->rollback if $zdb;
} #end END


#***************************************************************************
# NAME:
#   zdie
#
# DESCRIPTION:
#   Called to end (exit) a program due to some error.
#
# PARAMETERS:
#   $l_errcode - Error code to pass to ataierr
#   @l_errparams - Parameters to be passed to the error message
#
# RETURNS:
#   Never.  Exits with error code 1.
#***************************************************************************
sub zdie {
    my($l_errcode, @l_errparams) = @_;

    if ($l_errcode) {
        &ataiLogMsg($l_errcode, @l_errparams);
    }
    $znow = localtime;
    &ataiLogMsg($STAT_PROC_END, $MODULE_NAME, $znow);
    if ($opt_debug) {
        use Carp;
        # Don't output the call to zdie in the stack trace
        $Carp::CarpLevel = 1;
        confess('Fatal error');
        # Don't get here as confess calls die
    }
    exit(1);
} #end zdie


#***************************************************************************
# NAME:
#   zusage
#
# DESCRIPTION:
#   Prints usage details.
#
# PARAMETERS:
#   None
#
# RETURNS:
#   Never.  Exits with error code 2.
#***************************************************************************
sub zusage {

    &GetOptions("debug");

    print STDERR <<EOS;
$MODULE_NAME <task_id> <effective_start_date> [-migration] [-debug]
    
  COMMON PARAMETERS / OPTIONS
    task_id                     Task ID of task running this process. Used for tracking against processed event billing events and orders. 
    effective_start_date        Effective start date of task running this process. Not used. 
    -child <num_of_child>       Multi-processing setting. If not provided will use the configuration item setting of PLANB seqnr = 1.
    -cly_connect <Connection string> This allows for the connection string to clarify to be provided to override the configured value to support switching environments for testing. 
    -schedule_id <schedule_id>  To use for all customers created/migrated
    -force_commit               During migration mode only, this flag will cause commit after each Singleview call to support very large hierarchies. 
                                Note that on any error, then there will be a message in the log that the customer was NOT successfully migrated and needs to be purged before tried again. 
    -tran_time <seconds>        Optionally overrides the default transaction time from configuration item PLANB1 if needed for large account migrations. 
    -debug                      Debug mode.  Lots of extra DEBUG output. provide with -? for hidden testing options.
    -filename <filename>        The filename that lists the site/account_names (one per line in file) that need to migrated to the target system. Or limit actions to.
    
  SPECIAL MODES
    -migration              With this flag (different schedule) the expected parameters are different. 
$MODULE_NAME  <task_id> <effective_start_date> -migration -filename <filename> [-debug] [-mock] [-site <CRM_REF>]
    
EOS

    if ($opt_debug) {
        print STDERR <<EOS;
        
        Extra debugging options: [-prefix <PREFIX>] [-site <CRM_REF>] [-objid <number>] [-mock_clean] [-v] [-perf] [-mock] [-testing] [-load_mock]
            -load_mock              This is a special mode that will extract data from Clarify connection and populate the MOCK tables to be used 
                                    will also anonymise the data. Supported with the -site flag. 
            -mock_clean             Empties the mock tables. 
            -site <CRM_REF>         If this is provided, then limit all changes to the provided bill site only.
            -testing                In this mode changes to the DB are made. all are rolled back. for testing only. 
            -v                      Verbose mode. More detailed output, but less than Debug, can be used with -debug. 
            -perf                   Performance mode. Extra output with detailed timing of each API and SQL to allow for slow querries / API calls to be identified.
            -objid                  The number to use to only extact BILLING_EVENT entries where the objid is greater than provided number. This is only for first execution of normal run.
            -mock                   In this mode, use Singleview mock tables as sournce of data instead of the Clarify tables. not compatible with -load_mock
            -only_mig               If this is provided, then accounts will not be created if they do not existing in Singleview based on events that are processed.
EOS
    }

    exit(2);
} #end zusage


#---------------------------------------------------------------------------
#       MAIN CODE
#---------------------------------------------------------------------------

### Parse Command Line Parameters
#
if ( ($ARGV[0] eq "-?") || ($ARGV[0] eq "-h") || (!&GetOptions("mock", "site=s", "migration", "filename=s", "testing", "load_mock","debug", 
        "v", "u=s", "d", "child=i", "objid=i", "mock_clean", "only_mig", "perf=i", "vas", "cly_connect=s", "schedule_id=i", "force_commit", "tran_time=i")) ) {
    &zusage;
}

### Log a start message
#
$znow = localtime;
&ataiLogMsg($STAT_PROC_START, $MODULE_NAME, $znow);
my $totStartTime = [gettimeofday];
### Open a database connection
#
$zdb = &ataiDbOpen($opt_u) || &zdie($MSG_PASSTHRU, $errstr);


# script can be called directly and not from Singleview. 
# In this case no effective date is provided and current date will be used to limit changes to form billing table to use. 

### Initialize
&zInit();

# If -vas is provided, we will check and created the VAS account if it does not exist. Then exit the script. 
if ($opt_vas) {
    &zCreateVAS();
    $znow = localtime;
    &ataiLogMsg($STAT_PROC_END, $MODULE_NAME, $znow);
    exit 0;
}


# Need to disconnect from DB and TRE before spawning children. 
my $modeMockSetup = ($opt_load_mock or $opt_mock_clean);

if (not $modeMockSetup ) {
    
    $zdb->disconnect();
    
    $opt_v && print "Creating $zNumStreams child processes\n";
    ### Spawn child processes. 
    SelfLoader->_load_stubs('ataiTRE');
    my $zRetVal = ataiSpawnChildren($zNumStreams, \&zInitChild, \&zChildWork, \&zChildExit, \&zSignalHandler, 0);
    $zdb = &ataiDbOpen($opt_u) || &zdie($MSG_PASSTHRU, $errstr);
    
    # Open PARENT log file for logging debug and errors with data and processing.
    # Get the current hour, minute, second.
    if ($opt_debug or $opt_v) {
        my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime;
        my ($yyyymmddhhmiss) = (sprintf("%04d%02d%02d%02d%02d%02d", $year, $mon, $mday, $hour, $min, $sec));
        my $lFilename = $MODULE_NAME . "_" . $yyyymmddhhmiss;
        $zLogFile = $ENV{ATA_DATA_SERVER_LOG} . '/' . $lFilename . '.log';
        open(LOGFILE,">",$zLogFile) or zdie($MSG_PASSTHRU,"Cannot open file $zLogFile"."'$errstr'");
        print "PARENT debug/verbose info written to file: $zLogFile\n";
    }
    
}

# Connect to source database for extraction of data. 
### Connect to clarify if needed. 
if ($opt_mock) {
    # In mock mode, we connect to Singleview and use the mock tables. 
    $zdb_cly = &ataiDbOpen() || &zdie($MSG_PASSTHRU, $errstr);
} else {
    $zdb_cly = &ataiDbOpen($zConnectString) || &zdie($MSG_PASSTHRU, $errstr);
}


### Mode processing. 
if ($opt_migration) {
    ### 1. Migration mode
    ### In this mode, it will determine all sites in clarify not loaded into the target and fetch them to be loaded. 
    ### Note for testing, this mode should be used with the -site option or -site_file option to limit the sites that will be loaded. 
    &zPrepareSvCursors();
    
    # get all the accounts that exist
    &zFetchAccounts();
    
    # perform the migration
    &zMigrateAccounts();
    
    # Wait for everything to finish. 
    &zWaitForChildren();
    
    &ataiShutdownChildren();
    
} elsif ($opt_load_mock) {
    ### 2. loading mock tables mode:
    ### In this mode, we load the customers and details into the mocking tables instead of into the system. Same other options to migration mode. 
    &zMockLoad();
} elsif ($opt_mock_clean) {
    ### 2. loading mock tables mode:
    ### In this mode, we load the customers and details into the mocking tables instead of into the system. Same other options to migration mode. 
    &zMockClean();
    
} else {
    ### 4. Normal processing mode
    ### 4.1 set up needed cursors to CLY or LOCAL depending on -mock flag.
    ### 4.2 fetch all billing events not processed yet and move to local table to be processed. 
    ###     Skip this step in mocking mode which expects the local table to be updated by another process.
    ### 4.3 Process all the events for account insert / update first. 
    ### 4.4 Process all the events for product changes. 
    $zModeNormal = 1;
    
    &zSetupClyConnection();
    
    # Prepare cursors for SV connection
    &zPrepareSvCursors();
    
    # get all the accounts that exist
    &zFetchAccounts();
    
    # Fetch events that we need to process from Clarify table and load into local AUDIT table. 
    &zProcessEvents() if ( not $opt_mock);
    
    ### Customer requests are done first.
    &zProcessAccounts();
    
    ### Process Product updates
    &zProcessProducts();
    
    ### Create all needed adjustments that were found. 
    &zProcessAdjustments();
    
    &ataiShutdownChildren();
    
}

### Print out processing stats
$zStats{TOTAL_TIME} += tv_interval($totStartTime);
if (not $modeMockSetup ) {
    &zPrintStats();
}

### Log an end message
#
$znow = localtime;
&ataiLogMsg($STAT_PROC_END, $MODULE_NAME, $znow);

exit 0;

#
#       End (of H3AT_PlanBEngine.pl)
#
