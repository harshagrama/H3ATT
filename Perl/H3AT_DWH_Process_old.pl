#!/usr/bin/env perl
#---------------------------------------------------------------------------
#
#   File:       H3AT_DWH_Process.pl
#   Created:    30-Sep-2019 02:58:11
#   Creator:    t_keirench  (Admin Christophe Keirens)
#   $Revision: 2.16 $
#   $Id: H3AT_DWH_Process.pl,v 2.16 2020/02/27 12:43:26 t_nagavaha Exp $
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
#   H3AT_DWH_Process
#
# DESCRIPTION:
#   Populates the tables that are used by the Tele2 Data warehouse platform. 
#
# EXIT STATUS:
#   0       - Succeeded
#   1       - Error (+ Description)
#   2       - Usage
#
# REVISION HISTORY:
#   $Log: H3AT_DWH_Process.pl,v $
#   Revision 2.16  2020/02/27 12:43:26  t_nagavaha
#   Ticket CF-9024: Converting decimal seperator from "." to ","
#
#   Revision 2.15  2020/02/27 07:48:48  t_nagavaha
#   Ticket CF-9028: Removed Process Type Customer
#
#   Revision 2.14  2020/02/27 07:47:08  t_nagavaha
#   Removed Process Type Customer, instead a view is provided.
#
#   Revision 2.13  2020/02/11 14:59:43  t_nagavaha
#   Ticket CF-8463: PRODUCTS currently only support the following products H3ATT2_NUC_Flex_NonProYear, H3ATT2_NUC_Flex_ProAdvMon, H3ATT2_NUC_Flex_ProArrMon and H3AT_pGenericBusPackage
#
#   Revision 2.12  2020/02/04 15:15:38  t_nagavaha
#   Ticket CF-8463: Updated zusage
#
#   Revision 2.11  2020/02/04 14:47:29  t_nagavaha
#   Ticket CF-8463: All the code review comments addressed
#
#   Revision 2.9  2020/02/03 13:15:41  t_nagavaha
#   Ticket CF-8463: Fixed couple of issues after verifying data from production
#
#   Revision 2.8  2020/02/03 09:05:14  t_nagavaha
#   Ticket CF-8463: Query re-write discount products
#
#   Revision 2.7  2020/01/31 16:04:57  t_nagavaha
#   Ticket CF-8463: Added a where clause with last_modified date for discount product_instance_history fetch
#
#   Revision 2.6  2020/01/31 13:06:07  t_nagavaha
#   Ticket CF-8463: Adding Process Type Product
#
#   Revision 2.4  2019/11/26 15:23:18  ghazizra
#   Ticket CF-8312: Added handling of CUSTOMER_NODE_INV_FORMAT table.
#
#   Revision 2.3  2019/11/26 13:05:41  ghazizra
#   Ticket CF-8312: Added Support for process type Customer.
#
#   Revision 2.2  2019/10/22 06:35:08  t_keirench
#   Ticket CF-7674: code review comments
#
#   Revision 2.1  2019/10/18 08:14:21  t_keirench
#   Ticket CF-7674: Initial version - prototype
#
#---------------------------------------------------------------------------

#---------------------------------------------------------------------------
#       CORE PERL LIBRARIES
#---------------------------------------------------------------------------
use strict;
use FileHandle;
use Getopt::Long;

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

my $MODULE_NAME         = "H3AT_DWH_Process";
my $TYPE_EVENT          = 10;       # From RT - H3ATT_DWH_PROCESS_TYPE
my $TYPE_CUSTOMER       = 20;       # From RT - H3ATT_DWH_PROCESS_TYPE
my $TYPE_CONFIGURATION  = 30;       # From RT - H3ATT_DWH_PROCESS_TYPE
my $TYPE_INVOICE        = 40;       # From RT - H3ATT_DWH_PROCESS_TYPE
my $TYPE_PRODUCT        = 50;       # From RT - H3ATT_DWH_PROCESS_TYPE
my $EC_USAGE            = 1;        # Event class for usgae charges.
my $EC_RECURRING        = 2;        # Event class for recurring charges. 
my $EC_ONEOFF           = 3;        # Event class for oneoff charges.
my $NEF_ONEOFF          = 4400001;  # File type id - H3G One Off Charge Events

 
#---------------------------------------------------------------------------
#       PRIVATE GLOBAL VARIABLES
#---------------------------------------------------------------------------

use vars qw($zdb);  # Database handle
my $znow = "";      # Current time string
my %zSQLs;          # stores the DB cursors
my %zCSRs;          # Global cursors  
my %zRefData;       # stores the reference type data
my %zTabColumns;    # stored all the insert into table columns. 
my %zMapCache;      # The hash structure used to map data to the output fields. 
my %zStats;         # Has that stores statistics
my $zType;          # The type of process which dictates what data is extracted and populated.
my $zTypeName;
my $zTaskId;
my $zEffDate;
my $zNumStreams;
my $zMigrationDate;
my $zBR_Business;
my $zBR_Reseller;
my $zMigSortDate;
my $zLastExecDate;
my $zParentPid = $$;


#---------------------------------------------------------------------------
#       GETOPT VARIABLES
#---------------------------------------------------------------------------

use vars qw(
    $opt_debug
    $opt_v
    $opt_u
    $opt_d
    $opt_child
    $opt_testing
    $opt_site
);


#---------------------------------------------------------------------------
#       PRIVATE SUBROUTINES
#---------------------------------------------------------------------------

#***************************************************************************
# NAME:
#   zInit
#
# DESCRIPTION:
#   Initialises the script performing the following tasks: 
#   - Create DB and TRE connections to Source DB systesm
#   - Handle the provided parameters - Process Type
#   - Fetches system specific configuration item settings
#   - Caches needed configuration data for performance. 
#   - Sets up needed cursors for this process type
#
# PARAMETERS:
#   
#
# RETURNS:
#   0 or dies
#***************************************************************************
sub zInit
{
    ### Open a database connection
    #
    $zdb = &ataiDbOpen($opt_u) || &zdie($MSG_PASSTHRU, $errstr);
    # set the date format for the session as per the NATIVE FORMAT
    $zdb->do('ALTER SESSION set NLS_DATE_FORMAT = \'DD-MM-YYYY HH24:MI:SS\'') or die $zdb->errstr;
    
    ### Open TRE connection
    &ataiTREConnect($opt_u) || &zdie($MSG_PASSTHRU, $errstr);
    
    ### process input parameters
    $zTaskId    = $ARGV[0];
    $zEffDate   = $ARGV[1];
    $zType      = $ARGV[2];
    
    if (length($zType) == 0) { 
        &zusage();
    }
    
    
    $opt_testing && print("TESTING MODE - No change will be commited to the Database\n");
    
    
    # Cache reference types
    &zCacheRefData('H3ATT_DWH_PROCESS_TYPE');
    
    # Validate parameters. 
    if (not $zRefData{'H3ATT_DWH_PROCESS_TYPE'}{$zType}) {
        &zusage;
    } else {
        $zTypeName = $zRefData{'H3ATT_DWH_PROCESS_TYPE'}{$zType};
    }
    
    # Cache Mapping table
    my ($data,$tableName,$columnName,$inputVal,$outputVal);
    
    # Fetch the mapping table content.
    $data = &biDerivedTableContents('dH3ATT_DWH_Mapping', $zEffDate);
    
    # Loop through the da table and store the content. 
    foreach my $row (@{$data}) {
        $tableName  = $row->[0];
        $columnName = $row->[1];
        $inputVal   = $row->[2];
        $outputVal  = $row->[3];
        # Cache the entry for mapping logic
        $zMapCache{$tableName}{$columnName}{$inputVal} = $outputVal;
    }
    $opt_debug && zDebug("Configuration Item settings: " . Dumper(%zMapCache));
    
    # Set up Fetch and insert cursors
    &zBuildCursors();
    $opt_debug && zDebug("All SQL Cursors: " . Dumper(%zSQLs));
    $opt_debug && zDebug("All Table columns: " . Dumper(%zTabColumns));
    
    # Prepare cursors that will be used for this type. 
    &zPrepareCursors();
    
    # Fetch the last execution date of this type of task. 
    my @row_ary = $zdb->selectrow_array('SELECT MAX(START_DATE) FROM H3ATT_DWH_PROCESS WHERE PROCESS_TYPE = \'' . $zTypeName . '\'');
    if ($zdb->errstr) {
        &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
    }
    $zLastExecDate = $row_ary[0];
    
    # Insert the process start into the H3ATT_DWH_PROCESS table.
    $zCSRs{I_PROCESS}->execute($zTaskId,'Running',$zTypeName) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);


    # Initialise stats
    $zStats{INSERT_CNT}  = 0;
    $zStats{UPDATE_CNT}  = 0;
    $zStats{ERROR_CNT}   = 0;
    
    return 0;

}

#***************************************************************************
# NAME:
#   zBuildCursors
#
# DESCRIPTION:
#   Setup the fetch and insert/update corsors based on the process type
#   
#
# PARAMETERS:
#   
#
# RETURNS:
#   0 or dies
#***************************************************************************
sub zBuildCursors
{
    
    # Insert a new process start. 
    $zSQLs{I_PROCESS} = <<"EOS";
        INSERT INTO H3ATT_DWH_PROCESS
            (TASK_QUEUE_ID,TASK_STATUS,PROCESS_TYPE,START_DATE)
        VALUES
            (?,?,?,SYSDATE)
EOS

    # Update the process status. 
    $zSQLs{U_PROCESS} = <<"EOS";
        UPDATE H3ATT_DWH_PROCESS 
            SET TASK_STATUS = ?,
                END_DATE = SYSDATE,
                ROWS_INSERTED = ?,
                ROWS_UPDATED = ?,
                ERROR_COUNT = ?
          WHERE TASK_QUEUE_ID = ?
EOS

    
    ###########################
    ### FETCH FILES that have not been processed. 
    ### TODO, determine which usage files will be considered. 
    ###########################
    $zSQLs{NE_FILES} = <<"EOS";
    SELECT NORMALISED_EVENT_FILE_ID, 
           FILENAME,
           TASK_QUEUE_ID,
           NORMALISED_EVENT_FT_ID,
           PROCESS_START_DATE,
           PROCESS_END_DATE,
           EVENT_SOURCE,
           EXTERNAL_INPUT_EVENT_COUNT,
           INPUT_EVENT_COUNT,
           RATED_EVENT_COUNT,
           NORM_ERROR_EVENT_COUNT,
           RATING_ERROR_EVENT_COUNT,
           REPROCESSED_EVENT_COUNT,
           REPROCESSED_ERROR_EVENT_COUNT,
           TO_CHAR(MIN_CHARGE_START_DATE,'DD-MM-YYYY HH24:MI:SS') as MIN_CHARGE_START_DATE,
           TO_CHAR(MAX_CHARGE_START_DATE,'DD-MM-YYYY HH24:MI:SS') as MAX_CHARGE_START_DATE
      FROM NORMALISED_EVENT_FILE NEF
     WHERE PROCESS_END_DATE IS NOT NULL
       AND PROCESS_END_DATE >= TO_DATE(?,'DD-MM-YYYY HH24:MI:SS')
       AND NOT EXISTS (SELECT 1 FROM H3ATT_DWH_NEF NEW_NEF WHERE NEF.NORMALISED_EVENT_FILE_ID = NEW_NEF.NORMALISED_EVENT_FILE_ID)
       ORDER BY PROCESS_END_DATE
EOS

    ###########################
    ### FETCH EVENTS that have not been processed. 
    ###########################
    $zSQLs{EVENTS_NEW} = <<"EOS";
    SELECT NE.NORMALISED_EVENT_ID,
           NE.LAST_MODIFIED,
           DECODE(NE.NORMALISED_EVENT_TYPE_ID,4100003,25000080,NE.NORMALISED_EVENT_TYPE_ID) AS NORMALISED_EVENT_TYPE_ID,
           NE.NORMALISED_EVENT_FILE_ID,
           NE.FILE_RECORD_NR,
           NE.EXTERNAL_FILE_RECORD_NR,
           NE.ORIGINAL_FILE_ID,
           NE.ORIGINAL_FILE_RECORD_NR,
           NE.REPROCESSED_COUNT,
           NE.A_PARTY_ID, NE.A_PARTY_NAME, NE.A_PARTY_TON_CODE,A_PARTY_ROUTE,
           NE.B_PARTY_ID, NE.B_PARTY_TON_CODE, NE.B_PARTY_ROUTE,
           DECODE (NE.C_PARTY_LOCATION_CODE,4,6470,NE.C_PARTY_LOCATION_CODE) as C_PARTY_LOCATION_CODE,
           NE.FULL_PATH,
           NE.CHARGE_START_DATE,
           NE.DURATION,
           NE.RATE_BAND,
           NE.BILL_RUN_ID,
           NE.EVENT_CLASS_CODE, NE.EVENT_SUB_TYPE_CODE, NE.EVENT_TYPE_CODE,
           NE.GENERAL_1, 
           NE.GENERAL_2, 
           NE.GENERAL_3, 
           NE.GENERAL_4, 
           NE.GENERAL_5, 
           NE.GENERAL_6, 
           NE.GENERAL_7,
           NE.GENERAL_8, 
           NE.GENERAL_9, 
           NE.GENERAL_10, 
           NE.GENERAL_11, 
           NE.GENERAL_12, 
           NE.GENERAL_13, 
           NE.GENERAL_14, 
           NE.GENERAL_15, 
           NE.GENERAL_16, 
           NE.GENERAL_17, 
           NE.GENERAL_18, 
           NE.GENERAL_19, 
           NE.GENERAL_20,
           C.CHARGE_ID,
           C.LAST_MODIFIED C_LAST_MODIFIED,
           C.CHARGE,
           C.ACCOUNT_ID,
           C.UNINVOICED_IND_CODE,
           C.SERVICE_ID,
           C.CUSTOMER_NODE_ID,
           C.CHARGE_DATE,
           C.TARIFF_ID,
           C.INVOICE_TXT,
           C.INVOICE_ID,
           C.GENERAL_1 as C_GENERAL_1, C.GENERAL_2 as C_GENERAL_2, C.GENERAL_3 as C_GENERAL_3, 
           C.GENERAL_4 as C_GENERAL_4, C.GENERAL_5 as C_GENERAL_5,
           C.GENERAL_6 as C_GENERAL_6, C.GENERAL_7 as C_GENERAL_7, C.GENERAL_8 as C_GENERAL_8, 
           C.GENERAL_9 as C_GENERAL_9, C.GENERAL_10 as C_GENERAL_10,
           PIH.INDUSTRY_GENERAL_2 as PRODUCT_GROUP,
           PIH.INDUSTRY_GENERAL_7 as PRODUCT_CLASS
      FROM NORMALISED_EVENT NE 
      JOIN CHARGE C ON C.NORMALISED_EVENT_ID = NE.NORMALISED_EVENT_ID AND C.CHARGE_DATE = NE.CHARGE_START_DATE
      JOIN ACCOUNT ACC ON C.ACCOUNT_ID = ACC.ACCOUNT_ID AND ACC.GENERAL_2 = 'CLARIFY'
      JOIN SERVICE_HISTORY SH ON C.SERVICE_ID = SH.SERVICE_ID AND C.CHARGE_DATE BETWEEN SH.EFFECTIVE_START_DATE AND SH.EFFECTIVE_END_DATE
      JOIN PRODUCT_INSTANCE_HISTORY PIH ON PIH.PRODUCT_INSTANCE_ID = SH.BASE_PRODUCT_INSTANCE_ID AND C.CHARGE_DATE BETWEEN PIH.EFFECTIVE_START_DATE AND PIH.EFFECTIVE_END_DATE
     WHERE NE.NORMALISED_EVENT_FILE_ID = ?
       AND C.TARIFF_ID != 11000001
       AND NE.CHARGE_START_DATE >= TO_DATE(?,'DD-MM-YYYY HH24:MI:SS')
       AND NE.CHARGE_START_DATE <= TO_DATE(?,'DD-MM-YYYY HH24:MI:SS')
        
EOS

    if ($opt_site) {
        $zSQLs{EVENTS_NEW} .= " AND ACC.ACCOUNT_NAME = \'PBA$opt_site\' ";
    }else{
        $zSQLs{EVENTS_NEW} .= " ORDER BY NE.LAST_MODIFIED ASC, C.CHARGE DESC";
    }

    # Insert event files 
    $zSQLs{I_FILES} = <<"EOS";
    INSERT INTO H3ATT_DWH_NEF 
        (NORMALISED_EVENT_FILE_ID,FILENAME,TASK_QUEUE_ID,NORMALISED_EVENT_FT_ID,PROCESS_START_DATE,PROCESS_END_DATE,EVENT_SOURCE,EXTERNAL_INPUT_EVENT_COUNT,
         INPUT_EVENT_COUNT,RATED_EVENT_COUNT,NORM_ERROR_EVENT_COUNT,RATING_ERROR_EVENT_COUNT,REPROCESSED_EVENT_COUNT,REPROCESSED_ERROR_EVENT_COUNT,MIN_CHARGE_START_DATE,MAX_CHARGE_START_DATE)
    VALUES
        (?,?,?,?,?,?,?,?, ?,?,?,?,?,?,to_date(?,'DD-MM-YYYY HH24:MI:SS'),to_date(?,'DD-MM-YYYY HH24:MI:SS'))
EOS

    
    # insert events on creation. 
    $zSQLs{I_NE} = <<"EOS";
    INSERT INTO H3ATT_DWH_NORMALISED_EVENT 
        (NORMALISED_EVENT_ID,LAST_MODIFIED,NORMALISED_EVENT_TYPE_ID,NORMALISED_EVENT_FILE_ID,FILE_RECORD_NR,A_PARTY_ID,A_PARTY_ROUTE,B_PARTY_ID,C_PARTY_ID,CHARGE_START_DATE,
         PERIOD_START_DATE,PERIOD_END_DATE,EVENT_CLASS_CODE,BILL_RUN_ID,EVENT_TYPE_CODE,EVENT_SUB_TYPE_CODE,DURATION,CHARGE,RATE_BAND,GENERAL_1,
         GENERAL_2,GENERAL_3,GENERAL_4,GENERAL_5,GENERAL_6,GENERAL_7,GENERAL_8,GENERAL_9,GENERAL_10,GENERAL_11,
         GENERAL_12,GENERAL_13,GENERAL_14,GENERAL_15,GENERAL_16,GENERAL_17,GENERAL_18,GENERAL_19,GENERAL_20)
    VALUES
        (?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?)
EOS

    # Store all the insert table column names per table: 
    my @ne_columns = qw(NORMALISED_EVENT_ID LAST_MODIFIED NORMALISED_EVENT_TYPE_ID NORMALISED_EVENT_FILE_ID FILE_RECORD_NR A_PARTY_ID A_PARTY_ROUTE B_PARTY_ID C_PARTY_ID CHARGE_START_DATE 
         PERIOD_START_DATE PERIOD_END_DATE EVENT_CLASS_CODE BILL_RUN_ID EVENT_TYPE_CODE EVENT_SUB_TYPE_CODE DURATION CHARGE RATE_BAND GENERAL_1 
         GENERAL_2 GENERAL_3 GENERAL_4 GENERAL_5 GENERAL_6 GENERAL_7 GENERAL_8 GENERAL_9 GENERAL_10 GENERAL_11 
         GENERAL_12 GENERAL_13 GENERAL_14 GENERAL_15 GENERAL_16 GENERAL_17 GENERAL_18 GENERAL_19 GENERAL_20);
    $zTabColumns{H3ATT_DWH_NORMALISED_EVENT} = \@ne_columns;
        
    # Insert charge records on creation. 
    $zSQLs{I_CHARGE} = <<"EOS";
    INSERT INTO H3ATT_DWH_CHARGE 
        (CHARGE_ID,LAST_MODIFIED,CHARGE,ACCOUNT_ID,UNINVOICED_IND_CODE,NORMALISED_EVENT_ID,SERVICE_ID,CUSTOMER_NODE_ID,CHARGE_DATE,TARIFF_ID,
         INVOICE_TXT,INVOICE_ID,GENERAL_1,GENERAL_2,GENERAL_3,GENERAL_4,GENERAL_5,GENERAL_6,GENERAL_7,GENERAL_8,GENERAL_9,GENERAL_10)
    VALUES
        (?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?,?,?)
EOS

    # Store all the insert table column names per table: 
    my @c_columns = qw(CHARGE_ID LAST_MODIFIED CHARGE ACCOUNT_ID UNINVOICED_IND_CODE NORMALISED_EVENT_ID SERVICE_ID CUSTOMER_NODE_ID CHARGE_DATE TARIFF_ID 
         INVOICE_TXT INVOICE_ID GENERAL_1 GENERAL_2 GENERAL_3 GENERAL_4 GENERAL_5 GENERAL_6 GENERAL_7 GENERAL_8 GENERAL_9 GENERAL_10);
    $zTabColumns{H3ATT_DWH_CHARGE} = \@c_columns;

    # Insert into the emulated customer query table for one-off charges / adjustments. 
    $zSQLs{I_CUST_QUERY} = <<"EOS";
    INSERT INTO H3ATT_DWH_CUSTOMER_QUERY 
        (CUSTOMER_QUERY_ID,LAST_MODIFIED,CUSTOMER_QUERY_NR,CUSTOMER_QUERY_TYPE_ID,CUSTOMER_NODE_ID,SERVICE_ID,QUERY_STATUS_CODE,OPEN_DATE,CLOSE_DATE,RESOLUTION_TEXT,
         GENERAL_1,GENERAL_2,GENERAL_3,GENERAL_4,GENERAL_5,GENERAL_6,GENERAL_9)
    VALUES
        (?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?)
EOS

    # Store all the insert table column names per table: 
    my @cq_columns = qw(CUSTOMER_QUERY_ID LAST_MODIFIED CUSTOMER_QUERY_NR CUSTOMER_QUERY_TYPE_ID CUSTOMER_NODE_ID SERVICE_ID QUERY_STATUS_CODE OPEN_DATE CLOSE_DATE RESOLUTION_TEXT 
         GENERAL_1 GENERAL_2 GENERAL_3 GENERAL_4 GENERAL_5 GENERAL_6 GENERAL_9);
    $zTabColumns{H3ATT_DWH_CUSTOMER_QUERY} = \@cq_columns;

    # Insert into the emulated customer query table for one-off charges / adjustments. 
    $zSQLs{U_CUST_QUERY} = <<"EOS";
    UPDATE H3ATT_DWH_CUSTOMER_QUERY 
        SET GENERAL_8 = ?
        WHERE CUSTOMER_QUERY_ID = ?
EOS

    # RATEPLAN (COMPANION PRODUCT NAME)
    $zSQLs{RATEPLAN} = <<"EOS";
    SELECT PRODUCT_NAME, REPLACE(PRODUCT_DISPLAY_NAME,'H3ATT','dTT_RAT') as PRODUCT_DISPLAY_NAME
      FROM PRODUCT_HISTORY
     WHERE PRODUCT_ID = ?
       AND sysdate BETWEEN EFFECTIVE_START_DATE AND EFFECTIVE_END_DATE
EOS
    
    #CRM_REF 
    $zSQLs{CRM_REF} = <<"EOS";
    SELECT GENERAL_10 as CRM_REF
      FROM PRODUCT_INSTANCE_HISTORY PIH
     WHERE PRODUCT_INSTANCE_ID = ?
       AND sysdate BETWEEN PIH.EFFECTIVE_START_DATE AND PIH.EFFECTIVE_END_DATE
EOS

    ############################################
    # PRODUCT_INSTANCE_HISTORY
    # EXCLUDING DISCOUNT PRODUCT_INSTANCE RECORDS
    ############################################
    
    # Store all the insert table column names per table:
    my @pih_columns = qw(PRODUCT_INSTANCE_ID LAST_MODIFIED ATLANTA_OPERATOR_ID EFFECTIVE_START_DATE EFFECTIVE_END_DATE PRODUCT_ID PRODUCT_INSTANCE_STATUS_CODE 
                         CUSTOMER_NODE_ID BASE_PRODUCT_INSTANCE_ID CONTRACT_ID FROM_PRODUCT_INSTANCE_ID FROM_PRODUCT_ID TO_PRODUCT_INSTANCE_ID TO_PRODUCT_ID 
                         UNCOMPLETED_IND_CODE GENERAL_1 GENERAL_2 GENERAL_3 GENERAL_4 GENERAL_5 GENERAL_6 GENERAL_7 GENERAL_8 GENERAL_9 GENERAL_10 
                         ACTIVE_DATE PRODUCT_BUNDLE_ID INDUSTRY_GENERAL_1 INDUSTRY_GENERAL_2 INDUSTRY_GENERAL_3 INDUSTRY_GENERAL_4 INDUSTRY_GENERAL_5 
                         INDUSTRY_GENERAL_6 INDUSTRY_GENERAL_7 INDUSTRY_GENERAL_8 INDUSTRY_GENERAL_9 INDUSTRY_GENERAL_10);
    $zTabColumns{H3ATT_DWH_PROD_INST_HIST} = \@pih_columns;
    
    #################################################################
    # Fetch product instance history rows for the following products:
    #       H3ATT2_NUC_Flex_NonProYear, H3ATT2_NUC_Flex_ProAdvMon
    #       H3ATT2_NUC_Flex_ProArrMon, H3AT_pGenericBusPackage
    #################################################################
    
    $zSQLs{PIH_NEW} = <<"EOS";
    SELECT 
        PIH.PRODUCT_INSTANCE_ID,          
        PIH.LAST_MODIFIED,          
        PIH.ATLANTA_OPERATOR_ID,         
        PIH.EFFECTIVE_START_DATE,        
        PIH.EFFECTIVE_END_DATE,       
        PIH.PRODUCT_ID,                   
        PIH.PRODUCT_INSTANCE_STATUS_CODE, 
        PIH.CUSTOMER_NODE_ID,             
        PIH.BASE_PRODUCT_INSTANCE_ID,     
        PIH.CONTRACT_ID,                  
        PIH.FROM_PRODUCT_INSTANCE_ID,     
        PIH.FROM_PRODUCT_ID,              
        PIH.TO_PRODUCT_INSTANCE_ID,       
        PIH.TO_PRODUCT_ID,                
        PIH.UNCOMPLETED_IND_CODE,         
        PIH.GENERAL_1,                    
        PIH.GENERAL_2,                    
        PIH.GENERAL_3,                    
        PIH.GENERAL_4,                    
        PIH.GENERAL_5,                    
        PIH.GENERAL_6,                    
        PIH.GENERAL_7,                    
        PIH.GENERAL_8,                    
        PIH.GENERAL_9,                    
        PIH.GENERAL_10,                   
        PIH.ACTIVE_DATE,                        
        PIH.INDUSTRY_GENERAL_1,          
        PIH.INDUSTRY_GENERAL_2,           
        PIH.INDUSTRY_GENERAL_3,          
        PIH.INDUSTRY_GENERAL_4,           
        PIH.INDUSTRY_GENERAL_5,           
        PIH.INDUSTRY_GENERAL_6,           
        PIH.INDUSTRY_GENERAL_7,           
        PIH.INDUSTRY_GENERAL_8,           
        PIH.INDUSTRY_GENERAL_9,           
        PIH.INDUSTRY_GENERAL_10 
    FROM PRODUCT_INSTANCE_HISTORY PIH
    JOIN ACCOUNT ACC ON PIH.CUSTOMER_NODE_ID = ACC.CUSTOMER_NODE_ID AND ACC.GENERAL_2 = 'CLARIFY'
    WHERE 1=1
    AND (PIH.LAST_MODIFIED >= TO_DATE(?,'DD-MM-YYYY HH24:MI:SS') OR (NOT EXISTS (SELECT 1 FROM H3ATT_DWH_PROD_INST_HIST PIH_DWH WHERE PIH_DWH.PRODUCT_INSTANCE_ID = PIH.PRODUCT_INSTANCE_ID)))
    AND PIH.PRODUCT_ID IN (31000882,31000922,31000881,31000921)
    
EOS

    if ($opt_site) {
        $zSQLs{PIH_NEW} .= " AND ACC.ACCOUNT_NAME = \'PBA$opt_site\' ";
    }

    # Insert rows into the H3ATT_DWH_PROD_INST_HIST
    $zSQLs{I_PIH} = <<"EOS";
    INSERT INTO H3ATT_DWH_PROD_INST_HIST
        (PRODUCT_INSTANCE_ID,          
            LAST_MODIFIED,          
            ATLANTA_OPERATOR_ID,         
            EFFECTIVE_START_DATE,        
            EFFECTIVE_END_DATE,       
            PRODUCT_ID,                   
            PRODUCT_INSTANCE_STATUS_CODE, 
            CUSTOMER_NODE_ID,             
            BASE_PRODUCT_INSTANCE_ID,     
            CONTRACT_ID,                  
            FROM_PRODUCT_INSTANCE_ID,     
            FROM_PRODUCT_ID,              
            TO_PRODUCT_INSTANCE_ID,       
            TO_PRODUCT_ID,                
            UNCOMPLETED_IND_CODE,         
            GENERAL_1,                    
            GENERAL_2,                    
            GENERAL_3,                    
            GENERAL_4,                    
            GENERAL_5,                    
            GENERAL_6,                    
            GENERAL_7,                    
            GENERAL_8,                    
            GENERAL_9,                    
            GENERAL_10,                   
            ACTIVE_DATE,
            PRODUCT_BUNDLE_ID,    
            INDUSTRY_GENERAL_1,          
            INDUSTRY_GENERAL_2,           
            INDUSTRY_GENERAL_3,          
            INDUSTRY_GENERAL_4,           
            INDUSTRY_GENERAL_5,           
            INDUSTRY_GENERAL_6,           
            INDUSTRY_GENERAL_7,           
            INDUSTRY_GENERAL_8,           
            INDUSTRY_GENERAL_9,           
            INDUSTRY_GENERAL_10)
        VALUES
        (?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,? )
EOS

# Delete old entries for an updated product instance 
    $zSQLs{D_PIH} = <<"EOS";
    DELETE FROM H3ATT_DWH_PROD_INST_HIST
     WHERE PRODUCT_INSTANCE_ID = ?
       AND EFFECTIVE_START_DATE = TO_DATE(?,'DD-MM-YYYY HH24:MI:SS')
EOS

# Fetch Discount Product_Instance_History Record
    $zSQLs{DSC_NEW} = <<"EOS";
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
        PIH.GENERAL_10
    FROM  PRODUCT_INSTANCE_HISTORY PIH
    JOIN ACCOUNT ACC ON PIH.CUSTOMER_NODE_ID = ACC.CUSTOMER_NODE_ID AND ACC.GENERAL_2 = 'CLARIFY'
    JOIN PRODUCT_HISTORY PH ON PIH.PRODUCT_ID=PH.PRODUCT_ID AND PH.PRODUCT_DISPLAY_NAME like '%Discount%'
    WHERE 1=1
    AND PIH.LAST_MODIFIED >= TO_DATE(?,'DD-MM-YYYY HH24:MI:SS')
    
EOS
    
    return 0;
    
}

#***************************************************************************
# NAME:
#   zPrepareCursors
#
# DESCRIPTION:
#   Prepares the needed cursors based on the type of operation. 
#   
#
# PARAMETERS:
#   
#
# RETURNS:
#   0 or dies
#***************************************************************************
sub zPrepareCursors
{
    
    $opt_debug && &zDebug("Preparing SQL - " . $zSQLs{I_PROCESS});
    $zCSRs{I_PROCESS} = $zdb->prepare($zSQLs{I_PROCESS}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
    
    $opt_debug && &zDebug("Preparing SQL - " . $zSQLs{U_PROCESS});
    $zCSRs{U_PROCESS} = $zdb->prepare($zSQLs{U_PROCESS}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
    
    if ($zType eq $TYPE_EVENT) {
    
        $opt_debug && &zDebug("Preparing SQL - " . $zSQLs{NE_FILES});
        $zCSRs{NE_FILES} = $zdb->prepare($zSQLs{NE_FILES}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
    
        $opt_debug && &zDebug("Preparing SQL - " . $zSQLs{EVENTS_NEW});
        $zCSRs{EVENTS_NEW} = $zdb->prepare($zSQLs{EVENTS_NEW}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
    
        $opt_debug && &zDebug("Preparing SQL - " . $zSQLs{I_FILES});
        $zCSRs{I_FILES} = $zdb->prepare($zSQLs{I_FILES}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
        
        $opt_debug && &zDebug("Preparing SQL - " . $zSQLs{I_NE});
        $zCSRs{I_NE} = $zdb->prepare($zSQLs{I_NE}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
        
        $opt_debug && &zDebug("Preparing SQL - " . $zSQLs{I_CHARGE});
        $zCSRs{I_CHARGE} = $zdb->prepare($zSQLs{I_CHARGE}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
        
        $opt_debug && &zDebug("Preparing SQL - " . $zSQLs{I_CUST_QUERY});
        $zCSRs{I_CUST_QUERY} = $zdb->prepare($zSQLs{I_CUST_QUERY}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
        
        $opt_debug && &zDebug("Preparing SQL - " . $zSQLs{U_CUST_QUERY});
        $zCSRs{U_CUST_QUERY} = $zdb->prepare($zSQLs{U_CUST_QUERY}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
        
        $opt_debug && &zDebug("Preparing SQL - " . $zSQLs{RATEPLAN});
        $zCSRs{RATEPLAN} = $zdb->prepare($zSQLs{RATEPLAN}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
        
        $opt_debug && &zDebug("Preparing SQL - " . $zSQLs{CRM_REF});
        $zCSRs{CRM_REF} = $zdb->prepare($zSQLs{CRM_REF}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
    
    
    } elsif ($zType eq $TYPE_INVOICE) {
    
        1;
            
    } elsif ($zType eq $TYPE_CONFIGURATION) {
    
        1;   
    
        
    }elsif ($zType eq $TYPE_PRODUCT) {
    
        $opt_debug && &zDebug("Preparing SQL - " . $zSQLs{PIH_NEW});
        $zCSRs{PIH_NEW} = $zdb->prepare($zSQLs{PIH_NEW}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);

        $opt_debug && &zDebug("Preparing SQL - " . $zSQLs{I_PIH});
        $zCSRs{I_PIH} = $zdb->prepare($zSQLs{I_PIH}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr); 

        $opt_debug && &zDebug("Preparing SQL - " . $zSQLs{D_PIH});
        $zCSRs{D_PIH} = $zdb->prepare($zSQLs{D_PIH}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
        
        $opt_debug && &zDebug("Preparing SQL - " . $zSQLs{DSC_NEW});
        $zCSRs{DSC_NEW} = $zdb->prepare($zSQLs{DSC_NEW}) || &zdie(6000, __LINE__, __FILE__, $zdb->errstr);
    
    }
    
    
    return 0;
}


#***************************************************************************
# NAME:
#   zProcessEvent
#
# DESCRIPTION:
#   Performs the event data processing. 
#   - Find event files not processed yet. 
#   - Determine any events that have been updated since last execution of this process. 
#   
#
# PARAMETERS:
#   
#
# RETURNS:
#   0 or dies
#***************************************************************************
sub zProcessEvent
{
    my (@FileRow, @data, @mappedData, %recCharge, $lastDate, $i, $event_found, $fileCnt, %processedEvents, $changes, $startTime);
    
    $startTime = [gettimeofday];
    # Fetch event files not processed yet.
    $lastDate = ( defined($zLastExecDate) ) ? $zLastExecDate : $zEffDate;
    $opt_v && &zDebug("Fetching files to process that completed after - $lastDate");
    $zCSRs{NE_FILES}->execute($lastDate) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
    
    my $arrayRef = $zCSRs{NE_FILES}->fetchall_arrayref();
    
    ### Loop through each top level product to build the orders for all the products. 
    foreach my $FileRowRef (@{$arrayRef}) {
        @FileRow = @{$FileRowRef};
        
        $opt_v && &zDebug("Processing file - $FileRow[1] - process end date - $FileRow[5], Counter " . $fileCnt++);
        # Insert the file into
        my $fileId = $FileRow[0];
        my $neFileTypeId = $FileRow[3];
        my $minStartDate = $FileRow[14];
        my $maxStartDate = $FileRow[15];
        my $ratedCount = $FileRow[9];
        my $errorCount = ($FileRow[10] + $FileRow[11]);
        undef(%processedEvents);
        
        $i = 1;
        $event_found = 0;
        # Fetch each charge record and insert it into the data into the appropriate tables. 
        $opt_debug && &zDebug("Fetching events with - $fileId,$minStartDate,$maxStartDate, $ratedCount, $errorCount");
        $zCSRs{EVENTS_NEW}->execute($fileId,$minStartDate,$maxStartDate) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
        while (my $EventRow = $zCSRs{EVENTS_NEW}->fetchrow_hashref("NAME_uc")) {
            # if we found an event to process, then we will add the file to our records. 
            if ( not $event_found) {
                $opt_v && &zDebug("Record found - Inserting file as processed. ");
                $opt_testing || $zCSRs{I_FILES}->execute(@FileRow) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
                $event_found = 1;
                $zStats{INSERT_CNT}++;
            }
            
            
            # Each record needs to be split into an insert into the charge and NE record with needed mapping performed. 
            # Create NE record. 
            $opt_v && &zDebug("Processing file event $i");
            $opt_debug && &zDebug("Event $i extracted fields = \n" . Dumper($EventRow));
            $i++;
            
            my $crmRef;
            my $skipChgIns = 0;
            
            # Skip insert of event if already processed. 
            if ( not $processedEvents{$EventRow->{NORMALISED_EVENT_ID}} ) {
                $processedEvents{$EventRow->{NORMALISED_EVENT_ID}} = 1;
                undef @data;
                push @data, $EventRow->{NORMALISED_EVENT_ID};
                push @data, $EventRow->{LAST_MODIFIED};
                push @data, $EventRow->{NORMALISED_EVENT_TYPE_ID};
                push @data, $EventRow->{NORMALISED_EVENT_FILE_ID};
                push @data, $EventRow->{FILE_RECORD_NR};
                push @data, $EventRow->{A_PARTY_ID};
                push @data, substr ($EventRow->{A_PARTY_ID},6); #A_PARTY_ROUTE
                push @data, $EventRow->{B_PARTY_ID};
                push @data, $EventRow->{C_PARTY_ID};
                push @data, $EventRow->{CHARGE_START_DATE};
                push @data, $EventRow->{PERIOD_START_DATE};
                push @data, $EventRow->{PERIOD_END_DATE};
                push @data, $EventRow->{EVENT_CLASS_CODE};
                push @data, $EventRow->{BILL_RUN_ID};
                                          
                
                if ($EventRow->{EVENT_CLASS_CODE} == $EC_RECURRING) {
                    
                    push @data, 100;                            # EVENT_TYPE_CODE -> RECURRING
                    
                    if ( $EventRow->{DURATION} > 0) {
                        push @data, 110;                        # EVENT_SUB_TYPE_CODE -> RECURRING
                    }else{
                        push @data, 120;                        # EVENT_SUB_TYPE_CODE -> RECURRING ADJUSTMENT
                    }
                    
                    push @data, $EventRow->{DURATION};
                    push @data, $EventRow->{CHARGE};
                    push @data, $EventRow->{RATE_BAND};
                    push @data, $EventRow->{GENERAL_1}; 
                    push @data, $EventRow->{GENERAL_2};
                    push @data, $EventRow->{PRODUCT_GROUP}; # GENERAL_3 product_group
                    push @data, $EventRow->{GENERAL_4};
                    push @data, $EventRow->{GENERAL_5};
                    push @data, $EventRow->{GENERAL_6};
                    push @data, $EventRow->{GENERAL_7};
                    push @data, $EventRow->{GENERAL_8};
                    push @data, $EventRow->{GENERAL_9};
                    push @data, $EventRow->{GENERAL_10};
                    push @data, $EventRow->{GENERAL_11};
                    push @data, $EventRow->{GENERAL_3};  # GENERAL_12 -> frequency based on product ID
                    push @data, $EventRow->{GENERAL_3};  # GENERAL_13 -> is_advance based on product ID
                    push @data, $EventRow->{GENERAL_14};
                    push @data, $EventRow->{GENERAL_15}; # GENERAL_15 -> discount_type
                    push @data, $EventRow->{GENERAL_16}; # GENERAL_16 -> discount
                    push @data, $EventRow->{GENERAL_17};
                    push @data, $EventRow->{GENERAL_18};
                    push @data, $EventRow->{GENERAL_19};
                    push @data, $EventRow->{GENERAL_20};
                    
                    ###
                    #  CRM_REF TO MAP RCD TO RC PRODUCT AND IT'S EVENT
                    $zCSRs{CRM_REF}->execute($EventRow->{GENERAL_2}) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
                    while (my $crmRow = $zCSRs{CRM_REF}->fetchrow_hashref("NAME_uc")) {
                         $crmRef = $crmRow->{CRM_REF};
                         $recCharge{$EventRow->{SERVICE_ID}}{$EventRow->{NORMALISED_EVENT_ID}}{$crmRow->{CRM_REF}}{NE}=\@data; 
                    }
                    $skipChgIns = 1;
                                                            
                    
                } elsif ($EventRow->{EVENT_CLASS_CODE} == $EC_USAGE) {
                    
                    push @data, $EventRow->{EVENT_TYPE_CODE};
                    push @data, $EventRow->{EVENT_SUB_TYPE_CODE};
                    push @data, $EventRow->{DURATION};
                    push @data, $EventRow->{CHARGE};
                    push @data, $EventRow->{RATE_BAND};
                    # usage and one-off charges mapping
                    push @data, $EventRow->{C_PARTY_LOCATION_CODE};         # GENERAL_1 -> C_PARTY_LOCATION_CODE 
                    push @data, $EventRow->{GENERAL_19};                    # GENERAL_2 -> GENERAL_19
                    
                    ###
                    #  RATEPLAN ID and RATEPLAN 
                    my @splitStr = split /:/,$EventRow->{C_GENERAL_4};
                    push @data, $splitStr[2];                               # GENERAL_3 -> CH.GENERAL_4
                    $zCSRs{RATEPLAN}->execute($splitStr[2]) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
                    while (my $RpRow = $zCSRs{RATEPLAN}->fetchrow_hashref("NAME_uc")) {
                        push @data, $RpRow->{PRODUCT_DISPLAY_NAME};         # GENERAL_4 -> RATEPLAN
                        
                    }
                    
                    push @data, $EventRow->{GENERAL_20};                    # GENERAL_5 -> GENERAL_20
                    push @data, $EventRow->{GENERAL_6};
                    push @data, $EventRow->{GENERAL_7};
                    push @data, $EventRow->{GENERAL_9};
                    push @data, $EventRow->{GENERAL_10};
                    push @data, $EventRow->{GENERAL_11};
                    push @data, $EventRow->{GENERAL_12};
                    push @data, $EventRow->{GENERAL_13};
                    push @data, $EventRow->{GENERAL_14};
                    push @data, $EventRow->{GENERAL_15};
                    push @data, $EventRow->{GENERAL_16};
                    push @data, $EventRow->{GENERAL_17};
                    push @data, $EventRow->{GENERAL_18};
                    push @data, $EventRow->{GENERAL_19};
                    push @data, $EventRow->{GENERAL_20};
                } else { # One-Off
                    push @data, $EventRow->{EVENT_TYPE_CODE};
                    push @data, $EventRow->{EVENT_SUB_TYPE_CODE};
                    push @data, $EventRow->{DURATION};
                    push @data, $EventRow->{CHARGE};
                    push @data, $EventRow->{RATE_BAND};
                    push @data, $EventRow->{GENERAL_1}; 
                    push @data, $EventRow->{GENERAL_2};
                    push @data, $EventRow->{GENERAL_3}; 
                    push @data, $EventRow->{GENERAL_4};
                    push @data, $EventRow->{GENERAL_5};
                    push @data, $EventRow->{GENERAL_6};
                    push @data, $EventRow->{GENERAL_7};
                    push @data, $EventRow->{GENERAL_8};
                    push @data, $EventRow->{GENERAL_9};
                    push @data, $EventRow->{GENERAL_10};
                    push @data, $EventRow->{GENERAL_11};
                    push @data, $EventRow->{GENERAL_12}; 
                    push @data, $EventRow->{GENERAL_13}; 
                    push @data, $EventRow->{GENERAL_14};
                    push @data, $EventRow->{GENERAL_15}; 
                    push @data, $EventRow->{GENERAL_16}; 
                    push @data, $EventRow->{GENERAL_17};
                    push @data, $EventRow->{GENERAL_18};
                    push @data, $EventRow->{GENERAL_19};
                    push @data, $EventRow->{GENERAL_20};
                }
                $zTabColumns{REFERENCE_TYPE} = $zTabColumns{H3ATT_DWH_NORMALISED_EVENT};
                @mappedData = &zMapData('REFERENCE_TYPE',\@data);
                
                if ($EventRow->{EVENT_CLASS_CODE} != $EC_RECURRING) {
                    # Insert the row into the NE table. 
                    $opt_v && zDebug("inserting entry into table - H3ATT_DWH_NORMALISED_EVENT");
                    $opt_testing || $zCSRs{I_NE}->execute(@mappedData) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
                }   $zStats{INSERT_CNT}++;
            }
            
            # Prepare the date for the charge record. 
            undef @data;
            push @data, $EventRow->{CHARGE_ID};
            push @data, $EventRow->{C_LAST_MODIFIED};
            push @data, $EventRow->{CHARGE};
            push @data, $EventRow->{ACCOUNT_ID};
            push @data, $EventRow->{UNINVOICED_IND_CODE};
            push @data, $EventRow->{NORMALISED_EVENT_ID};
            push @data, $EventRow->{SERVICE_ID};
            push @data, $EventRow->{CUSTOMER_NODE_ID};
            push @data, $EventRow->{CHARGE_DATE}; 
            push @data, $EventRow->{TARIFF_ID};
            push @data, $EventRow->{INVOICE_TXT};
            push @data, $EventRow->{INVOICE_ID};
            push @data, $EventRow->{C_GENERAL_1};
            push @data, $EventRow->{C_GENERAL_2};
            push @data, $EventRow->{C_GENERAL_3};
            push @data, $EventRow->{C_GENERAL_4};
            push @data, $EventRow->{C_GENERAL_5};
            push @data, $EventRow->{C_GENERAL_6};
            push @data, $EventRow->{C_GENERAL_7};
            push @data, $EventRow->{C_GENERAL_8};
            push @data, $EventRow->{C_GENERAL_1};       #GENERAL_9 -> CO:FI
            push @data, $EventRow->{C_GENERAL_10};
            @mappedData = &zMapData('H3ATT_DWH_CHARGE',\@data);
            
            if ($skipChgIns == 0) {
                # Insert the row into the charge table. 
                $opt_v && zDebug("inserting entry into table - H3ATT_DWH_CHARGE");
                $opt_testing || $zCSRs{I_CHARGE}->execute(@mappedData) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
                $zStats{INSERT_CNT}++;
            } else {
            
                 $recCharge{$EventRow->{SERVICE_ID}}{$EventRow->{NORMALISED_EVENT_ID}}{$crmRef}{CHG} = \@mappedData;
            }
            
            # If this is a one-off charge, we need to create a customer query record.
            if ($neFileTypeId == $NEF_ONEOFF) {
                # If this is a discount, then we need to update the main record. 
                if (substr($EventRow->{GENERAL_8},-4) eq '_DSC') {
                    # This is the discount charge for the NRC. Just need to update the query to add the needed details. 
                    undef @data;
                    push @data, $EventRow->{GENERAL_19};          # GENERAL_8 - Discount Text
                    push @data, $EventRow->{NORMALISED_EVENT_ID}; # CUSTOMER_QUERY_ID
                    
                    
                    # Insert the row into the charge table. 
                    $opt_v && zDebug("Updating entry into table - H3ATT_DWH_CUSTOMER_QUERY with - " . Dumper(@data));
                    $opt_testing || $zCSRs{U_CUST_QUERY}->execute(@data) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
                    
                } else {
                    # Charge record, insert
                    undef @data;
                    push @data, $EventRow->{NORMALISED_EVENT_ID};   # CUSTOMER_QUERY_ID
                    push @data, $EventRow->{LAST_MODIFIED};
                    push @data, $EventRow->{GENERAL_8};             # CUSTOMER_QUERY_NR - CRM_REF
                    push @data, 25000060;                           # CUSTOMER_QUERY_TYPE_ID - NRC - 25000060
                    push @data, $EventRow->{CUSTOMER_NODE_ID};
                    push @data, $EventRow->{SERVICE_ID};
                    push @data, 2;                                  # QUERY_STATUS_CODE - 2 = CLOSED
                    push @data, $EventRow->{LAST_MODIFIED};         # OPEN_DATE
                    push @data, $EventRow->{LAST_MODIFIED};         # CLOSE_DATE
                    push @data, $EventRow->{RESOLUTION_TEXT};       # RESOLUTION_TEXT
                    push @data, $EventRow->{GENERAL_8};             # GENERAL_1 - CRM_REF
                    push @data, $EventRow->{GENERAL_19};            # GENERAL_2 - Invoice text
                    push @data, $EventRow->{CHARGE};                # GENERAL_3 - Charge amount
                    push @data, $EventRow->{CHARGE_START_DATE};     # GENERAL_4 - Charge date requested
                    push @data, $EventRow->{GENERAL_12};            # GENERAL_5 - CO:FI code
                    push @data, '';                                 # GENERAL_6 - Credit Note text
                    push @data, $EventRow->{GENERAL_11};            # GENERAL_9 - Charge Class
                    @mappedData = &zMapData('H3ATT_DWH_CUSTOMER_QUERY',\@data);
                
                    # Insert the row into the charge table. 
                    $opt_v && zDebug("inserting entry into table - H3ATT_DWH_CUSTOMER_QUERY");
                    $opt_testing || $zCSRs{I_CUST_QUERY}->execute(@mappedData) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
                    $zStats{INSERT_CNT}++;
                }
            }
        }# END OF WHILE
        
        if ($event_found) {
            $opt_testing || $zdb->commit()|| &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
        }
        
    }#For loop event file
    
    #Process Recurring Charge/Recurring Discount     
    foreach my $serId (keys %recCharge) {
        foreach my $neId (keys %{$recCharge{$serId}}) {
            foreach my $crmRef (keys %{$recCharge{$serId}{$neId}}) {
                print Dumper($recCharge{$serId}{$neId}{$crmRef});
            }
        }
    }
    
    return 0;

}


#***************************************************************************
# NAME:
#   zProcessConf
#
# DESCRIPTION:
#   Performs the Customer data processing. 
#   -    
#
# PARAMETERS:
#   
#
# RETURNS:
#   0 or dies
#***************************************************************************
sub zProcessConf
{

    1;
}


#***************************************************************************
# NAME:
#   zProcessInv
#
# DESCRIPTION:
#   Performs the Customer data processing. 
#   -    
#
# PARAMETERS:
#   
#
# RETURNS:
#   0 or dies
#***************************************************************************
sub zProcessInv
{

    1;
}


#***************************************************************************
# NAME:
#   zProcessProduct
#
# DESCRIPTION:
#   Performs the product data processing. 
#   -    
#
# PARAMETERS:
#   
#
# RETURNS:
#   0 or dies
#***************************************************************************
sub zProcessProduct
{
    my (@data, @mappedData, $lastDate, $disData);
    
    $lastDate = ( defined($zLastExecDate) ) ? $zLastExecDate : $zEffDate;

    print "Effective Date: $lastDate\n";
    
    # Handling Discount Product Instance 
    $opt_v && &zDebug("Fetching Discount Records - $lastDate");
    $zCSRs{DSC_NEW}->execute($lastDate) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
    
    while (my $DscRow = $zCSRs{DSC_NEW}->fetchrow_hashref("NAME_uc")) {
        my $l_dscAmt = $DscRow->{DISCOUNT_AMOUNT};
        $l_dscAmt =~ tr/./,/;        
        $disData->{$DscRow->{GENERAL_10}}{DISCOUNT_AMOUNT}          = $l_dscAmt;
        $disData->{$DscRow->{GENERAL_10}}{DISCOUNT_PERCENTAGE}      = $DscRow->{DISCOUNT_PERCENTAGE};
        $disData->{$DscRow->{GENERAL_10}}{DISCOUNT_INVOICE_TEXT}    = $DscRow->{DISCOUNT_INVOICE_TEXT};
                
        if (defined $DscRow->{DISCOUNT_AMOUNT}) {
            $disData->{$DscRow->{GENERAL_10}}{DISCOUNT_TYPE} = 1; #Discount type amount from ARGOS reference data TT_PRD_DISCOUNT_TYPE
            $disData->{$DscRow->{GENERAL_10}}{GENERAL_10} = $disData->{$DscRow->{GENERAL_10}}{DISCOUNT_TYPE}.':'.$l_dscAmt.':'.$DscRow->{DISCOUNT_INVOICE_TEXT};
        } else {
            $disData->{$DscRow->{GENERAL_10}}{DISCOUNT_TYPE} = 2; #Discount type amount from ARGOS reference data TT_PRD_DISCOUNT_TYPE
            $disData->{$DscRow->{GENERAL_10}}{GENERAL_10} = $disData->{$DscRow->{GENERAL_10}}{DISCOUNT_TYPE}.':'.$DscRow->{DISCOUNT_PERCENTAGE}.':'.$DscRow->{DISCOUNT_INVOICE_TEXT};
           
        }
        
    }
    
       
    # Handling PRODUCT_INSTANCE_HISTORY 
    $opt_v && &zDebug("Fetching entries for cleanup - $lastDate");
    $zCSRs{PIH_NEW}->execute($lastDate) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
    
    while (my $PihRow = $zCSRs{PIH_NEW}->fetchrow_hashref("NAME_uc")) {
        
        
        #Preparing data for insert
        undef @data;
        
        $opt_v && zDebug("clean up outdated entry from table - H3ATT_DWH_PROD_INST_HIST");
        $opt_testing || $zCSRs{D_PIH}->execute($PihRow->{PRODUCT_INSTANCE_ID},$PihRow->{EFFECTIVE_START_DATE}) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
        
        if (not $opt_testing) {
            $zStats{UPDATE_CNT} = $zStats{UPDATE_CNT} + $zCSRs{D_PIH}->rows;
        }
        
        
        push @data, $PihRow->{PRODUCT_INSTANCE_ID};         
        push @data, $PihRow->{LAST_MODIFIED};          
        push @data, $PihRow->{ATLANTA_OPERATOR_ID};         
        push @data, $PihRow->{EFFECTIVE_START_DATE};        
        push @data, $PihRow->{EFFECTIVE_END_DATE};       
        push @data, $PihRow->{PRODUCT_ID};                  
        push @data, $PihRow->{PRODUCT_INSTANCE_STATUS_CODE};
        push @data, $PihRow->{CUSTOMER_NODE_ID};            
        push @data, $PihRow->{BASE_PRODUCT_INSTANCE_ID};    
        push @data, $PihRow->{CONTRACT_ID};                 
        push @data, $PihRow->{FROM_PRODUCT_INSTANCE_ID};    
        push @data, $PihRow->{FROM_PRODUCT_ID};             
        push @data, $PihRow->{TO_PRODUCT_INSTANCE_ID};      
        push @data, $PihRow->{TO_PRODUCT_ID};               
        push @data, $PihRow->{UNCOMPLETED_IND_CODE};
        
        ##########################
        #DWH GENERAL FILED MAPPING
        ##########################
        
        #Handling BASE PRODUCTS
        if ($PihRow->{BASE_PRODUCT_INSTANCE_ID} == ''){                 
                        
            push @data, substr($PihRow->{INDUSTRY_GENERAL_1},3);    #GENERAL_1 -> CRM_REF Parent
            push @data, 'Needs to be verified by DWH team';         #GENERAL_2 -> Yet to implement
            push @data, $PihRow->{INDUSTRY_GENERAL_7};              #GENERAL_3 -> Product class
            push @data, $PihRow->{INDUSTRY_GENERAL_2};              #GENERAL_4 -> Product Group
            push @data, $PihRow->{INDUSTRY_GENERAL_3};              #GENERAL_5 -> Product Ref Code
            push @data, $PihRow->{INDUSTRY_GENERAL_4};              #GENERAL_6 -> Product Name
            push @data, $PihRow->{INDUSTRY_GENERAL_5};              #GENERAL_7 -> Service Info
            push @data, $PihRow->{INDUSTRY_GENERAL_6};              #GENERAL_8 -> Notice On Invoice
            push @data, $PihRow->{INDUSTRY_GENERAL_8};              #GENERAL_9 -> Technical site
            push @data, $PihRow->{INDUSTRY_GENERAL_9};              #GENERAL_10-> Service_name
            
            
            
        } else { #Handling Companion Products
                        
            push @data, $PihRow->{GENERAL_10};                                              #GENERAL_1 -> CRM_REF                   
            push @data, $PihRow->{PRODUCT_ID};                                              #GENERAL_2 -> For Mapping Data From dH3ATT_DWH_Mapping (Frequency)
            push @data, $PihRow->{PRODUCT_ID};                                              #GENERAL_3 -> For Mapping Data From dH3ATT_DWH_Mapping (Advance/Arrears) Reference Type H3ATT_REC_ADVANCE_PERIOD
            push @data, 'Todo can not be implemented for now';                              #GENERAL_4 -> Yet to implement
            push @data, '10';                                                               #GENERAL_5 -> Hard coded to 10 -> Charge
            my $l_rate = $PihRow->{GENERAL_5}; 
            $l_rate =~ tr/./,/;
            push @data, $l_rate;                                                            #GENERAL_6 -> Rate
            push @data, $PihRow->{GENERAL_9};                                               #GENERAL_7 -> Invoice Text
            push @data, $PihRow->{INDUSTRY_GENERAL_2};                                      #GENERAL_8 -> Charge Class
            push @data, $PihRow->{INDUSTRY_GENERAL_1}.":".$PihRow->{INDUSTRY_GENERAL_3};    #GENERAL_9 -> CO:FI:EXTERNAL CHARGE_ID
            
            #GENERAL_10 -> Discount Information (Discount Type:Discount Amount:Discount Text)
            if (defined $disData->{$PihRow->{GENERAL_10}}) {    
                push @data, $disData->{$PihRow->{GENERAL_10}}{GENERAL_10};         
            } else {
                push @data, '';
            }
    
        }
        
                
        push @data, $PihRow->{ACTIVE_DATE};                 
        push @data, $PihRow->{PRODUCT_BUNDLE_ID };          
        push @data, $PihRow->{INDUSTRY_GENERAL_1};          
        push @data, $PihRow->{INDUSTRY_GENERAL_2};          
        push @data, $PihRow->{INDUSTRY_GENERAL_3};          
        push @data, $PihRow->{INDUSTRY_GENERAL_4};          
        push @data, $PihRow->{INDUSTRY_GENERAL_5};          
        push @data, $PihRow->{INDUSTRY_GENERAL_6};          
        push @data, $PihRow->{INDUSTRY_GENERAL_7};          
        push @data, $PihRow->{INDUSTRY_GENERAL_8};          
        push @data, $PihRow->{INDUSTRY_GENERAL_9};          
        push @data, $PihRow->{INDUSTRY_GENERAL_10};          
  
        
        #Map data 
        @mappedData = &zMapData('H3ATT_DWH_PROD_INST_HIST',\@data); 
        
        $opt_v && zDebug("inserting entry into table - H3ATT_DWH_PROD_INST_HIST");
        $opt_testing || $zCSRs{I_PIH}->execute(@mappedData) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);

        $zStats{INSERT_CNT}++;
                  
    }
    
    $opt_testing || $zdb->commit()|| &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
    
    return 0;
}

#***************************************************************************
# NAME:
#   zMapData
#
# DESCRIPTION:
#   determines if the passed in value should be changed based on the mapping table
#   
#
# PARAMETERS:
#   $l_table - The Table name for the output of the data
#   $l_column - The column name of the data to be mapped. 
#   $l_data - The input data of the field
#
# RETURNS:
#   0 or dies
#***************************************************************************
sub zMapData
{
    my ($l_table, $l_dataRef) = @_;
    
    my @data = @{$l_dataRef};
    my (@result, $column, $value);
    
    # initialize result to input data in-case no mapping is found to be needed. 
    my @columns = @{$zTabColumns{$l_table}};
    for my $i (0..$#columns) {
        $column = $columns[$i];
        $value = $data[$i];
        if ( defined($value) && defined($zMapCache{$l_table}{$column}{$value}) ) {
            push @result, $zMapCache{$l_table}{$column}{$value};
            $opt_debug && zDebug("Mapped value for column ($column) $value -> " . $zMapCache{$l_table}{$column}{$value});
        } else {
            push @result, $value;
        }
        
    }
    
    return @result;
    
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
#   Describe what the subroutine returns.
#***************************************************************************
sub zDebug
{
    my $msg = shift;
    my $now = localtime;
    if ($$ == $zParentPid) {
        print("$now - $msg\n");
    } else {
        print("*CHILD*$$* $now - $msg\n");
    }
    
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
    
    $opt_v && print("Loaded Referendce Type: $l_labelName \n");
    
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
    if ($zStats{INSERT_CNT} + $zStats{UPDATE_CNT} + $zStats{ERROR_CNT} > 0) {
    
        # Prepare stats: 
        print "\n\n\t******** TYPE - EVENT ********\n";
        printf("\tNumber of rows inserted:    %8s\n",$zStats{INSERT_CNT});
        printf("\tNumber of rows updated:     %8s\n",$zStats{UPDATE_CNT});
        printf("\tNumber of rows error:       %8s\n",$zStats{ERROR_CNT});
        
        
        print("\n\t*************************************\n\n");
    } else {
        print("\nNo changes made\n");
    }
    
    return 0;

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
    print STDERR <<EOS;
$MODULE_NAME task_id effective_start_date process_type [-testing] [-debug] [-v]
    task_id                 Task ID of task running this process
    effective_start_date    Effective start date of task running this process
    process_type            Process type code from RT - H3ATT_DWH_PROCESS_TYPE
                            10 - Event
                            30 - Configuration
                            40 - Invoice
                            50 - Product
    -d                      Debug level.  Currently only supports
    -testing                testing mode with no changes committed. 
    -site                   Support to limit loading to a specific bill_site_id 
EOS
    exit(2);
} #end zusage


#---------------------------------------------------------------------------
#       MAIN CODE
#---------------------------------------------------------------------------

### Parse Command Line Parameters
#
if ((!defined($ARGV[0])) || ($ARGV[0] eq "-?") || ($ARGV[0] eq "-h") ||
    !&GetOptions("debug", "v", "u=s", "d", "child=i", "testing", "site=s")) {
    &zusage;
}

### Log a start message
#
$znow = localtime;
&ataiLogMsg($STAT_PROC_START, $MODULE_NAME, $znow);

### Initialise the script
&zInit();


### Populate the progress table for the task.


## Fetch the data for the type of request
if ($zType eq $TYPE_EVENT) {
    
    &zProcessEvent();

} elsif ($zType eq $TYPE_INVOICE) {

    &zProcessInv();

} elsif ($zType eq $TYPE_CONFIGURATION) {

    &zProcessConf();

}elsif ($zType eq $TYPE_PRODUCT) {

    &zProcessProduct();

}


### Finalize process
# 1. update the process table. 
$opt_v && zDebug("Updating entry in table - H3ATT_DWH_PROCESS");
if (not $opt_testing) {
    $zCSRs{U_PROCESS}->execute('processed', $zStats{INSERT_CNT}, $zStats{UPDATE_CNT}, $zStats{ERROR_CNT}, $zTaskId) || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
    $zdb->commit() || &zdie(6001, __LINE__, __FILE__, $zdb->errstr);
}

# Print the statistics to the task output. 
&zPrintStats();


### Log an end message
#
$znow = localtime;
&ataiLogMsg($STAT_PROC_END, $MODULE_NAME, $znow);

exit 0;

#
#       End (of H3AT_DWH_Process.pl)
#
