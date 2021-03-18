CREATE SCHEMA IF NOT EXISTS xml_test;
OPEN SCHEMA xml_test;

--------------------
--Create Meta Table
CREATE OR REPLACE TABLE xml_source(id INT IDENTITY, 
                                file_name VARCHAR(500), 
                                file_size INT, 
                                last_changed TIMESTAMP,
                                loaded TIMESTAMP);            

--------------------
--Create xml_staging
CREATE OR REPLACE TABLE xml_staging(
HEADER_ID INT IDENTITY,
MANDANT VARCHAR(50000),
FILENAME VARCHAR(50000),
MD5 VARCHAR(50000),
RETAIL_STORE_ID VARCHAR(50000),
WORKSTATION_ID VARCHAR(50000),
BON_ID VARCHAR(50000),
TX_LINENUM VARCHAR(50000),
RECEIPT_DATE VARCHAR(50000),
OPERATOR_ID VARCHAR(50000),
SATZART VARCHAR(50000),
TRANSACTION_TAX VARCHAR(50000),
TRANSACTION_COUNT VARCHAR(50000),
ITEM_COUNT VARCHAR(50000),
RECEIPT_DATE_TIME VARCHAR(50000),
OPERATOR_NAME VARCHAR(50000),
CURRENCY_CODE VARCHAR(50000),
VERSION VARCHAR(50000),
GRAND_TOTAL VARCHAR(50000),
LINEITEM_SATZART VARCHAR(50000),
LINEITEM_LINENUM VARCHAR(50000),
ITEM_TYPE VARCHAR(50000),
ITEM_ID VARCHAR(50000),
ITEM_DESCRIPTION VARCHAR(50000),
REGULAR_SALES_UNIT_PRICE VARCHAR(50000),
DISCOUNT_AMOUNT VARCHAR(50000),
QUANTITY VARCHAR(50000),
UNIT_LIST_PRICE VARCHAR(50000),
ENTRY_METHOD VARCHAR(50000),
TENDER_TYPE_CODE VARCHAR(50000),
TENDER_TYPE VARCHAR(50000),
TENDER_AMOUNT VARCHAR(50000)
);

-----------------------
-----------------------
--Create Prod Tables
        
CREATE OR REPLACE TABLE TRANSACTION_HEADER (
        HEADER_ID                        DECIMAL(19,0) IDENTITY,
        MANDANT                          DECIMAL(10,0),
        FILENAME                         VARCHAR(50),
        MD5                              VARCHAR(32),
        STORE_ID                         DECIMAL(10,0),
        WORKSTATION_ID                   DECIMAL(5,0),
        RECEIPT_ID                       DECIMAL(10,0),
        XML_LINENUM                      DECIMAL(10,0),
        RECEIPT_DATE                     DATE,
        OPERATOR_ID                      VARCHAR(5),
        SATZART                          VARCHAR(50),
        TRANSACTION_TAX                  DECIMAL(31,2),
        TRANSACTION_COUNT                DECIMAL(31,2),
        ITEM_COUNT                       DECIMAL(31,2),
        RECEIPT_DATE_TIME                TIMESTAMP,
        OPERATOR_NAME                    VARCHAR(500),
        CURRENCY_CODE                    VARCHAR(5),
        VERSION                          VARCHAR(50),
        GRAND_TOTAL                      DECIMAL(31,2));
        
CREATE OR REPLACE TABLE TRANSACTION_POSITIONS ( 
        HEADER_ID                        DECIMAL(19,0) IDENTITY,
        MANDANT                          DECIMAL(10,0),
        FILENAME                         VARCHAR(50),
        MD5                              VARCHAR(32),
        STORE_ID                         DECIMAL(10,0),
        WORKSTATION_ID                   DECIMAL(5,0),
        RECEIPT_ID                       DECIMAL(10,0),
        XML_LINENUM                      DECIMAL(10,0),
        RECEIPT_DATE                     DATE,
        OPERATOR_ID                      VARCHAR(5),
        SATZART                          VARCHAR(50),
        TRANSACTION_TAX                  DECIMAL(31,2),
        TRANSACTION_COUNT                DECIMAL(31,2),
        ITEM_COUNT                       DECIMAL(31,2),
        RECEIPT_DATE_TIME                TIMESTAMP,
        OPERATOR_NAME                    VARCHAR(500),
        CURRENCY_CODE                    VARCHAR(5),
        VERSION                          VARCHAR(50),
        GRAND_TOTAL                      DECIMAL(31,2),
        LINEITEM_KIND                    VARCHAR(20),
        LINEITEM_SATZART                 VARCHAR(200),
        LINEITEM_LINENUM                 INT,
        ITEM_TYPE                        VARCHAR(200),
        ITEM_ID                          VARCHAR(2000),
        ITEM_DESCRIPTION                 VARCHAR(2000),
        REGULAR_SALES_UNIT_PRICE         DOUBLE,
        DISCOUNT_AMOUNT                  DOUBLE,
        QUANTITY                         DOUBLE,
        UNIT_LIST_PRICE                  DOUBLE,
        ENTRY_METHOD                     VARCHAR(500));
        
CREATE OR REPLACE TABLE TRANSACTION_TENDER ( 
        HEADER_ID                        DECIMAL(19,0) IDENTITY,
        MANDANT                          DECIMAL(10,0),
        FILENAME                         VARCHAR(50),
        MD5                              VARCHAR(32),
        STORE_ID                         DECIMAL(10,0),
        WORKSTATION_ID                   DECIMAL(5,0),
        RECEIPT_ID                       DECIMAL(10,0),
        XML_LINENUM                      DECIMAL(10,0),
        RECEIPT_DATE                     DATE,
        OPERATOR_ID                      VARCHAR(5),
        SATZART                          VARCHAR(50),
        TRANSACTION_TAX                  DECIMAL(31,2),
        TRANSACTION_COUNT                DECIMAL(31,2),
        ITEM_COUNT                       DECIMAL(31,2),
        RECEIPT_DATE_TIME                TIMESTAMP,
        OPERATOR_NAME                    VARCHAR(500),
        CURRENCY_CODE                    VARCHAR(5),
        VERSION                          VARCHAR(50),
        GRAND_TOTAL                      DECIMAL(31,2),
        TENDER_TYPE_CODE                 VARCHAR(50000),
        TENDER_TYPE                      VARCHAR(50000),
        TENDER_AMOUNT                    DOUBLE);