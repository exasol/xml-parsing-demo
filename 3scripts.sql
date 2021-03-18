OPEN SCHEMA xml_test;

--/
CREATE OR REPLACE SCRIPT xml_load(schema, ftp_host, ftp_dir, ftp_user, ftp_password) RETURNS ROWCOUNT AS
-- Clean up staging
query([[TRUNCATE TABLE ::s.XML_STAGING]], {s = schema});
query([[TRUNCATE TABLE ::s.XML_SOURCE]], {s = schema});

-- Fill xml_source table with FTP files to load
xml_source_res = query([[INSERT INTO ::s.xml_source (file_name, last_changed, file_size, loaded)
                        SELECT ::s.ftp_metadata_load(:h, :d, :u, :p), null]],
                        {s = schema, h = ftp_host, d = ftp_dir, u = ftp_user, p = ftp_password})
output(xml_source_res.rows_inserted..' FTP files added to download queue.')

-- Write into staging
xml_staging_res = query([[INSERT INTO ::s.xml_staging ( 
                             MANDANT,
                             FILENAME,
                             MD5,
                             RETAIL_STORE_ID,
                             WORKSTATION_ID,
                             BON_ID,
                             TX_LINENUM,
                             RECEIPT_DATE,
                             OPERATOR_ID,
                             SATZART,
                             TRANSACTION_TAX,
                             TRANSACTION_COUNT,
                             ITEM_COUNT,
                             RECEIPT_DATE_TIME,
                             OPERATOR_NAME,
                             CURRENCY_CODE,
                             VERSION,
                             GRAND_TOTAL,
                             LINEITEM_SATZART,
                             LINEITEM_LINENUM,
                             ITEM_TYPE,
                             ITEM_ID,
                             ITEM_DESCRIPTION,
                             REGULAR_SALES_UNIT_PRICE,
                             DISCOUNT_AMOUNT,
                             QUANTITY,
                             UNIT_LIST_PRICE,
                             ENTRY_METHOD,
                             TENDER_TYPE_CODE,
                             TENDER_TYPE,
                             TENDER_AMOUNT)
                        WITH temp AS (  SELECT    ::s.PARSE_MY_XML(s.file_name, :d, :h, :u, :p)
                                        FROM      ::s.XML_SOURCE s)
                        SELECT * FROM temp;
        ]], {s = schema, h = ftp_host, d = ftp_dir, u = ftp_user, p = ftp_password});
output(xml_staging_res.rows_inserted..' rows inserted into XML_STAGING')    

----
--Mark loaded files as loaded
----
xml_source_res = query([[UPDATE ::s.xml_source
                        SET loaded = current_timestamp
                        WHERE true]],
                        {s = schema});
                        
----
--Write into target tables
----

--Header
xml_header_res = query([[INSERT INTO ::s.TRANSACTION_HEADER (header_id,
                                                                MANDANT,           
                                                                FILENAME,          
                                                                MD5,               
                                                                STORE_ID,          
                                                                WORKSTATION_ID,    
                                                                RECEIPT_ID,        
                                                                XML_LINENUM,
                                                                RECEIPT_DATE,                
                                                                OPERATOR_ID,       
                                                                SATZART,           
                                                                TRANSACTION_TAX,   
                                                                TRANSACTION_COUNT, 
                                                                ITEM_COUNT,        
                                                                RECEIPT_DATE_TIME,   
                                                                OPERATOR_NAME,     
                                                                CURRENCY_CODE,     
                                                                VERSION,           
                                                                GRAND_TOTAL)
        SELECT  header_id,
                MANDANT,           
                FILENAME,          
                MD5,               
                RETAIL_STORE_ID,          
                WORKSTATION_ID,    
                BON_ID,        
                TX_LINENUM, 
                RECEIPT_DATE,             
                OPERATOR_ID,       
                SATZART,           
                TRANSACTION_TAX,   
                TRANSACTION_COUNT, 
                ITEM_COUNT,        
                to_timestamp(RECEIPT_DATE_TIME, 'YYYY-MM-DDTHH24:MI:SS'),
                OPERATOR_NAME,     
                CURRENCY_CODE,     
                VERSION,           
                GRAND_TOTAL
        FROM ::s.xml_staging
        where header_id in (
                select distinct first_value(header_id) over (partition by bon_id) header_id
                                from ::s.xml_staging)
                ]], {s = schema})
output(xml_header_res.rows_inserted..' rows inserted into TRANSACTION_HEADER')


--Position
xml_positions_res = query([[INSERT INTO ::s.TRANSACTION_POSITIONS
        SELECT  HEADER_ID,                 
                MANDANT,                   
                FILENAME,                  
                MD5,                       
                RETAIL_STORE_ID,                  
                WORKSTATION_ID,            
                BON_ID,                
                TX_LINENUM,               
                RECEIPT_DATE,              
                OPERATOR_ID,               
                SATZART,                   
                TRANSACTION_TAX,           
                TRANSACTION_COUNT,         
                ITEM_COUNT,                
                to_timestamp(RECEIPT_DATE_TIME, 'YYYY-MM-DDTHH24:MI:SS'),
                OPERATOR_NAME,             
                CURRENCY_CODE,             
                VERSION,                   
                GRAND_TOTAL,               
                LINEITEM_SATZART,             
                LINEITEM_SATZART,          
                LINEITEM_LINENUM,          
                ITEM_TYPE,                 
                ITEM_ID,                   
                ITEM_DESCRIPTION,          
                REGULAR_SALES_UNIT_PRICE,  
                DISCOUNT_AMOUNT,           
                QUANTITY,                  
                UNIT_LIST_PRICE,           
                ENTRY_METHOD  
        FROM ::s.xml_staging
        WHERE ITEM_ID is not null
        ]], {s = schema})
output(xml_positions_res.rows_inserted..' rows inserted into TRANSACTION_POSITION')

--Zahlungsart
xm_tender_res = query([[INSERT INTO ::s.TRANSACTION_TENDER
       SELECT   HEADER_ID,         
                MANDANT,           
                FILENAME,          
                MD5,               
                RETAIL_STORE_ID,          
                WORKSTATION_ID,    
                BON_ID,        
                TX_LINENUM,       
                RECEIPT_DATE,      
                OPERATOR_ID,       
                SATZART,           
                TRANSACTION_TAX,   
                TRANSACTION_COUNT, 
                ITEM_COUNT,        
                to_timestamp(RECEIPT_DATE_TIME, 'YYYY-MM-DDTHH24:MI:SS'),
                OPERATOR_NAME,     
                CURRENCY_CODE,     
                VERSION,           
                GRAND_TOTAL,       
                TENDER_TYPE_CODE,  
                TENDER_TYPE,       
                TENDER_AMOUNT   
        FROM ::s.xml_staging
        WHERE TENDER_TYPE_CODE is not null
        ]], {s = schema})
output(xm_tender_res.rows_inserted..' rows inserted into TRANSACTION_TENDER')
/
------------------------

EXECUTE SCRIPT xml_load(
'XML_TEST',                     -- schema
'52.17.71.231',                 --ftp host
'lennart',                      --ftp directory
'lennart',                      --ftp user
'pRC*8Y3WwkhoZJMFBZ78r6BM'      --ftp password
) WITH OUTPUT;