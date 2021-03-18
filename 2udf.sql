OPEN SCHEMA xml_test;

--Create metadata load udf
--/
CREATE OR REPLACE python3 SCALAR SCRIPT ftp_metadata_load(ftp_host VARCHAR(5000), ftp_dir VARCHAR(500), ftp_user VARCHAR(500), ftp_password VARCHAR(500))
EMITS (file_name v(500), last_changed TIMESTAMP, file_size INT) AS
FROM ftplib import FTP
import datetime

def run(ctx):
        with FTP(ctx.ftp_host) as ftp:
            ftp.login(ctx.ftp_user, ctx.ftp_password)
            ftp.cwd(ctx.ftp_dir)
            for file in ftp.mlsd():
                ctx.emit(file[0], datetime.datetime.strptime(file[1].get('modify'), '%Y%m%d%H%M%S'), int(file[1].get('size')))
/

--If your FTP server does not support mlsd() you are left with using dir(). Neither file size nor file modification date are supported bei dir() we in this case we use dummy values.
--If your server does not support mlsd() the command above will throw an "ftplib.error_perm: 500 Unknown command." redeploy fpt_medatada_load() with the code below.
----/
--CREATE OR REPLACE python3 SCALAR SCRIPT ftp_metadata_load(ftp_host VARCHAR(5000), ftp_dir VARCHAR(500), ftp_user VARCHAR(500), ftp_password VARCHAR(500))
--EMITS (file_name VARCHAR(500), last_changed TIMESTAMP, file_size INT) AS
--from ftplib import FTP
--import datetime
--
--def run(ctx):
--        with FTP(ctx.ftp_host) as ftp:
--            ftp.login(ctx.ftp_user, ctx.ftp_password)
--            ftp.cwd(ctx.ftp_dir)
--            for file in ftp.nlst():
--                ctx.emit(file, datetime.datetime.strptime('20200101200000', '%Y%m%d%H%M%S'), -1)
--/   

--Create xml_udf
--/
CREATE OR REPLACE python3 SCALAR SCRIPT parse_my_xml(file_name varchar(20000), 
                                                  ftp_dir varchar(20000), 
                                                  ftp_host varchar(20000), 
                                                  ftp_user varchar(20000), 
                                                  ftp_password varchar(20000)) 
EMITS ( MANDANT VARCHAR(50000),
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
        TOTAL_GRAND VARCHAR(50000),
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
        ) AS
from lxml import etree as ET
from io import BytesIO
from ftplib import FTP
from datetime import datetime
from hashlib import md5

def run(ctx):
    with FTP(ctx.ftp_host) as ftp:
        raw_xml = BytesIO()

        ftp.login(ctx.ftp_user, ctx.ftp_password)
        ftp.cwd(ctx.ftp_dir)

        path = f'RETR {ctx.file_name}'
        ftp.retrbinary(path, raw_xml.write)
        raw_xml.seek(0)
        root = ET.parse(BytesIO(raw_xml.read())).getroot()

    namespaces = {"poslog":"http://www.nrf-arts.org/IXRetail/namespace/",
                    "x":"http://www.nrf-arts.org/IXRetail/namespace/",
                    "acs":"http://www.ncr.com/rsd/acs/tlog/markup/poslog/2.1",
                    "raw":"http://www.ncr.com/rsd/acs/tlog/markup/raw/base/6.1",
                    "xsi":"http://www.w3.org/2001/XMLSchema-instance",
                    "msxsl":"urn:schemas-microsoft-com:xslt",
                    "as":"urn:ACSScript",
                    "acssm":"urn:ACSSigMap"}

    def get_Satzart(element):
        tag = element.tag
        tag = tag.replace('{http://www.nrf-arts.org/IXRetail/namespace/}', '')
        tag = tag.replace('{http://www.wincor-nixdorf.com}', '')

        if tag == "Transaction":
            return "Transaction"
        elif tag == "LineItem" and element.get('EntryMethod'):
            return "Sale"
        elif tag == "RetailPriceModifier":
            return "RetailPriceModifierInSale"
        elif tag == "Tender":
            return "Tender"
        elif tag == "BSCMTransaction":
            return "BSCMTransaction"

    def safe_list_get(l, i, default='', attribute_flag=False):
        try:
            itm = l[i]
            if attribute_flag:
                return itm
            else:
                return itm.text
        except IndexError:
            return default

    def safe_attribute_get(e, keyword, default=''):
        try:
            return e.get(keyword)
        except AttributeError:
            return default

    for transaction in root.xpath('./x:Transaction', namespaces=namespaces):
        RETAIL_STORE_ID = safe_list_get(transaction.xpath('x:RetailStoreID', namespaces=namespaces), 0)

        try:
            MANDANT = RETAIL_STORE_ID[:3]
        except IndexError:
            MANDANT = ""

        FILENAME = ctx.file_name
        raw_xml.seek(0)
        MD5 = md5(raw_xml.read()).hexdigest()
        WORKSTATION_ID = safe_list_get(transaction.xpath('x:WorkstationID', namespaces=namespaces), 0)
        BON_ID = safe_list_get(transaction.xpath('x:SequenceNumber', namespaces=namespaces), 0)
        TX_LINENUM = transaction.sourceline
        RECEIPT_DATE = safe_list_get(transaction.xpath('x:BusinessDayDate', namespaces=namespaces), 0)
        OPERATOR_ID = safe_list_get(transaction.xpath('x:OperatorID', namespaces=namespaces), 0)
        SATZART = get_Satzart(transaction)

        RECEIPT_DATE_TIME = safe_list_get(transaction.xpath('x:RetailTransaction/x:ReceiptDateTime', namespaces=namespaces), 0)

        _OPERATOR_NAME = safe_list_get(transaction.xpath('x:OperatorID', namespaces=namespaces), 0, attribute_flag=True)
        OPERATOR_NAME = safe_attribute_get(_OPERATOR_NAME, 'OperatorName')

        CURRENCY_CODE = safe_list_get(transaction.xpath('x:CurrencyCode', namespaces=namespaces), 0)
        _VERSION = safe_list_get(transaction.xpath('x:RetailTransaction', namespaces=namespaces), 0, attribute_flag=True)
        VERSION = safe_attribute_get(_VERSION, 'Version')

        TOTAL_GRAND = safe_list_get(transaction.xpath('x:RetailTransaction/x:Total[@TotalType="TransactionGrandAmount"]', namespaces=namespaces), 0)
        TRANSACTION_TAX = safe_list_get(transaction.xpath('x:RetailTransaction/x:Total[@TotalType="TransactionTaxAmount"]', namespaces=namespaces), 0)

        if transaction.xpath('x:RetailTransaction/x:LineItem', namespaces=namespaces):
            TRANSACTION_COUNT = safe_list_get(transaction.xpath('x:RetailTransaction/x:TransactionCount', namespaces=namespaces), 0)
            ItemCount = safe_list_get(transaction.xpath('x:RetailTransaction/acs:ItemCount', namespaces=namespaces), 0)
            for line_item in transaction.xpath('x:RetailTransaction/x:LineItem', namespaces=namespaces):
                # Satzarten
                LINEITEM_SATZART = get_Satzart(line_item)

                # Linenums
                LINEITEM_LINENUM = line_item.sourceline

                _ITEM_TYPE = safe_list_get(line_item.xpath("x:Sale", namespaces=namespaces), 0, attribute_flag=True)
                if not _ITEM_TYPE:
                    _ITEM_TYPE = safe_list_get(line_item.xpath("x:Return", namespaces=namespaces), 0, attribute_flag=True)
                ITEM_TYPE = safe_attribute_get(_ITEM_TYPE, 'ItemType')

                ITEM_ID = safe_list_get(line_item.xpath("x:Sale/x:ItemID", namespaces=namespaces), 0)
                ITEM_DESCRIPTION = safe_list_get(line_item.xpath("x:Sale/x:Description", namespaces=namespaces), 0)
                REGULAR_SALES_UNIT_PRICE = safe_list_get(line_item.xpath("x:Sale/x:RegularSalesUnitPrice", namespaces=namespaces), 0)
                DiscountAmount = safe_list_get(line_item.xpath("x:Sale/x:DiscountAmount", namespaces=namespaces), 0)

                QUANTITY = safe_list_get(line_item.xpath("x:Sale/x:Quantity", namespaces=namespaces), 0)

                UNIT_LIST_PRICE = safe_list_get(line_item.xpath("x:Sale/x:RegularSalesUnitPrice", namespaces=namespaces), 0)
                ENTRY_METHOD = line_item.get('EntryMethod')

                _TENDER_ROOT = safe_list_get(line_item.xpath("x:Tender", namespaces=namespaces), 0, attribute_flag=True)
                TENDER_TYPE_CODE = safe_attribute_get(_TENDER_ROOT, 'TypeCode')
                TENDER_TYPE = safe_attribute_get(_TENDER_ROOT, 'TenderType')
                TENDER_AMOUNT = safe_list_get(line_item.xpath("x:Tender/x:Amount", namespaces=namespaces), 0)

                ctx.emit(
                    MANDANT,
                     FILENAME,
                     MD5,
                     RETAIL_STORE_ID,
                     WORKSTATION_ID,
                     BON_ID,
                     str(TX_LINENUM),
                     RECEIPT_DATE,
                     OPERATOR_ID,
                     SATZART,
                     TRANSACTION_TAX,
                     TRANSACTION_COUNT,
                     ItemCount,
                     RECEIPT_DATE_TIME,
                     OPERATOR_NAME,
                     CURRENCY_CODE,
                     VERSION,
                     TOTAL_GRAND,
                     str(LINEITEM_SATZART),
                     str(LINEITEM_LINENUM),
                     ITEM_TYPE,
                     ITEM_ID,
                     ITEM_DESCRIPTION,
                     REGULAR_SALES_UNIT_PRICE,
                     DiscountAmount,
                     QUANTITY,
                     UNIT_LIST_PRICE,
                     ENTRY_METHOD,
                     TENDER_TYPE_CODE,
                     TENDER_TYPE,
                     TENDER_AMOUNT
                         )
        else:
            ctx.emit(
                *[MANDANT,
                 FILENAME,
                 MD5,
                 RETAIL_STORE_ID,
                 WORKSTATION_ID,
                 BON_ID,
                 str(TX_LINENUM),
                 RECEIPT_DATE,
                 OPERATOR_ID,
                 SATZART,
                 TRANSACTION_TAX,
                 '',
                 '',
                 RECEIPT_DATE_TIME,
                 OPERATOR_NAME,
                 CURRENCY_CODE,
                 VERSION,
                 TOTAL_GRAND,
                 *[""] * 13])
/