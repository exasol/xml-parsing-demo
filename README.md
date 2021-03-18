# XML-Parsing demo

##### Please note that this is an open source project which is not officially supported by EXASOL. We will try to help you as much as possible, but can't guarantee anything since this is not an official EXASOL product.

## Overview
When working with databases we often need to ingest different source formats. XML is one of them. This tutorial shows an approach to load XML files into Exasol using an FTP server as source and our UDF framework for parsing.

## Getting Started
- Run the SQL in the `1ddl.sql` file. This will create our test `SCHEMA`and our `TABLE`s.
- Run the SQL in the `2udf.sql` file. This will deploy two UDF functions providing the parsing functionality to work with XML.
- Rund the SQL in the `3scripts.sql`file. This will deploy a LUA script stringing together the whole ETL process of loading and transforming the data. The LUA script has to be parametrized with your FTP credentials etc. 

## Writeup
Please refer to the [ExaCommunity](https://community.exasol.com/t5/database-features/xml-parsing-in-retail-using-python3-udfs/ta-p/3304) for the tutorial for this repository.
