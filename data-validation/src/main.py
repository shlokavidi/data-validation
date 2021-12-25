import mysql.connector
import snowflake.connector
import pandas as pd
import hashlib
import numpy as np
import time
import threading
from snowflake.connector.pandas_tools import write_pandas
import logging.config
import logging
from connection_to_db import connect_to_mysql
from connection_to_db import connect_to_snowflake


logging.config.fileConfig(fname = logger_file_path,
                                    disable_existing_loggers = False)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)



# Creating Snowflake table
def create_snowflake_table(sfq):
    logger.info('Creating Snowflake table...')
    sfq.execute("CREATE warehouse IF NOT EXISTS WH;")
    sfq.execute('USE warehouse WH;')
    sfq.execute("CREATE DATABASE IF NOT EXISTS Sales_db;")
    sfq.execute('USE DATABASE Sales_db;')
    sfq.execute('''CREATE OR REPLACE TABLE sales_table (Transaction_Id  Integer,
                                            Product_Id  Integer,
                                            Trans_Date  date,
                                            Region string,
                                            Zip_code string,
                                            Items_Sold Integer,
                                            Items_Returned Integer,
                                            Reason string,
                                            Unit_Price float,
                                            Revenue float);''')
    logger.info('***  Snowflake table created  ***')



# Migrating data from MySQL to Snowflake
def migrate(sf_con, df):
    logger.info('Initiating migration...')
    sfq.execute('USE warehouse WH;')
    sfq.execute('USE DATABASE Sales_db;')
    success, nchunks, nrows, _ = write_pandas(sf_con, df, table_name = 'sales_table', schema = 'public', quote_identifiers=False)
    logger.info('***  Migration completed!  ***')



# Getting hash values of all rows of SQL table (Hash values to be used for comparing)
def SQL_getHash(conn, data1):
    query = 'Select * from sales_table;'
    SQL_getHash.data1 = pd.read_sql(query, conn)
    SQL_getHash.hash2 = pd.Series((hash(tuple(row)) for _, row in SQL_getHash.data1.iterrows()))

    SQL_getHash.data1['sql hash'] = SQL_getHash.hash2
    logger.info('************************************************************************************')
    logger.debug(SQL_getHash.data1)



# Getting hash values of all rows of Snowflake table
def SF_getHash(sf_con_hash, sfq_hash):

    USE_WH = 'Use warehouse WH;'

    sfq_hash.execute(USE_WH)
    sfq_hash.execute('Use database Sales_db;')
    query = 'Select * from sales_table;  '
    SF_getHash.Table = pd.read_sql(query, sf_con_hash)
    logger.debug(SF_getHash.Table)

    SF_getHash.hash1 = pd.Series((hash(tuple(row)) for _, row in SF_getHash.Table.iterrows()))

    SF_getHash.Table['SF hash'] = SF_getHash.hash1
    logger.debug(SF_getHash.Table)




def basic_validation(df, sf_con, sfq):
    sfq.execute('Use warehouse WH;')
    sfq.execute('Use database Sales_db;')
    query = 'Select * from sales_table;  '
    sf_df = pd.read_sql(query, sf_con)

    if len(df.columns) == len(sf_df.columns):
        logger.info('Number of colums in MySQL table = Number of colums Snowflake table')
    else:
        logger.info('Number of rows in the two tables are not consistent.')
        logger.info('Exiting validation')
    if len(df) == len(sf_df):
        logger.info('Number of rows in MySQL table = Number of rows Snowflake table')
        logger.info('Reconciliation test - success')
    else:
        logger.info('Number of coulmns in the two tables are not consistent.')
        logger.info('Exiting validation')

    temp_sf = []
    temp_sql = []
    for i in list(df.columns):
        temp_sql.append(i.upper())
    for j in list(sf_df.columns):
        temp_sf.append(j.upper())
    if temp_sql == temp_sf:
        logger.info("Schema validation successful")
    else:
        logger.info('Schema inconsistent')
        logger.info('Exiting validation')




conn, cur, df = connect_to_mysql()
sf_con, sfq = connect_to_snowflake()
create_snowflake_table(sfq)
migrate(sf_con, df)
basic_validation(df, sf_con, sfq)

SF_getHash(sf_con, sfq)
SQL_getHash(conn, df)

t1= time.time()

th1 = threading.Thread(target=SF_getHash(sf_con, sfq),args=())
th2 = threading.Thread(target=SQL_getHash(conn, df),args=())

th1.start()
th2.start()
th1.join()
th2.join()
Hash_table = pd.DataFrame()
Hash_table["sql Hash Value"] = SQL_getHash.data1["sql hash"]
Hash_table["SF Hash Value"] = SF_getHash.Table["SF hash"]

# Comparing hash values of MySQL rows and hash values of Snowflake rows
Hash_table["comparison_column"] = np.where(Hash_table["sql Hash Value"] == Hash_table["SF Hash Value"], "Same", "Not Same")
logger.debug(Hash_table)

logger.info('Total time taken:',time.time()-t1)
