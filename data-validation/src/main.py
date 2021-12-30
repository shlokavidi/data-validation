'''
Data Validation After Migration from MySQL to Snowflake

Apart from basic validation techniques like comparison of number of rows and
columns (reconciliation test), hashing has been
used to perform validation.

We get the hash value of each tuple in the MySQL table and compare it with the
hash value of each tuple in the Snowflake table. This process can be followed
when no operations are performed on the data before migration
(example - deleting/adding rows or columns, transforming data to
an alternative form, etc.).

Multithreading has been used as a basic approach to speed up the
process - two threads have been used to simultaneously get the hash
values of both the MySQL table and the Snowflake table.
'''
import mysql.connector
import snowflake.connector
import pandas as pd
import hashlib
import numpy as np
import time
import threading
from snowflake.connector.pandas_tools import write_pandas
import sys

import logging.config
import logging
from conn2db import MyDBConnection
from create_sf_table import create_snowflake_table
from get_config import get_ini_config
from get_config import get_common_config_value
import defines


logging.config.fileConfig(fname = defines.LOGGER_FILE_PATH,
                                    disable_existing_loggers = False)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Needed in all the functions
tab_name = get_common_config_value(defines.TABLE_NAME)
sql_cmd_g = ("Select * from %s" % tab_name)


# Migrating data from MySQL to Snowflake
def migrate(mysql_con, sf_con):
    '''
    Migrating data from MySQL to Snowflake
    '''

    sf_con.my_write_pandas(mysql_df_g, table_name = tab_name,
        schema = 'public', quote_identifiers=False )
    logger.info('***  Migration completed!  ***')


def get_hash_table(conn, df_type):
    '''
    Getting hash values of all rows of SQL table (Hash values
    to be used for comparing)
    '''

    if df_type == defines.MYSQL_STR:
        get_hash_table.df_sql_hash = mysql_df_g
        get_hash_table.hashx = pd.Series((hash(tuple(row)) for _,
                row in get_hash_table.df_sql_hash.iterrows()))
        get_hash_table.df_sql_hash['%s' % df_type] = get_hash_table.hashx
        # logger.info(get_hash_table.df_sql_hash)

    elif df_type == defines.SNOWFLAKE_STR:
        get_hash_table.df_sf_hash = sf_df_g
        get_hash_table.hashx = pd.Series((hash(tuple(row)) for _,
                row in get_hash_table.df_sf_hash.iterrows()))
        get_hash_table.df_sf_hash['%s' % df_type] = get_hash_table.hashx
        # logger.info(get_hash_table.df_sf_hash)


def basic_validation(mysql_con, sf_con):
    '''
    Does basic validations by checking number of rows, etc.
    '''

    ''' Validate consistency in columns '''

    query_l = ("desc table %s; " % tab_name)
    sf_count = len(sf_con.query(query_l))
    query_l = ("desc %s; " % tab_name)
    mysql_count = len(mysql_con.query(query_l))
    logger.info("COLUMNS> mysql_count: %s  and sf_count: %s" % (mysql_count, sf_count))

    if sf_count == mysql_count:
        logger.info('Columns validation -- PASS')
    else:
        logger.error('Colums validataion - FAIL')
        logger.error('Colums are not consistent. Colums in MySQL is %s and in Snowflake is %s'
                % (mysql_count, sf_count))
        logger.critical('Exiting validation')
        sys.exit(1)

    ''' Validate number of rows'''
    query_l = ('Select count(*) from %s ; ' % tab_name)
    res_set = mysql_con.query(query_l)
    mysql_count = res_set[0][0]
    res_set = sf_con.query(query_l)
    sf_count = res_set[0][0]
    logger.info("ROWS> mysql_count: %s  and sf_count: %s" % (mysql_count, sf_count))
    if mysql_count == sf_count:
        logger.info('Rows validation - PASS')
    else:
        logger.error('Rows validataion - FAIL')
        logger.error('Rows are not consistent. Rows in MySQL is %s and in Snowflake is %s'
                % (mysql_count, sf_count))
        logger.critical('Exiting validation')
        sys.exit(2)
    logger.info('Reconcilation test - PASS')

mysql_conn = MyDBConnection('mysql')
sf_conn = MyDBConnection('snowflake')
create_snowflake_table(sf_conn)

mysql_conx = mysql_conn.connection
mysql_df_g = pd.read_sql(sql_cmd_g, mysql_conx)

sf_conx = sf_conn.connection
sf_df_g = pd.read_sql(sql_cmd_g, sf_conx)

migrate(mysql_conn.connection, sf_conn)
basic_validation(mysql_conn, sf_conn)

time_without_thread = time.time()
get_hash_table(mysql_conn, defines.MYSQL_STR)
get_hash_table(sf_conn, defines.SNOWFLAKE_STR)
time_without_thread = time.time() - time_without_thread
logger.info('Time for hashing entire table without multithreads: %s ms' % str(time_without_thread)[:10])

time_with_thread = time.time()
th1 = threading.Thread(target = get_hash_table(mysql_conn, defines.MYSQL_STR), args=())
th2 = threading.Thread(target = get_hash_table(sf_conn, defines.SNOWFLAKE_STR), args=())

th1.start()
th2.start()
th1.join()
th2.join()
Hash_table = pd.DataFrame()
time_with_thread = time.time() - time_with_thread
logger.info('Time for hashing entire table with multithreads: %s ms' % str(time_with_thread)[:10])

Hash_table["sql Hash Value"] = get_hash_table.df_sql_hash[defines.MYSQL_STR]
Hash_table["SF Hash Value"] = get_hash_table.df_sf_hash[defines.SNOWFLAKE_STR]

# Comparing hash values of MySQL rows and hash values of Snowflake rows
Hash_table["comparison_column"] = np.where(Hash_table["sql Hash Value"] == Hash_table["SF Hash Value"], "Same", "Not Same")
# logger.info(Hash_table)
percentage = (time_without_thread - time_with_thread) / time_without_thread * 100
logger.info('Percentage of time improvement with multithreading: %2.2f%s' \
            % (percentage, '%'))
