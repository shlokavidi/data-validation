import mysql.connector
import snowflake.connector
import pandas as pd
import hashlib
import numpy as np
import time
import threading
from snowflake.connector.pandas_tools import write_pandas
# import configparser
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


'connecting to MySQL'
def connect_to_mysql():
    logger.info('Tring to connect to MySQL.....')

    conn = mysql.connector.connect(host='localhost',
                    user='root',
                    password='root4mysql',
                    database = 'Sales_db')

    cur = conn.cursor()
    query = 'select * from sales_table;'
    df = pd.read_sql(query, conn)
    logger.info('***  Connection to MySQL seccessful  ***')
    logger.info('Connection Verification: Length of SQL table: %s' % len(df))
    print('*****************************************************************')
    return conn, cur, df



'connecting to Snowflake'
def connect_to_snowflake():

    logger.info("Trying to conenct to Snowflake.....")

    sf_con = snowflake.connector.connect(
        user='Gri',
        password='EF2win&win',
        account='fza75044'
        )
    logger.info('***  Connection to snowflake successful  ***')
    print('*****************************************************************')
        sfq = sf_con.cursor()

    return sf_con, sfq
