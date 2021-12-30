'''
This is a standalone program to move data from a CSV file
to MySQL file. A sample CSV file (sample_data_big.csv) of approx
0.5 million records is included in the 'data' folder. After loading the CSV
file to a table in MySQL, the same table will be migrated to snowflake
which is a part of main.py.

NOTE 1: sample_data.csv is a 10 line version of
    the data to use for convenience

NOTE 2: Loading of CSV file to MySQL may take time
    as the size of the CSV file is large.
'''

import mysql.connector
import pandas as pd

# from our modules
import defines
from conn2db import MyDBConnection
from get_config import get_common_config_value

tab_name = get_common_config_value(defines.TABLE_NAME)


def read_csv_file():
    csvdata = pd.read_csv(defines.CSV_FILE,
        index_col = False,
        delimiter = ',', dtype = {"Transaction_Id" : int, "Product_Id" : int, "Trans_Date": "string",
        "Region": "string", "Zip_code": "string", "Items_Sold": int,
    "Items_Returned": int, "Reason": "string", "Unit_Price": float, "Revenue": float})
    csvdata.head()
    return csvdata


def create_migrate_table(mysql_con):
    '''
    Creates the table for loading sample data
    '''

    sql_cmd = "CREATE TABLE if not EXISTS " + tab_name + """ (Transaction_Id  BIGINT(30),
                                            Product_Id  Integer(20),
                                            Trans_Date  date,
                                            Region varchar(20),
                                            Zip_code varchar(10),
                                            Items_Sold Integer(20),
                                            Items_Returned Integer(10),
                                            Reason varchar(100),
                                            Unit_Price float,
                                            Revenue float);"""
    mysql_con.execute(sql_cmd)
    print('Table Created!')


def load_csv_to_sql(mysql_con):
    print("Sample CSV filename: ", defines.CSV_FILE)
    csvdata = pd.read_csv(defines.CSV_FILE, skiprows = 1,
        index_col = False,
        delimiter = ',')
    for i, row in csvdata.iterrows():
        sql_cmd = "INSERT INTO " + tab_name +  " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        mysql_con.execute(sql_cmd, tuple(row))
    sql_cmd = 'commit'

    mysql_con.execute(sql_cmd)

    sql_cmd = ("Select count(*) from %s " % tab_name)
    count = mysql_con.query(sql_cmd)
    print("Current number of records: %s" % count[0][0])


# create_database()
mysql_conn = MyDBConnection(defines.MYSQL_STR)
create_migrate_table(mysql_conn)
load_csv_to_sql(mysql_conn)
print("MySQL data loaded!")
