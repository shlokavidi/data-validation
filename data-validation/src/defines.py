'''
Here, we have defined the constants we use in our project.

CONFIG_FILE - path to the file containing the required credentials (MySQL and Snowflake)
TABLE_NAME - name of MySQL table
MYSQL_DB_NAME - name of MySQL database

SNOWFLAKE_STR - string to represent snoflake
MYSQL_STR - string to respent MySQL

CSV_FILE - path to sample data
LOGGER_FILE_PATH - path to the logger file

'''


CONFIG_FILE = "../config/dv_config.ini"
TABLE_NAME = 'table_name'
MYSQL_DB_NAME = 'mysql_db_name'

SNOWFLAKE_STR = 'snowflake'
MYSQL_STR = 'mysql'

CSV_FILE = "../data/sample_data.csv"
LOGGER_FILE_PATH = '../config/dv_logger.ini'
