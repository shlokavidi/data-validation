'''
Creating Snowflake table
'''
from get_config import get_common_config_value
import defines

import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)



# Creating Snowflake table
def create_snowflake_table(sfq):
    logger.info('Creating Snowflake table...')
    tab_name = get_common_config_value(defines.TABLE_NAME)
    sql_cmd = ("CREATE OR REPLACE TABLE " + tab_name +
                """ (Transaction_Id  Integer,
                    Product_Id  Integer,
                    Trans_Date  date,
                    Region string,
                    Zip_code string,
                    Items_Sold Integer,
                    Items_Returned Integer,
                    Reason string,
                    Unit_Price float,
                    Revenue float);""")
    sfq.execute(sql_cmd)

    # Validate if table created
    sql_cmd = ("select exists (select count(*) from %s);" % tab_name)
    exists = []
    try:
        exists = sfq.query(sql_cmd)
    except Exception as err:
        logger.error("table %s creation error -- %s" % (tab_name, err))
    if exists:
        logger.info('***  Snowflake table %s created  ***' % tab_name)
    else:
        logger.error('creating table %s unusccessful' % tab_name)
