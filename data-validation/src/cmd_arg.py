****************************************************************************

Command line arguments while runnning the App are parsed here.

usage: etl_main.py [-h] [-D database_name] [-e] [-d DEBUG]

Revenue recognition simulator and flattening engine:

optional arguments:
  -h, --help            show this help message and exit
  -D database_name, --Database_name database_name
                        the database (Snowflake or MySQL) to be used
  -e, --explain         Explains how it works
  -d DEBUG, --debug DEBUG
                        -d 1 for SQL debug -d 2 for general debug


****************************************************************************


import argparse
import sys
from Constant import *
import global_
import logging

# Logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def explain_how_it_works():
    """
    Reads and displays a text file (explain_etl_engine.txt) that explains
    the ETL Engine functionality.
    """

    inputf = None
    try:
        with open(ExplainFileName, 'r') as inputf:
            for line in inputf:
                print(line.strip())
    except IOError as e:
        if inFile is None:
            logger.critical ('Error in opening input text file',ExplainFileName)
        logger.critical ("%s - Operation failed. Exiting." % e.strerror)
        sys.exit(-1)



def parse_arguments():
    """
    Using argparse library, parse the command line arguments efficiently.
    """

    user_selected_db = ''
    yes = {'yes','y', 'ye', ''}

    # Create the parser
    rr_parser = argparse.ArgumentParser(description=app_description)

    # Add the arguments
    rr_parser.add_argument('-D', '--Database_name',
        metavar = 'database_name',
        type=str,
        #required=True,
        help='the database (Snowflake or MySQL) to be used')

    rr_parser.add_argument('-e', '--explain',
        action='store_true',
        help='Explains how it works')

    rr_parser.add_argument('-d', '--debug',
        #dest = 'd_mode_value',
        type = int,
        #action='store_true',
        help='-d 1 for SQL debug \n \
              -d 2 for general debug\n')

    # Execute the parse_args() method
    args = rr_parser.parse_args()

    if args.Database_name:
        user_selected_db = args._get_kwargs()[0][1]
        if ((user_selected_db != mysql_db) and (user_selected_db != snowflake_db)) :
            logger.critical("Invalid database name '%s' provided " % user_selected_db)
            sys.exit(-2)
        else:
            logger.info("Database_name '%s' provided " % user_selected_db)
    elif user_selected_db != mysql_db and user_selected_db != snowflake_db:
        logger.info("Database option (-D) not given. Default DB selected:", Default_database)
        user_selected_db = Default_database

    if user_selected_db == mysql_db :
        global_.mysql_db_selected = True
    elif user_selected_db == snowflake_db:
        global_.snowflake_db_selected = True
    else:
        # It should be coming here. Provide a gentle error message
        logger.critical("Database_name selection issue. Database not selected")

    if args.debug:
        global_.debug_mode = args._get_kwargs()[1][1]
        # logger.info("Selected debug_mode :", global_.debug_mode)
        print("Selected debug_mode :", global_.debug_mode)

    if args.explain:
        explain_how_it_works()
        sys.exit(0) # Explained. So Exit.
