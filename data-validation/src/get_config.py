import logging

from Constant import *
from configparser import ConfigParser

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_ini_config(filename, section):
    """
    filename: Name of the configuration file
    section: Section of database configuration
    return: A dictionary of database parameters

    Read database configuration file and return a dictionary object
    """

    # create parser and read ini configuration file
    parser = ConfigParser()
    parser.read(filename)

    # get section
    db = {}
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            db[item[0]] = item[1]
    else:
        raise Exception('{0} not found in the {1} file'.format(section, filename))
        logger.critical('{0} not found in the {1} file'.format(section, filename))

    return db
