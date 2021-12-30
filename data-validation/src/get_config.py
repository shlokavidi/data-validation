'''
To read database configuration file and return a dictionary object
'''
import logging
import defines
from configparser import ConfigParser

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_ini_config(config_fname, Config_section):
    """
    config_fname: Name of the configuration file
    Config_section: Config_section of database configuration
    return: A dictionary of database parameters
    """

    # create parser and read ini configuration file
    parser = ConfigParser()
    parser.read(config_fname)

    # get Config_section
    config_dict = {}
    if parser.has_section(Config_section):
        items = parser.items(Config_section)
        for item in items:
            config_dict[item[0]] = item[1]
    else:
        #raise Exception('{0} not found in the {1} file'.format(Config_section, filename))
        logger.critical('{0} not found in the {1} file'.format(Config_section, filename))

    return config_dict



def get_common_config_value(key_name):

    """ Get parameter from config file in 'common' Config_section"""

    common_Config_section = 'common'
    param_value = ''

    logger.info("key_name: %s" % key_name)

    common_config  = get_ini_config(defines.CONFIG_FILE, common_Config_section)
    logger.info(common_config)
    # Check if key_name is in the config file
    for item in common_config:
        if item == key_name:
            param_value = common_config[item]

    logger.info("param_value:%s", param_value)
    return (param_value)
