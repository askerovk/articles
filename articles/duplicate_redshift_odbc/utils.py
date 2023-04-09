"""Miscellaneous utility functions."""

import logging


def get_logger(name):
    """Generate a logger with a given name.

    Args:
        name (str): logger name

    Returns:
        logger (logging.Logger): logger object.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    file_formatter = logging.Formatter(
        '%(levelname)s - %(asctime)s - %(message)s - %(module)s',
        "%Y-%m-%d %H:%M:%S")

    file_handler = logging.FileHandler("logfile.log")
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(file_formatter)

    logger.addHandler(file_handler)

    # console_formatter = logging.Formatter(
    #     '%(levelname)s - %(asctime)s - %(message)s - %(module)s',
    #     "%Y-%m-%d %H:%M:%S")

    # console_handler = logging.StreamHandler()
    # console_handler.setLevel(logging.INFO)
    # console_handler.setFormatter(console_formatter)

    # logger.addHandler(console_handler)

    return logger


def odbc_engine(credentials, odbc_driver):
    """Create a dictionary of parameters for an odbc connection.

    Args:
        credentials (dict): a dictionary of redshift credentials.
        odbc_driver (str): string containing the odbc driver name or its file path.

    Returns:
        odbc_engine (dict): parameters for an odbc connection
    """

    odbc_engine = {
        'Driver': odbc_driver,
        'Server': credentials['destination']['host'],
        'Database': credentials['destination']['database'],
        'UID': credentials['destination']['username'],
        'PWD': credentials['destination']['password'],
        'Port': credentials['destination']['port'],
        # For some reason, the default behavior for ODBC is to convert boolean values to
        # string. The variale below fixes this.
        'BoolsAsChar': 0
    }

    return odbc_engine
