import logging

def get_logger(log_level="NOTSET"):
    """Get a logger configured to write to the console.

    Args:
        log_level (str): The logging level to use for the logger and console
            handler. Defaults to "INFO".

    Returns:
        A logger configured to write log messages with a level equal to or higher
        than `log_level` to the console.
    """
    # create logger
    logger = logging.getLogger("kafkactl")
    log_level_number = logging.getLevelName(log_level.upper())
    logger.setLevel(log_level_number)

    # create console handler and set log level
    ch = logging.StreamHandler()
    ch.setLevel(log_level.upper())

    # create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)

    return logger