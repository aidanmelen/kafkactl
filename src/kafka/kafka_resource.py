from abc import ABC, abstractmethod

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

class KafkaResource(ABC):
    """An abstract class for a Kafka resource."""

    def __init__(self, admin_client, log_level="NOTSET"):
        self.admin_client = admin_client
        self.logger = get_logger(log_level)
    
    @abstractmethod
    def get(self) -> bool:
        """Get one or many resources."""
        raise NotImplemented
    
    @abstractmethod
    def create(self) -> bool:
        """Create one or many resources."""
        raise NotImplemented
    
    @abstractmethod
    def describe(self) -> bool:
        """Describe one or many resources."""
        raise NotImplemented
    
    @abstractmethod
    def alter(self) -> bool:
        """Alter one or many resources."""
        raise NotImplemented
    
    @abstractmethod
    def delete(self) -> bool:
        """Delete one or many resources."""
        raise NotImplemented