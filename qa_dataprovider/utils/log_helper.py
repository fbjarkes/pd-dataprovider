import logging


def init_logging(loggers, verbose):
    logging.Logger.manager.root.setLevel(logging.WARNING)
    level = logging.WARNING
    if verbose == 1:
        level = logging.INFO
    elif verbose == 2:
        level = logging.DEBUG
    for logger in loggers:
        logger.setLevel(level)
