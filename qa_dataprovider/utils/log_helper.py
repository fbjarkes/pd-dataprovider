import logging


def init_logging(loggers: [logging.Logger], verbose: int = 0):
    logging.Logger.manager.root.setLevel(logging.WARNING)
    level = logging.WARNING
    if verbose == 1:
        level = logging.INFO
    elif verbose == 2:
        level = logging.DEBUG
    for logger in loggers:
        logger.setLevel(level)
