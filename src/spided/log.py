import logging


def create_stream_logger(name, level=logging.DEBUG):
    datefmt_classic = "%Y-%m-%d %H:%M:%S"
    fmt_classic = '%(asctime)s.%(msecs)03d UTC %(levelname)s %(name)s: %(message)s'

    formatter = logging.Formatter(fmt=fmt_classic, datefmt=datefmt_classic)
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    return logger


def getLogger(name):
    return logging.getLogger(name)