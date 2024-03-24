import logging
from typing import Optional


def _enable_single_class_logger(
    logger_name: str,
    log_level: int = logging.DEBUG,
    log_format: Optional[str] = None,
    log_stream: logging.StreamHandler = None,
    propagate: bool = False,
):
    logger = logging.getLogger(logger_name)
    logger.setLevel(log_level)
    if log_format is None:
        log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    formatter = logging.Formatter(log_format)
    if log_stream is None:
        log_stream = logging.StreamHandler()
    log_stream.setFormatter(formatter)
    logger.addHandler(log_stream)
    logger.propagate = propagate
    return logger


def _show_initialized_hsfs_module_loggers() -> None:
    print(
        [
            logger
            for logger in logging.Logger.manager.loggerDict.keys()
            if logger.startswith("hsfs")
        ]
    )


def _show_available_hsfs_module_loggers() -> None:
    loggers = [
        "hsfs.core.vector_server",
        "hsfs.core.online_store_rest_client_engine",
        "hsfs.core.online_store_rest_client_api",
        "hsfs.core.online_store_sql_client",
    ]
    print(loggers)
