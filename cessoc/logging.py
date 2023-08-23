import logging
import os
from pythonjsonlogger import jsonlogger
from logging.handlers import RotatingFileHandler
import sys


class cessoc_logging:
    logging_setup = False
    logger = None

    def set_root_logging(force_json_logging=False, file_log=False):
        """
        Sets up the root logger so it will properly output data.
        """
        root_logger = logging.getLogger()
        logging_severity_dict = {
            "info": logging.INFO,
            "debug": logging.DEBUG,
            "warning": logging.WARNING,
        }
        if os.environ["LOGGING_SEVERITY"] in logging_severity_dict:
            root_logger.setLevel(logging_severity_dict[os.environ["LOGGING_SEVERITY"]])
        else:
            root_logger.setLevel(logging.INFO)
        handler = cessoc_logging.get_logging_handler(file_log)
        if len(root_logger.handlers) != 0:
            root_logger.handlers = []
        root_logger.addHandler(handler)
        # set log format
        if cessoc_logging.isatty() and force_json_logging is False and file_log is False:
            # if terminal, set single line output
            handler.setFormatter(
                logging.Formatter(
                    "[%(asctime)s] [%(levelname)-19s] [%(lineno)04d] [%(name)-25s] [%(funcName)-15s] %(message)s"
                )
            )
        else:
            # if non-terminal, set json output
            handler.setFormatter(
                jsonlogger.JsonFormatter(
                    "%(asctime)s %(levelname)s %(lineno)d %(name)s %(funcName)s %(message)s"
                )
            )
        cessoc_logging.logging_setup = True

    def get_logging_handler(force_json_logging, file_log):
        """Gets the corresponding logging handler based on the type configured for the class. Can be a stdout or file logger"""
        if file_log:
            # Create the rotating file handler. Limit the size to 1000000Bytes ~ 1MB .
            return RotatingFileHandler("log.json", maxBytes=1000000, backupCount=2)
        else:
            # set logging to stdout
            return logging.StreamHandler(sys.stdout)

    def isatty() -> bool:
        """
        Check if stdout is going to a terminal
        :returns: True or False if current tty is a terminal
        """
        if (hasattr(sys.stdout, "isatty") and sys.stdout.isatty()) or (
            "TERM" in os.environ and os.environ["TERM"] == "ANSI"
        ):
            return True
        return False

    def getLogger(name, force_json_logging=False, file_log=False):
        """
        This gets a logger with the name specified. If the root logger has not been configured, it will do so.
        This only configures the root logger the first time it is configured.
        """
        if not cessoc_logging.logging_setup:
            cessoc_logging.set_root_logging(force_json_logging=force_json_logging, file_log=file_log)
            cessoc_logging.logger = logging.getLogger("cessoc")
        if name == "cessoc":
            return cessoc_logging.logger
        else:
            return logging.getLogger(name)
