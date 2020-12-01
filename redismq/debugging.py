"""
Debugging Module
"""

import os
import sys
import logging

from typing import (
    Any,
    Callable,
    Dict,
    Tuple,
    TypeVar,
    Union,
    Optional,
    cast,
)

# set the level of the root logger
_root = logging.getLogger()  # pylint: disable=invalid-name
_LOG_LEVEL = "WARNING"
if os.environ.get("LOG"):
    _LOG_LEVEL = os.environ["LOG"].upper()
_NUMERIC_LEVEL = getattr(logging, _LOG_LEVEL, None)
if not isinstance(_NUMERIC_LEVEL, int):
    raise ValueError("Invalid log level: %s" % _LOG_LEVEL)
_root.setLevel(_NUMERIC_LEVEL)

# add a stream handler for warnings and up
hdlr = logging.StreamHandler(stream=sys.stderr)
hdlr.setLevel(_NUMERIC_LEVEL)
hdlr.setFormatter(logging.Formatter("%(levelname)s:%(name)s %(message)s"))
_root.addHandler(hdlr)
_root.debug("log level %s", _LOG_LEVEL)
del hdlr

# module level logger
_log = logging.getLogger(__name__)  # pylint: disable=invalid-name

# types
FuncType = Callable[..., Any]
FuncTypeVar = TypeVar("FuncTypeVar", bound=FuncType)


def debugging(obj: FuncTypeVar) -> FuncTypeVar:
    """
    Decorator function that attaches a debugging logger to a class or function.
    """
    # create a logger for this object
    logger = logging.getLogger(obj.__module__ + "." + obj.__qualname__)

    # make it available to instances
    setattr(obj, "log_logger", logger)
    setattr(obj, "log_debug", logger.debug)

    return obj


# pylint: disable=bad-continuation
@debugging
def create_log_handler(
    logger: Union[str, logging.Logger] = "",
    handler: Optional[logging.StreamHandler] = None,
    level: int = logging.DEBUG,
) -> None:
    """
    Add a stream handler with our custom formatter to a logger.
    """
    logger_ref: logging.Logger

    if isinstance(logger, logging.Logger):
        logger_ref = logger

    elif isinstance(logger, str):
        # check for root
        if not logger:
            logger_ref = _log

        # check for a valid logger name
        elif logger not in logging.Logger.manager.loggerDict:  # type: ignore
            raise RuntimeError("not a valid logger name: %r" % (logger,))

        # get the logger
        logger_ref = logging.getLogger(logger)

    else:
        raise RuntimeError("not a valid logger reference: %r" % (logger,))

    # make a handler if one wasn't provided
    if not handler:
        handler = logging.StreamHandler()
        handler.setLevel(level)

        # use our formatter
        handler.setFormatter(logging.Formatter("%(levelname)s:%(name)s %(message)s"))

    # add it to the logger
    logger_ref.addHandler(handler)

    # make sure the logger has at least this level
    logger_ref.setLevel(level)


class LoggingMetaclass(type):
    """
    Logging Metaclass
    """

    def __new__(
        cls: Any,
        clsname: str,
        superclasses: Tuple[type, ...],
        attributedict: Dict[str, Any],
    ) -> "LoggingMetaclass":
        """
        Called for new classes inheriting from Logging, this calls the
        debugging function to set the _logger and _debug references to
        a Logger and its debug() method.
        """
        return cast(
            LoggingMetaclass,
            debugging(type.__new__(cls, clsname, superclasses, attributedict)),
        )


# pylint: disable=too-few-public-methods
class Logging(metaclass=LoggingMetaclass):
    """
    Inherit from this class to get a log_logger and log_debug method.
    """

    log_logger: logging.Logger
    log_debug: Callable[..., None]
