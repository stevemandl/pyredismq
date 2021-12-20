"""
Debugging Module
"""

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
    logger: Union[logging.Logger, str, None] = None,
    handler: Optional[logging.StreamHandler] = None,
    level: Union[int, str] = logging.DEBUG,
) -> None:
    """
    Add a stream handler with our custom formatter to a logger.
    """
    logger_ref: logging.Logger

    # find a reference to the logger
    if isinstance(logger, logging.Logger):
        logger_ref = logger

    elif isinstance(logger, str):
        # check for a valid logger name
        if logger not in logging.Logger.manager.loggerDict:  # type: ignore
            raise RuntimeError("not a valid logger name: %r" % (logger,))

        # get the logger
        logger_ref = logging.getLogger(logger)

    elif logger is None:
        # get the root logger
        logger_ref = logging.getLogger(None)

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

    # find the level if it is a name
    if isinstance(level, str):
        try:
            level = getattr(logging, level.upper())
        except:
            raise ValueError(f"not a logging level name: {level}")

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
