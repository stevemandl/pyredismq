"""Debugging.py""" # pylint: disable=invalid-name

import os
import sys
import logging

# set the level of the root logger
_root = logging.getLogger()
_LOG_LEVEL = 'WARNING'
if os.environ.get("LOG"):
    _LOG_LEVEL = os.environ['LOG'].upper()
_NUMERIC_LEVEL = getattr(logging, _LOG_LEVEL, None)
if not isinstance(_NUMERIC_LEVEL, int):
    raise ValueError('Invalid log level: %s' % _LOG_LEVEL)
# _root.setLevel(_NUMERIC_LEVEL)
# add a stream handler for warnings and up
hdlr = logging.StreamHandler(stream=sys.stderr)
hdlr.setLevel(_NUMERIC_LEVEL)
hdlr.setFormatter(logging.Formatter("%(levelname)s:%(name)s %(message)s"))
_root.addHandler(hdlr)
_root.debug('log level %s',_LOG_LEVEL)
del hdlr

#
#   _StringToHex
#

def _StringToHex(x, sep=''): # pylint: disable=invalid-name
    return sep.join(["%02X" % (ord(c),) for c in x])

#
#   ModuleLogger
#

def ModuleLogger(globs): # pylint: disable=invalid-name
    """Create a module level logger."""
    # make sure that _debug is defined
    if not '_debug' in globs:
        raise RuntimeError( "define _debug before creating a module logger" )

    # create a logger to be assgined to _log
    logger = logging.getLogger(globs['__name__'])

    # put in a reference to the module globals
    logger.globs = globs

    return logger

#
#   Typical Use
#
#   Create a _debug variable in the module, then use the ModuleLogger function
#   to create a "module level" logger.  When a handler is added to this logger
#   or a child of this logger, the _debug variable will be incremented.
#

# some debugging
_debug = 0 # pylint: disable=invalid-name
_log = ModuleLogger(globals())


#
#   _LoggingWrapper
#

def add_loggers(obj, logger_name):
    """adds logger methods to obj"""
    # return logger dict for this name
    logger = logging.getLogger(logger_name)
    logger.setLevel(_NUMERIC_LEVEL)

    # make it available to instances
    # pylint: disable=protected-access
    log_dict = {
        '_logger': logger,
        'log_debug': logger.debug,
        'log_info': logger.info,
        'log_warning': logger.warning,
        'log_error': logger.error,
        'log_exception': logger.exception,
        'log_fatal': logger.fatal
    }
    list(map(lambda item: setattr(obj, *item), log_dict.items()))

#
#   Logging
#

class Logging:
    """Logging"""
    def F(self):
        """Callable placeholder"""

    _logger = _debug = _info = _warning = _error = _exception = _fatal = F
    def __new__(cls, *args): # pylint: disable=unused-argument
        add_loggers(cls, cls.__module__ + '.' + cls.__name__)
        self = super(Logging, cls).__new__(cls)
        return self


#
#   FunctionLogging
#
#   This decorator is used to wrap a function.
#

def FunctionLogging(f): # pylint: disable=invalid-name
    """FunctionLogging"""
    log_name = f.__module__ + '.' + f.__name__
    add_loggers(f, log_name)
    return f
