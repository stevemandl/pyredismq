"""
Test Debugging
"""

import pytest  # type: ignore
from redismq.debugging import debugging, create_log_handler
import logging


@debugging
class DebugTester:
    def __init__(self) -> None:
        root_logger = logging.getLogger(None)
        a_handler = logging.StreamHandler()
        DebugTester.log_debug("")
        create_log_handler('test_debugging.DebugTester',level='WARN')
        create_log_handler(handler=a_handler)
        root_logger.removeHandler(a_handler)

@pytest.mark.asyncio  # type: ignore[misc]
async def test_debugging() -> None:
    "test debugging"
    t = DebugTester()
