"""
tests/utils.py
utilities for testing
"""
import os
TEST_URL = os.getenv("REDIS_URL", "redis://localhost")
