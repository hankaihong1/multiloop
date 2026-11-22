"""Shared fixtures for multiloop tests."""

import pytest

from multiloop.testing import wait_all_tasks_blocked


@pytest.fixture
def yielder():
    """Provide wait_all_tasks_blocked as a test-local name for brevity."""
    return wait_all_tasks_blocked
