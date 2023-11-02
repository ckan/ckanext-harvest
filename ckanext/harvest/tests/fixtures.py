import pytest

from ckanext.harvest import queue


@pytest.fixture
def clean_queues():
    queue.purge_queues()
