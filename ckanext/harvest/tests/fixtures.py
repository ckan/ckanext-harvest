import pytest

import ckanext.harvest.model as harvest_model
from ckanext.harvest import queue


@pytest.fixture
def harvest_setup():
    harvest_model.setup()


@pytest.fixture
def clean_queues():
    queue.purge_queues()
