import factory
from ckanext.harvest.model import HarvestSource, HarvestJob

class HarvestSourceFactory(factory.Factory):
    FACTORY_FOR = HarvestSource

    url = "http://harvest.test.com"
    type = "test-harvest-source"

class HarvestJobFactory(factory.Factory):
    FACTORY_FOR = HarvestJob

    status = "New"
    source = factory.SubFactory(HarvestSourceFactory)
