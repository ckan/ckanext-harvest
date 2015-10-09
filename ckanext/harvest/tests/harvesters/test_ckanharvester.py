from nose.tools import assert_equal

from ckan import model

from ckanext.harvest.tests.factories import HarvestSourceObj, HarvestJobObj
import ckanext.harvest.model as harvest_model
from ckanext.harvest.harvesters.ckanharvester import CKANHarvester

import mock_ckan

# Start CKAN-alike server we can test harvesting against
mock_ckan.serve()


class TestCkanHarvester(object):
    @classmethod
    def setup_class(cls):
        harvest_model.setup()

    @classmethod
    def teardown_class(cls):
        model.repo.rebuild_db()

    def test_gather_normal(self):
        source = HarvestSourceObj(url='http://localhost:%s/' % mock_ckan.PORT)
        job = HarvestJobObj(source=source)
        model.Session
        obj_ids = CKANHarvester().gather_stage(job)
        assert_equal(len(obj_ids), 3)
