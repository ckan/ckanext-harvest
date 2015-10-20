from nose.tools import assert_equal
import json

try:
    from ckan.tests.helpers import reset_db
    from ckan.tests.factories import Organization
except ImportError:
    from ckan.new_tests.helpers import reset_db
    from ckan.new_tests.factories import Organization
from ckan import model

from ckanext.harvest.tests.factories import (HarvestSourceObj, HarvestJobObj,
                                             HarvestObjectObj)
import ckanext.harvest.model as harvest_model
from ckanext.harvest.harvesters.ckanharvester import CKANHarvester

import mock_ckan

# Start CKAN-alike server we can test harvesting against
mock_ckan.serve()


class TestCkanHarvester(object):
    @classmethod
    def setup(cls):
        reset_db()
        harvest_model.setup()

    def test_gather_normal(self):
        source = HarvestSourceObj(url='http://localhost:%s/' % mock_ckan.PORT)
        job = HarvestJobObj(source=source)

        harvester = CKANHarvester()
        obj_ids = harvester.gather_stage(job)

        assert_equal(type(obj_ids), list)
        assert_equal(len(obj_ids), len(mock_ckan.DATASETS))
        harvest_object = harvest_model.HarvestObject.get(obj_ids[0])
        assert_equal(harvest_object.guid, mock_ckan.DATASETS[0]['id'])

    def test_fetch_normal(self):
        source = HarvestSourceObj(url='http://localhost:%s/' % mock_ckan.PORT)
        job = HarvestJobObj(source=source)
        harvest_object = HarvestObjectObj(guid=mock_ckan.DATASETS[0]['id'],
                                          job=job)

        harvester = CKANHarvester()
        result = harvester.fetch_stage(harvest_object)

        assert_equal(result, True)
        assert_equal(
            harvest_object.content,
            json.dumps(
                mock_ckan.convert_dataset_to_restful_form(
                    mock_ckan.DATASETS[0])))

    def test_import_normal(self):
        org = Organization()
        harvest_object = HarvestObjectObj(
            guid=mock_ckan.DATASETS[0]['id'],
            content=json.dumps(mock_ckan.convert_dataset_to_restful_form(
                               mock_ckan.DATASETS[0])),
            job__source__owner_org=org['id'])

        harvester = CKANHarvester()
        result = harvester.import_stage(harvest_object)

        assert_equal(result, True)
        assert harvest_object.package_id
        dataset = model.Package.get(harvest_object.package_id)
        assert_equal(dataset.name, mock_ckan.DATASETS[0]['name'])
