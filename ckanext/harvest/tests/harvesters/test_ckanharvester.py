import copy

from nose.tools import assert_equal
import json
from mock import patch

try:
    from ckan.tests.helpers import reset_db
    from ckan.tests.factories import Organization
except ImportError:
    from ckan.new_tests.helpers import reset_db
    from ckan.new_tests.factories import Organization
from ckan import model

from ckanext.harvest.tests.factories import (HarvestSourceObj, HarvestJobObj,
                                             HarvestObjectObj)
from ckanext.harvest.tests.lib import run_harvest
import ckanext.harvest.model as harvest_model
from ckanext.harvest.harvesters.ckanharvester import CKANHarvester

import mock_ckan

# Start CKAN-alike server we can test harvesting against it
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

    def test_harvest(self):
        results_by_guid = run_harvest(
            url='http://localhost:%s/' % mock_ckan.PORT,
            harvester=CKANHarvester())

        result = results_by_guid['dataset1-id']
        assert_equal(result['state'], 'COMPLETE')
        assert_equal(result['report_status'], 'added')
        assert_equal(result['dataset']['name'], mock_ckan.DATASETS[0]['name'])
        assert_equal(result['errors'], [])

        result = results_by_guid[mock_ckan.DATASETS[1]['id']]
        assert_equal(result['state'], 'COMPLETE')
        assert_equal(result['report_status'], 'added')
        assert_equal(result['dataset']['name'], mock_ckan.DATASETS[1]['name'])
        assert_equal(result['errors'], [])

    def test_harvest_twice(self):
        run_harvest(
            url='http://localhost:%s/' % mock_ckan.PORT,
            harvester=CKANHarvester())

        # change the modified date
        datasets = copy.deepcopy(mock_ckan.DATASETS)
        datasets[1]['metadata_modified'] = '2050-05-09T22:00:01.486366'
        with patch('ckanext.harvest.tests.harvesters.mock_ckan.DATASETS',
                   datasets):
            results_by_guid = run_harvest(
                url='http://localhost:%s/' % mock_ckan.PORT,
                harvester=CKANHarvester())

        # updated the dataset which has revisions
        result = results_by_guid[mock_ckan.DATASETS[1]['name']]
        assert_equal(result['state'], 'COMPLETE')
        assert_equal(result['report_status'], 'updated')
        assert_equal(result['dataset']['name'], mock_ckan.DATASETS[1]['name'])
        assert_equal(result['errors'], [])

        # the other dataset is unchanged and not harvested
        assert mock_ckan.DATASETS[1]['name'] not in result

    def test_harvest_invalid_tag(self):
        from nose.plugins.skip import SkipTest; raise SkipTest()
        results_by_guid = run_harvest(
            url='http://localhost:%s/invalid_tag' % mock_ckan.PORT,
            harvester=CKANHarvester())

        result = results_by_guid['dataset1-id']
        assert_equal(result['state'], 'COMPLETE')
        assert_equal(result['report_status'], 'added')
        assert_equal(result['dataset']['name'], mock_ckan.DATASETS[0]['name'])

    def test_exclude_organizations(self):
        config = {'organizations_filter_exclude': ['org1-id']}
        results_by_guid = run_harvest(
            url='http://localhost:%s' % mock_ckan.PORT,
            harvester=CKANHarvester(),
            config=json.dumps(config))
        assert 'dataset1-id' not in results_by_guid
        assert mock_ckan.DATASETS[1]['id'] in results_by_guid

    def test_include_organizations(self):
        config = {'organizations_filter_include': ['org1-id']}
        results_by_guid = run_harvest(
            url='http://localhost:%s' % mock_ckan.PORT,
            harvester=CKANHarvester(),
            config=json.dumps(config))
        assert 'dataset1-id' in results_by_guid
        assert mock_ckan.DATASETS[1]['id'] not in results_by_guid

    def test_harvest_not_modified(self):
        run_harvest(
            url='http://localhost:%s/' % mock_ckan.PORT,
            harvester=CKANHarvester())

        results_by_guid = run_harvest(
            url='http://localhost:%s/' % mock_ckan.PORT,
            harvester=CKANHarvester())

        # The metadata_modified was the same for this dataset so the import
        # would have returned 'unchanged'
        result = results_by_guid[mock_ckan.DATASETS[1]['name']]
        assert_equal(result['state'], 'COMPLETE')
        assert_equal(result['report_status'], 'not modified')
        assert 'dataset' not in result
        assert_equal(result['errors'], [])
