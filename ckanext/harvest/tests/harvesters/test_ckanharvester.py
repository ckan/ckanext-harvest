import copy

from nose.tools import assert_equal, assert_raises, assert_in
import json
from mock import patch, MagicMock


try:
    from ckan.tests.helpers import reset_db, call_action
    from ckan.tests.factories import Organization, Group
except ImportError:
    from ckan.new_tests.helpers import reset_db, call_action
    from ckan.new_tests.factories import Organization, Group
from ckan import model
from ckan.plugins import toolkit

from ckanext.harvest.tests.factories import (HarvestSourceObj, HarvestJobObj,
                                             HarvestObjectObj)
from ckanext.harvest.tests.lib import run_harvest
import ckanext.harvest.model as harvest_model
from ckanext.harvest.harvesters.ckanharvester import CKANHarvester

import mock_ckan

# Start CKAN-alike server we can test harvesting against it
mock_ckan.serve()


def was_last_job_considered_error_free():
    last_job = model.Session.query(harvest_model.HarvestJob) \
                    .order_by(harvest_model.HarvestJob.created.desc()) \
                    .first()
    job = MagicMock()
    job.source = last_job.source
    job.id = ''
    return bool(CKANHarvester._last_error_free_job(job))


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

        assert_equal(job.gather_errors, [])
        assert_equal(type(obj_ids), list)
        assert_equal(len(obj_ids), len(mock_ckan.DATASETS))
        harvest_object = harvest_model.HarvestObject.get(obj_ids[0])
        assert_equal(harvest_object.guid, mock_ckan.DATASETS[0]['id'])
        assert_equal(
            json.loads(harvest_object.content),
            mock_ckan.DATASETS[0])

    def test_fetch_normal(self):
        source = HarvestSourceObj(url='http://localhost:%s/' % mock_ckan.PORT)
        job = HarvestJobObj(source=source)
        harvest_object = HarvestObjectObj(
            guid=mock_ckan.DATASETS[0]['id'],
            job=job,
            content=json.dumps(mock_ckan.DATASETS[0]))

        harvester = CKANHarvester()
        result = harvester.fetch_stage(harvest_object)

        assert_equal(harvest_object.errors, [])
        assert_equal(result, True)

    def test_import_normal(self):
        org = Organization()
        harvest_object = HarvestObjectObj(
            guid=mock_ckan.DATASETS[0]['id'],
            content=json.dumps(mock_ckan.DATASETS[0]),
            job__source__owner_org=org['id'])

        harvester = CKANHarvester()
        result = harvester.import_stage(harvest_object)

        assert_equal(harvest_object.errors, [])
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
        assert was_last_job_considered_error_free()

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
        result = results_by_guid[mock_ckan.DATASETS[1]['id']]
        assert_equal(result['state'], 'COMPLETE')
        assert_equal(result['report_status'], 'updated')
        assert_equal(result['dataset']['name'], mock_ckan.DATASETS[1]['name'])
        assert_equal(result['errors'], [])

        # the other dataset is unchanged and not harvested
        assert mock_ckan.DATASETS[0]['id'] not in result
        assert was_last_job_considered_error_free()

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
        config = {'organizations_filter_exclude': ['org1']}
        results_by_guid = run_harvest(
            url='http://localhost:%s' % mock_ckan.PORT,
            harvester=CKANHarvester(),
            config=json.dumps(config))
        assert 'dataset1-id' not in results_by_guid
        assert mock_ckan.DATASETS[1]['id'] in results_by_guid

    def test_include_organizations(self):
        config = {'organizations_filter_include': ['org1']}
        results_by_guid = run_harvest(
            url='http://localhost:%s' % mock_ckan.PORT,
            harvester=CKANHarvester(),
            config=json.dumps(config))
        assert 'dataset1-id' in results_by_guid
        assert mock_ckan.DATASETS[1]['id'] not in results_by_guid

    def test_remote_groups_create(self):
        config = {'remote_groups': 'create'}
        results_by_guid = run_harvest(
            url='http://localhost:%s' % mock_ckan.PORT,
            harvester=CKANHarvester(),
            config=json.dumps(config))
        assert 'dataset1-id' in results_by_guid
        # Check that the remote group was created locally
        call_action('group_show', {}, id=mock_ckan.GROUPS[0]['id'])

    def test_remote_groups_only_local(self):
        # Create an existing group
        Group(id='group1-id', name='group1')

        config = {'remote_groups': 'only_local'}
        results_by_guid = run_harvest(
            url='http://localhost:%s' % mock_ckan.PORT,
            harvester=CKANHarvester(),
            config=json.dumps(config))
        assert 'dataset1-id' in results_by_guid

        # Check that the dataset was added to the existing local group
        dataset = call_action('package_show', {}, id=mock_ckan.DATASETS[0]['id'])
        assert_equal(dataset['groups'][0]['id'], mock_ckan.DATASETS[0]['groups'][0]['id'])

        # Check that the other remote group was not created locally
        assert_raises(toolkit.ObjectNotFound, call_action, 'group_show', {},
                      id='remote-group')

    def test_harvest_not_modified(self):
        run_harvest(
            url='http://localhost:%s/' % mock_ckan.PORT,
            harvester=CKANHarvester())

        results_by_guid = run_harvest(
            url='http://localhost:%s/' % mock_ckan.PORT,
            harvester=CKANHarvester())

        # The metadata_modified was the same for this dataset so the import
        # would have returned 'unchanged'
        result = results_by_guid[mock_ckan.DATASETS[1]['id']]
        assert_equal(result['state'], 'COMPLETE')
        assert_equal(result['report_status'], 'not modified')
        assert 'dataset' not in result
        assert_equal(result['errors'], [])
        assert was_last_job_considered_error_free()

    def test_harvest_whilst_datasets_added(self):
        results_by_guid = run_harvest(
            url='http://localhost:%s/datasets_added' % mock_ckan.PORT,
            harvester=CKANHarvester())

        assert_equal(sorted(results_by_guid.keys()),
                     [mock_ckan.DATASETS[1]['id'],
                      mock_ckan.DATASETS[0]['id']])

    def test_harvest_site_down(self):
        results_by_guid = run_harvest(
            url='http://localhost:%s/site_down' % mock_ckan.PORT,
            harvester=CKANHarvester())
        assert not results_by_guid
        assert not was_last_job_considered_error_free()

    def test_default_tags(self):
        config = {'default_tags': [{'name': 'geo'}]}
        results_by_guid = run_harvest(
            url='http://localhost:%s' % mock_ckan.PORT,
            harvester=CKANHarvester(),
            config=json.dumps(config))
        tags = results_by_guid['dataset1-id']['dataset']['tags']
        tag_names = [tag['name'] for tag in tags]
        assert 'geo' in tag_names

    def test_default_tags_invalid(self):
        config = {'default_tags': ['geo']}  # should be list of dicts
        with assert_raises(toolkit.ValidationError) as harvest_context:
            run_harvest(
                url='http://localhost:%s' % mock_ckan.PORT,
                harvester=CKANHarvester(),
                config=json.dumps(config))
        assert_in('default_tags must be a list of dictionaries',
                  str(harvest_context.exception))

    def test_default_groups(self):
        Group(id='group1-id', name='group1')
        Group(id='group2-id', name='group2')
        Group(id='group3-id', name='group3')

        config = {'default_groups': ['group2-id', 'group3'],
                  'remote_groups': 'only_local'}
        tmp_c = toolkit.c
        try:
            # c.user is used by the validation (annoying),
            # however patch doesn't work because it's a weird
            # StackedObjectProxy, so we swap it manually
            toolkit.c = MagicMock(user='')
            results_by_guid = run_harvest(
                url='http://localhost:%s' % mock_ckan.PORT,
                harvester=CKANHarvester(),
                config=json.dumps(config))
        finally:
            toolkit.c = tmp_c
        assert_equal(results_by_guid['dataset1-id']['errors'], [])
        groups = results_by_guid['dataset1-id']['dataset']['groups']
        group_names = set(group['name'] for group in groups)
        # group1 comes from the harvested dataset
        # group2 & 3 come from the default_groups
        assert_equal(group_names, set(('group1', 'group2', 'group3')))

    def test_default_groups_invalid(self):
        Group(id='group2-id', name='group2')

        # should be list of strings
        config = {'default_groups': [{'name': 'group2'}]}
        with assert_raises(toolkit.ValidationError) as harvest_context:
            run_harvest(
                url='http://localhost:%s' % mock_ckan.PORT,
                harvester=CKANHarvester(),
                config=json.dumps(config))
        assert_in('default_groups must be a list of group names/ids',
                  str(harvest_context.exception))

    def test_default_extras(self):
        config = {
            'default_extras': {
                'encoding': 'utf8',
                'harvest_url': '{harvest_source_url}/dataset/{dataset_id}'
                }}
        tmp_c = toolkit.c
        try:
            # c.user is used by the validation (annoying),
            # however patch doesn't work because it's a weird
            # StackedObjectProxy, so we swap it manually
            toolkit.c = MagicMock(user='')
            results_by_guid = run_harvest(
                url='http://localhost:%s' % mock_ckan.PORT,
                harvester=CKANHarvester(),
                config=json.dumps(config))
        finally:
            toolkit.c = tmp_c
        assert_equal(results_by_guid['dataset1-id']['errors'], [])
        extras = results_by_guid['dataset1-id']['dataset']['extras']
        extras_dict = dict((e['key'], e['value']) for e in extras)
        assert_equal(extras_dict['encoding'], 'utf8')
        assert_equal(extras_dict['harvest_url'],
                     'http://localhost:8998/dataset/dataset1-id')

    def test_default_extras_invalid(self):
        config = {
            'default_extras': 'utf8',  # value should be a dict
            }
        with assert_raises(toolkit.ValidationError) as harvest_context:
            run_harvest(
                url='http://localhost:%s' % mock_ckan.PORT,
                harvester=CKANHarvester(),
                config=json.dumps(config))
        assert_in('default_extras must be a dictionary',
                  str(harvest_context.exception))
