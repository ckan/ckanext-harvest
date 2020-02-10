import json

import pytest

from ckan import plugins as p
<<<<<<< HEAD
from ckan import model
=======

from ckantoolkit.tests import factories as ckan_factories, helpers
from ckanext.harvest.tests import factories
>>>>>>> 4ee8fa2a5df10b8ea583618e2e89076ef7f7c1b0

from ckantoolkit import ValidationError, get_action
from ckantoolkit.tests import factories as ckan_factories, helpers
from ckanext.harvest.tests import factories

from ckanext.harvest.interfaces import IHarvester
import ckanext.harvest.model as harvest_model


class MockHarvesterForActionTests(p.SingletonPlugin):
    p.implements(IHarvester)

    def info(self):
        return {'name': 'test-for-action',
                'title': 'Test for action',
                'description': 'test'}

    def validate_config(self, config):
        if not config:
            return config

        try:
            config_obj = json.loads(config)

            if 'custom_option' in config_obj:
                if not isinstance(config_obj['custom_option'], list):
                    raise ValueError('custom_option must be a list')

        except ValueError as e:
            raise e

        return config

    def gather_stage(self, harvest_job):
        return []

    def fetch_stage(self, harvest_object):
        return True

    def import_stage(self, harvest_object):
        return True


SOURCE_DICT = {
    "url": "http://test.action.com",
    "name": "test-source-action",
    "title": "Test source action",
    "notes": "Test source action desc",
    "source_type": "test-for-action",
    "frequency": "MANUAL",
    "config": json.dumps({"custom_option": ["a", "b"]})
}


@pytest.mark.usefixtures('with_plugins', 'clean_db', 'harvest_setup', 'clean_queues')
@pytest.mark.ckan_config('ckan.plugins', 'harvest test_action_harvester')
<<<<<<< HEAD
class HarvestSourceActionBase(object):
=======
class HarvestSourceActionBase():
>>>>>>> 4ee8fa2a5df10b8ea583618e2e89076ef7f7c1b0

    def _get_source_dict(self):
        return {
            "url": "http://test.action.com",
            "name": "test-source-action",
            "title": "Test source action",
            "notes": "Test source action desc",
            "source_type": "test-for-action",
            "frequency": "MANUAL",
            "config": json.dumps({"custom_option": ["a", "b"]})
        }

    def test_invalid_missing_values(self):
        source_dict = {}
        test_data = self._get_source_dict()
        if 'id' in test_data:
            source_dict['id'] = test_data['id']

<<<<<<< HEAD
        with pytest.raises(ValidationError) as e:
            helpers.call_action(self.action, **source_dict)

        for key in ('name', 'title', 'url', 'source_type'):
            assert e.value.error_dict[key] == [u'Missing value']
=======

        result = helpers.call_action(self.action,
                                 **source_dict)

        for key in ('name', 'title', 'url', 'source_type'):
            assert result[key] == [u'Missing value']
>>>>>>> 4ee8fa2a5df10b8ea583618e2e89076ef7f7c1b0

    def test_invalid_unknown_type(self):
        source_dict = self._get_source_dict()
        source_dict['source_type'] = 'unknown'

<<<<<<< HEAD
        with pytest.raises(ValidationError) as e:
            helpers.call_action(self.action, **source_dict)
=======

        result = helpers.call_action(self.action,
                                 **source_dict)
>>>>>>> 4ee8fa2a5df10b8ea583618e2e89076ef7f7c1b0

        assert u'Unknown harvester type' in e.value.error_dict['source_type'][0]

    def test_invalid_unknown_frequency(self):
        wrong_frequency = 'ANNUALLY'
        source_dict = self._get_source_dict()
        source_dict['frequency'] = wrong_frequency

<<<<<<< HEAD
        with pytest.raises(ValidationError) as e:
            helpers.call_action(self.action, **source_dict)
=======

        result = helpers.call_action(self.action,
                                 **source_dict)
>>>>>>> 4ee8fa2a5df10b8ea583618e2e89076ef7f7c1b0

        assert u'Frequency {0} not recognised'.format(wrong_frequency) in e.value.error_dict['frequency'][0]

    def test_invalid_wrong_configuration(self):
        source_dict = self._get_source_dict()
        source_dict['config'] = 'not_json'

<<<<<<< HEAD
        with pytest.raises(ValidationError) as e:
            helpers.call_action(self.action, **source_dict)
=======

        result = helpers.call_action(self.action,
                                 **source_dict)
>>>>>>> 4ee8fa2a5df10b8ea583618e2e89076ef7f7c1b0

        assert u'Error parsing the configuration options: No JSON object could be decoded' in e.value.error_dict['config'][0]

        source_dict['config'] = json.dumps({'custom_option': 'not_a_list'})

<<<<<<< HEAD
        with pytest.raises(ValidationError) as e:
            helpers.call_action(self.action, **source_dict)
=======
        result = helpers.call_action(self.action,
                                 **source_dict)
>>>>>>> 4ee8fa2a5df10b8ea583618e2e89076ef7f7c1b0

        assert u'Error parsing the configuration options: custom_option must be a list' in e.value.error_dict['config'][0]


class TestHarvestSourceActionCreate(HarvestSourceActionBase):

    action = 'harvest_source_create'

    def test_create(self):

        source_dict = self._get_source_dict()

<<<<<<< HEAD
        result = helpers.call_action(
            'harvest_source_create', **source_dict)
=======
        result = helpers.call_action('harvest_source_create',
                                 **source_dict)
>>>>>>> 4ee8fa2a5df10b8ea583618e2e89076ef7f7c1b0

        for key in source_dict.keys():
            assert source_dict[key] == result[key]

        # Check that source was actually created
        source = harvest_model.HarvestSource.get(result['id'])
        assert source.url == source_dict['url']
        assert source.type == source_dict['source_type']

        # Trying to create a source with the same URL fails
        source_dict = self._get_source_dict()
        source_dict['name'] = 'test-source-action-new'

<<<<<<< HEAD
        with pytest.raises(ValidationError) as e:
            result = helpers.call_action(
                'harvest_source_create', **source_dict)
=======
        result = helpers.call_action('harvest_source_create',
                                 **source_dict)
>>>>>>> 4ee8fa2a5df10b8ea583618e2e89076ef7f7c1b0

        assert u'There already is a Harvest Source for this URL' in e.value.error_dict['url'][0]


<<<<<<< HEAD
class HarvestSourceFixtureMixin(object):
    def _get_source_dict(self):
        '''Not only returns a source_dict, but creates the HarvestSource object
        as well - suitable for testing update actions.
        '''
        source = HarvestSourceActionBase._get_source_dict(self)
        source = factories.HarvestSource(**source)
        # delete status because it gets in the way of the status supplied to
        # helpers.call_action later on. It is only a generated value, not affecting
        # the update/patch anyway.
        del source['status']
        return source


class TestHarvestSourceActionUpdate(HarvestSourceFixtureMixin,
                                    HarvestSourceActionBase):

    action = 'harvest_source_update'

    def test_update(self):
        source_dict = self._get_source_dict()
        source_dict.update({
            "url": "http://test.action.updated.com",
            "name": "test-source-action-updated",
            "title": "Test source action updated",
            "notes": "Test source action desc updated",
            "source_type": "test",
            "frequency": "MONTHLY",
            "config": json.dumps({"custom_option": ["c", "d"]})
        })

        result = helpers.call_action(
            'harvest_source_update', **source_dict)

        for key in set(('url', 'name', 'title', 'notes', 'source_type',
                        'frequency', 'config')):
            assert source_dict[key], result[key] == "Key: %s" % key

        # Check that source was actually updated
        source = harvest_model.HarvestSource.get(result['id'])
        assert source.url == source_dict['url']
        assert source.type == source_dict['source_type']


class TestHarvestSourceActionPatch(HarvestSourceFixtureMixin,
                                   HarvestSourceActionBase):

    action = 'harvest_source_patch'

    def test_invalid_missing_values(self):
        pass

    def test_patch(self):
        source_dict = self._get_source_dict()

        patch_dict = {
            "id": source_dict['id'],
            "name": "test-source-action-patched",
            "url": "http://test.action.patched.com",
            "config": json.dumps({"custom_option": ["pat", "ched"]})
        }

        result = helpers.call_action(
            'harvest_source_patch', **patch_dict)

        source_dict.update(patch_dict)
        for key in set(('url', 'name', 'title', 'notes', 'source_type',
                        'frequency', 'config')):
            assert source_dict[key], result[key] == "Key: %s" % key

        # Check that source was actually updated
        source = harvest_model.HarvestSource.get(result['id'])
        assert source.url == source_dict['url']
        assert source.type == source_dict['source_type']


@pytest.mark.usefixtures('with_plugins', 'clean_db', 'harvest_setup', 'clean_queues')
@pytest.mark.ckan_config('ckan.plugins', 'harvest test_action_harvester')
class TestActions():
    def test_harvest_source_clear(self):
        source = factories.HarvestSourceObj(**SOURCE_DICT.copy())
        job = factories.HarvestJobObj(source=source)
        dataset = ckan_factories.Dataset()
        object_ = factories.HarvestObjectObj(
            job=job, source=source, package_id=dataset['id'])

        context = {
            'ignore_auth': True,
            'user': ''
        }
        result = get_action('harvest_source_clear')(
            context, {'id': source.id})

        assert result == {'id': source.id}
        source = harvest_model.HarvestSource.get(source.id)
        assert source
        assert harvest_model.HarvestJob.get(job.id) is None
        assert harvest_model.HarvestObject.get(object_.id) is None
        assert model.Package.get(dataset['id']) is None

    def test_harvest_source_job_history_clear(self):
        # prepare
        source = factories.HarvestSourceObj(**SOURCE_DICT.copy())
        job = factories.HarvestJobObj(source=source)
        dataset = ckan_factories.Dataset()
        object_ = factories.HarvestObjectObj(job=job, source=source,
                                             package_id=dataset['id'])

        # execute
        context = {'session': model.Session,
                   'ignore_auth': True, 'user': ''}
        result = get_action('harvest_source_job_history_clear')(
            context, {'id': source.id})

        # verify
        assert result == {'id': source.id}
        source = harvest_model.HarvestSource.get(source.id)
        assert source
        assert harvest_model.HarvestJob.get(job.id) is None
        assert harvest_model.HarvestObject.get(object_.id) is None
        dataset_from_db = model.Package.get(dataset['id'])
        assert dataset_from_db, 'is None'
        assert dataset_from_db.id == dataset['id']

    def test_harvest_sources_job_history_clear(self):
        # prepare
        data_dict = SOURCE_DICT.copy()
        source_1 = factories.HarvestSourceObj(**data_dict)
        data_dict['name'] = 'another-source'
        data_dict['url'] = 'http://another-url'
        source_2 = factories.HarvestSourceObj(**data_dict)

        job_1 = factories.HarvestJobObj(source=source_1)
        dataset_1 = ckan_factories.Dataset()
        object_1_ = factories.HarvestObjectObj(job=job_1, source=source_1,
                                               package_id=dataset_1['id'])
        job_2 = factories.HarvestJobObj(source=source_2)
        dataset_2 = ckan_factories.Dataset()
        object_2_ = factories.HarvestObjectObj(job=job_2, source=source_2,
                                               package_id=dataset_2['id'])

        # execute
        context = {'session': model.Session,
                   'ignore_auth': True, 'user': ''}
        result = get_action('harvest_sources_job_history_clear')(
            context, {})

        # verify
        assert sorted(result) == sorted([{'id': source_1.id}, {'id': source_2.id}])
        source_1 = harvest_model.HarvestSource.get(source_1.id)
        assert source_1
        assert harvest_model.HarvestJob.get(job_1.id) is None
        assert harvest_model.HarvestObject.get(object_1_.id) is None
        dataset_from_db_1 = model.Package.get(dataset_1['id'])
        assert dataset_from_db_1, 'is None'
        assert dataset_from_db_1.id == dataset_1['id']
        source_2 = harvest_model.HarvestSource.get(source_1.id)
        assert source_2
        assert harvest_model.HarvestJob.get(job_2.id) is None
        assert harvest_model.HarvestObject.get(object_2_.id) is None
        dataset_from_db_2 = model.Package.get(dataset_2['id'])
        assert dataset_from_db_2, 'is None'
        assert dataset_from_db_2.id == dataset_2['id']

    def test_harvest_source_create_twice_with_unique_url(self):
        data_dict = SOURCE_DICT.copy()
        factories.HarvestSourceObj(**data_dict)
        site_user = get_action('get_site_user')(
            {'ignore_auth': True}, {})['name']
        data_dict['name'] = 'another-source'
        data_dict['url'] = 'http://another-url'
        get_action('harvest_source_create')(
            {'user': site_user}, data_dict)

    def test_harvest_source_create_twice_with_same_url(self):
        data_dict = SOURCE_DICT.copy()
        factories.HarvestSourceObj(**data_dict)

        site_user = get_action('get_site_user')(
            {'ignore_auth': True}, {})['name']
        data_dict['name'] = 'another-source'
        with pytest.raises(ValidationError):
            get_action('harvest_source_create')(
                {'user': site_user}, data_dict)

    def test_harvest_source_create_twice_with_unique_url_and_config(self):
        data_dict = SOURCE_DICT.copy()
        factories.HarvestSourceObj(**data_dict)

        site_user = get_action('get_site_user')(
            {'ignore_auth': True}, {})['name']
        data_dict['name'] = 'another-source'
        data_dict['config'] = '{"something": "new"}'
        get_action('harvest_source_create')(
            {'user': site_user}, data_dict)

    def test_harvest_job_create_as_sysadmin(self):
        source = factories.HarvestSource(**SOURCE_DICT.copy())

        site_user = get_action('get_site_user')(
            {'ignore_auth': True}, {})['name']
        data_dict = {
            'source_id': source['id'],
            'run': True
        }
        job = get_action('harvest_job_create')(
            {'user': site_user}, data_dict)

        assert job['source_id'] == source['id']
        assert job['status'] == 'Running'
        assert job['gather_started'] is None
        assert 'stats' in job.keys()

    def test_harvest_job_create_as_admin(self):
        # as if an admin user presses 'refresh'
        user = ckan_factories.User()
        user['capacity'] = 'admin'
        org = ckan_factories.Organization(users=[user])
        source_dict = dict(
            SOURCE_DICT.items() + [('publisher_id', org['id'])]
        )
        source = factories.HarvestSource(**source_dict)

        data_dict = {
            'source_id': source['id'],
            'run': True
        }
        job = get_action('harvest_job_create')(
            {'user': user['name']}, data_dict)

        assert job['source_id'] == source['id']
        assert job['status'] == 'Running'
        assert job['gather_started'] is None
        assert 'stats' in job.keys()
=======
>>>>>>> 4ee8fa2a5df10b8ea583618e2e89076ef7f7c1b0
