import json
import factories
import unittest
from nose.tools import assert_equal, assert_raises
from nose.plugins.skip import SkipTest

try:
    from ckan.tests import factories as ckan_factories
    from ckan.tests.helpers import (_get_test_app, reset_db,
                                    FunctionalTestBase, assert_in)
except ImportError:
    from ckan.new_tests import factories as ckan_factories
    from ckan.new_tests.helpers import (_get_test_app, reset_db,
                                        FunctionalTestBase)
    try:
        from ckan.new_tests.helpers import assert_in
    except ImportError:
        # for ckan 2.2
        try:
            from nose.tools import assert_in
        except ImportError:
            # Python 2.6 doesn't have it
            def assert_in(a, b, msg=None):
                assert a in b, msg or '%r was not in %r' % (a, b)

from ckan import plugins as p
from ckan.plugins import toolkit
from ckan import model

from ckanext.harvest.interfaces import IHarvester
import ckanext.harvest.model as harvest_model


def call_action_api(action, apikey=None, status=200, **kwargs):
    '''POST an HTTP request to the CKAN API and return the result.

    Any additional keyword arguments that you pass to this function as **kwargs
    are posted as params to the API.

    Usage:

        package_dict = call_action_api('package_create', apikey=apikey,
                name='my_package')
        assert package_dict['name'] == 'my_package'

        num_followers = post(app, 'user_follower_count', id='annafan')

    If you are expecting an error from the API and want to check the contents
    of the error dict, you have to use the status param otherwise an exception
    will be raised:

        error_dict = call_action_api('group_activity_list', status=403,
                id='invalid_id')
        assert error_dict['message'] == 'Access Denied'

    :param action: the action to post to, e.g. 'package_create'
    :type action: string

    :param apikey: the API key to put in the Authorization header of the post
        (optional, default: None)
    :type apikey: string

    :param status: the HTTP status code expected in the response from the CKAN
        API, e.g. 403, if a different status code is received an exception will
        be raised (optional, default: 200)
    :type status: int

    :param **kwargs: any other keyword arguments passed to this function will
        be posted to the API as params

    :raises paste.fixture.AppError: if the HTTP status code of the response
        from the CKAN API is different from the status param passed to this
        function

    :returns: the 'result' or 'error' dictionary from the CKAN API response
    :rtype: dictionary

    '''
    params = json.dumps(kwargs)
    app = _get_test_app()
    response = app.post('/api/action/{0}'.format(action), params=params,
                        extra_environ={'Authorization': str(apikey)},
                        status=status)

    if status in (200,):
        assert response.json['success'] is True
        return response.json['result']
    else:
        assert response.json['success'] is False
        return response.json['error']


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

        except ValueError, e:
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


class ActionBase(object):
    @classmethod
    def setup_class(cls):
        if not p.plugin_loaded('test_action_harvester'):
            p.load('test_action_harvester')

    def setup(self):
        reset_db()
        harvest_model.setup()

    @classmethod
    def teardown_class(cls):
        p.unload('test_action_harvester')


class HarvestSourceActionBase(FunctionalTestBase):

    @classmethod
    def setup_class(cls):
        super(HarvestSourceActionBase, cls).setup_class()
        harvest_model.setup()

        if not p.plugin_loaded('test_action_harvester'):
            p.load('test_action_harvester')

    @classmethod
    def teardown_class(cls):
        super(HarvestSourceActionBase, cls).teardown_class()

        p.unload('test_action_harvester')

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

        sysadmin = ckan_factories.Sysadmin()
        result = call_action_api(self.action,
                                 apikey=sysadmin['apikey'], status=409,
                                 **source_dict)

        for key in ('name', 'title', 'url', 'source_type'):
            assert_equal(result[key], [u'Missing value'])

    def test_invalid_unknown_type(self):
        source_dict = self._get_source_dict()
        source_dict['source_type'] = 'unknown'

        sysadmin = ckan_factories.Sysadmin()
        result = call_action_api(self.action,
                                 apikey=sysadmin['apikey'], status=409,
                                 **source_dict)

        assert 'source_type' in result
        assert u'Unknown harvester type' in result['source_type'][0]

    def test_invalid_unknown_frequency(self):
        wrong_frequency = 'ANNUALLY'
        source_dict = self._get_source_dict()
        source_dict['frequency'] = wrong_frequency

        sysadmin = ckan_factories.Sysadmin()
        result = call_action_api(self.action,
                                 apikey=sysadmin['apikey'], status=409,
                                 **source_dict)

        assert 'frequency' in result
        assert u'Frequency {0} not recognised'.format(wrong_frequency) in result['frequency'][0]

    def test_invalid_wrong_configuration(self):
        source_dict = self._get_source_dict()
        source_dict['config'] = 'not_json'

        sysadmin = ckan_factories.Sysadmin()
        result = call_action_api(self.action,
                                 apikey=sysadmin['apikey'], status=409,
                                 **source_dict)

        assert 'config' in result
        assert u'Error parsing the configuration options: No JSON object could be decoded' in result['config'][0]

        source_dict['config'] = json.dumps({'custom_option': 'not_a_list'})

        result = call_action_api(self.action,
                                 apikey=sysadmin['apikey'], status=409,
                                 **source_dict)

        assert 'config' in result
        assert u'Error parsing the configuration options: custom_option must be a list' in result['config'][0]


class TestHarvestSourceActionCreate(HarvestSourceActionBase):

    def __init__(self):
        self.action = 'harvest_source_create'

    def test_create(self):

        source_dict = self._get_source_dict()

        sysadmin = ckan_factories.Sysadmin()
        result = call_action_api('harvest_source_create',
                                 apikey=sysadmin['apikey'], **source_dict)

        for key in source_dict.keys():
            assert_equal(source_dict[key], result[key])
            
        # Check that source was actually created
        source = harvest_model.HarvestSource.get(result['id'])
        assert_equal(source.url, source_dict['url'])
        assert_equal(source.type, source_dict['source_type'])

        # Trying to create a source with the same URL fails
        source_dict = self._get_source_dict()
        source_dict['name'] = 'test-source-action-new'

        result = call_action_api('harvest_source_create',
                                 apikey=sysadmin['apikey'], status=409,
                                 **source_dict)

        assert 'url' in result
        assert u'There already is a Harvest Source for this URL' in result['url'][0]


class HarvestSourceFixtureMixin(object):
    def _get_source_dict(self):
        '''Not only returns a source_dict, but creates the HarvestSource object
        as well - suitable for testing update actions.
        '''
        source = HarvestSourceActionBase._get_source_dict(self)
        source = factories.HarvestSource(**source)
        # delete status because it gets in the way of the status supplied to
        # call_action_api later on. It is only a generated value, not affecting
        # the update/patch anyway.
        del source['status']
        return source


class TestHarvestSourceActionUpdate(HarvestSourceFixtureMixin,
                                    HarvestSourceActionBase):
    def __init__(self):
        self.action = 'harvest_source_update'

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

        sysadmin = ckan_factories.Sysadmin()
        result = call_action_api('harvest_source_update',
                                 apikey=sysadmin['apikey'], **source_dict)

        for key in set(('url', 'name', 'title', 'notes', 'source_type',
                        'frequency', 'config')):
            assert_equal(source_dict[key], result[key], "Key: %s" % key)

        # Check that source was actually updated
        source = harvest_model.HarvestSource.get(result['id'])
        assert_equal(source.url, source_dict['url'])
        assert_equal(source.type, source_dict['source_type'])


class TestHarvestSourceActionPatch(HarvestSourceFixtureMixin,
                                   HarvestSourceActionBase):
    def __init__(self):
        self.action = 'harvest_source_patch'
        if toolkit.check_ckan_version(max_version='2.2.99'):
            # harvest_source_patch only came in with ckan 2.3
            raise SkipTest()

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

        sysadmin = ckan_factories.Sysadmin()
        result = call_action_api('harvest_source_patch',
                                 apikey=sysadmin['apikey'], **patch_dict)

        source_dict.update(patch_dict)
        for key in set(('url', 'name', 'title', 'notes', 'source_type',
                        'frequency', 'config')):
            assert_equal(source_dict[key], result[key], "Key: %s" % key)

        # Check that source was actually updated
        source = harvest_model.HarvestSource.get(result['id'])
        assert_equal(source.url, source_dict['url'])
        assert_equal(source.type, source_dict['source_type'])


class TestActions(ActionBase):
    def test_harvest_source_clear(self):
        source = factories.HarvestSourceObj(**SOURCE_DICT.copy())
        job = factories.HarvestJobObj(source=source)
        dataset = ckan_factories.Dataset()
        object_ = factories.HarvestObjectObj(job=job, source=source,
                                             package_id=dataset['id'])

        context = {'model': model, 'session': model.Session,
                   'ignore_auth': True, 'user': ''}
        result = toolkit.get_action('harvest_source_clear')(
            context, {'id': source.id})

        assert_equal(result, {'id': source.id})
        source = harvest_model.HarvestSource.get(source.id)
        assert source
        assert_equal(harvest_model.HarvestJob.get(job.id), None)
        assert_equal(harvest_model.HarvestObject.get(object_.id), None)
        assert_equal(model.Package.get(dataset['id']), None)

    def test_harvest_source_job_history_clear(self):
        # prepare
        source = factories.HarvestSourceObj(**SOURCE_DICT.copy())
        job = factories.HarvestJobObj(source=source)
        dataset = ckan_factories.Dataset()
        object_ = factories.HarvestObjectObj(job=job, source=source,
                                             package_id=dataset['id'])

        # execute
        context = {'model': model, 'session': model.Session,
                   'ignore_auth': True, 'user': ''}
        result = toolkit.get_action('harvest_source_job_history_clear')(
            context, {'id': source.id})

        # verify
        assert_equal(result, {'id': source.id})
        source = harvest_model.HarvestSource.get(source.id)
        assert source
        assert_equal(harvest_model.HarvestJob.get(job.id), None)
        assert_equal(harvest_model.HarvestObject.get(object_.id), None)
        dataset_from_db = model.Package.get(dataset['id'])
        assert dataset_from_db, 'is None'
        assert_equal(dataset_from_db.id, dataset['id'])

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
        context = {'model': model, 'session': model.Session,
                   'ignore_auth': True, 'user': ''}
        result = toolkit.get_action('harvest_sources_job_history_clear')(
            context, {})

        # verify
        assert_equal(
            sorted(result),
            sorted([{'id': source_1.id}, {'id': source_2.id}]))
        source_1 = harvest_model.HarvestSource.get(source_1.id)
        assert source_1
        assert_equal(harvest_model.HarvestJob.get(job_1.id), None)
        assert_equal(harvest_model.HarvestObject.get(object_1_.id), None)
        dataset_from_db_1 = model.Package.get(dataset_1['id'])
        assert dataset_from_db_1, 'is None'
        assert_equal(dataset_from_db_1.id, dataset_1['id'])
        source_2 = harvest_model.HarvestSource.get(source_1.id)
        assert source_2
        assert_equal(harvest_model.HarvestJob.get(job_2.id), None)
        assert_equal(harvest_model.HarvestObject.get(object_2_.id), None)
        dataset_from_db_2 = model.Package.get(dataset_2['id'])
        assert dataset_from_db_2, 'is None'
        assert_equal(dataset_from_db_2.id, dataset_2['id'])

    def test_harvest_source_create_twice_with_unique_url(self):
        data_dict = SOURCE_DICT.copy()
        factories.HarvestSourceObj(**data_dict)
        site_user = toolkit.get_action('get_site_user')(
            {'model': model, 'ignore_auth': True}, {})['name']
        data_dict['name'] = 'another-source'
        data_dict['url'] = 'http://another-url'
        toolkit.get_action('harvest_source_create')(
            {'user': site_user}, data_dict)

    def test_harvest_source_create_twice_with_same_url(self):
        data_dict = SOURCE_DICT.copy()
        factories.HarvestSourceObj(**data_dict)

        site_user = toolkit.get_action('get_site_user')(
            {'model': model, 'ignore_auth': True}, {})['name']
        data_dict['name'] = 'another-source'
        assert_raises(toolkit.ValidationError,
                      toolkit.get_action('harvest_source_create'),
                      {'user': site_user}, data_dict)

    def test_harvest_source_create_twice_with_unique_url_and_config(self):
        data_dict = SOURCE_DICT.copy()
        factories.HarvestSourceObj(**data_dict)

        site_user = toolkit.get_action('get_site_user')(
            {'model': model, 'ignore_auth': True}, {})['name']
        data_dict['name'] = 'another-source'
        data_dict['config'] = '{"something": "new"}'
        toolkit.get_action('harvest_source_create')(
            {'user': site_user}, data_dict)

    def test_harvest_job_create_as_sysadmin(self):
        source = factories.HarvestSource(**SOURCE_DICT.copy())

        site_user = toolkit.get_action('get_site_user')(
            {'model': model, 'ignore_auth': True}, {})['name']
        data_dict = {
            'source_id': source['id'],
            'run': True
            }
        job = toolkit.get_action('harvest_job_create')(
            {'user': site_user}, data_dict)

        assert_equal(job['source_id'], source['id'])
        assert_equal(job['status'], 'Running')
        assert_equal(job['gather_started'], None)
        assert_in('stats', job.keys())

    def test_harvest_job_create_as_admin(self):
        # as if an admin user presses 'refresh'
        user = ckan_factories.User()
        user['capacity'] = 'admin'
        org = ckan_factories.Organization(users=[user])
        source_dict = dict(SOURCE_DICT.items() +
                           [('publisher_id', org['id'])])
        source = factories.HarvestSource(**source_dict)

        data_dict = {
            'source_id': source['id'],
            'run': True
            }
        job = toolkit.get_action('harvest_job_create')(
            {'user': user['name']}, data_dict)

        assert_equal(job['source_id'], source['id'])
        assert_equal(job['status'], 'Running')
        assert_equal(job['gather_started'], None)
        assert_in('stats', job.keys())


class TestHarvestObject(unittest.TestCase):
    @classmethod
    def setup_class(cls):
        reset_db()
        harvest_model.setup()

    def test_create(self):
        job = factories.HarvestJobObj()

        context = {
            'model': model,
            'session': model.Session,
            'ignore_auth': True,
        }
        data_dict = {
            'guid': 'guid',
            'content': 'content',
            'job_id': job.id,
            'extras': {'a key': 'a value'},
        }
        harvest_object = toolkit.get_action('harvest_object_create')(
            context, data_dict)

        # fetch the object from database to check it was created
        created_object = harvest_model.HarvestObject.get(harvest_object['id'])
        assert created_object.guid == harvest_object['guid'] == data_dict['guid']

    def test_create_bad_parameters(self):
        source_a = factories.HarvestSourceObj()
        job = factories.HarvestJobObj()

        context = {
            'model': model,
            'session': model.Session,
            'ignore_auth': True,
        }
        data_dict = {
            'job_id': job.id,
            'source_id': source_a.id,
            'extras': 1
        }
        harvest_object_create = toolkit.get_action('harvest_object_create')
        self.assertRaises(toolkit.ValidationError, harvest_object_create,
                          context, data_dict)

        data_dict['extras'] = {'test': 1}

        self.assertRaises(toolkit.ValidationError, harvest_object_create,
                          context, data_dict)

          
class TestHarvestDBLog(unittest.TestCase):
    @classmethod
    def setup_class(cls):
        reset_db()
        harvest_model.setup()
        
    def test_harvest_db_logger(self):
        # Create source and check if harvest_log table is populated
        data_dict = SOURCE_DICT.copy()
        data_dict['source_type'] = 'test'
        source = factories.HarvestSourceObj(**data_dict)
        content = 'Harvest source created: %s' % source.id
        log = harvest_model.Session.query(harvest_model.HarvestLog).\
                filter(harvest_model.HarvestLog.content==content).first()
                
        self.assertIsNotNone(log)
        self.assertEqual(log.level, 'INFO')
        
        context = {
            'model': model,
            'session': model.Session,
            'ignore_auth': True,
        }

        data = toolkit.get_action('harvest_log_list')(context, {})
        self.assertTrue(len(data) > 0)
        self.assertIn('level', data[0])
        self.assertIn('content', data[0])
        self.assertIn('created', data[0])
        self.assertTrue(data[0]['created'] > data[1]['created'])
        
        per_page = 1
        data = toolkit.get_action('harvest_log_list')(context, {'level': 'info', 'per_page': per_page})
        self.assertEqual(len(data), per_page)
        self.assertEqual(data[0]['level'], 'INFO')
