import json
import copy
import ckan
import paste
import pylons.test
import factories
import unittest

from ckan.lib.create_test_data import CreateTestData
import ckan.new_tests.helpers as helpers
from ckan import plugins as p
from ckan.plugins import toolkit
from ckanext.harvest.interfaces import IHarvester
import ckanext.harvest.model as harvest_model

def call_action_api(app, action, apikey=None, status=200, **kwargs):
    '''POST an HTTP request to the CKAN API and return the result.

    Any additional keyword arguments that you pass to this function as **kwargs
    are posted as params to the API.

    Usage:

        package_dict = post(app, 'package_create', apikey=apikey,
                name='my_package')
        assert package_dict['name'] == 'my_package'

        num_followers = post(app, 'user_follower_count', id='annafan')

    If you are expecting an error from the API and want to check the contents
    of the error dict, you have to use the status param otherwise an exception
    will be raised:

        error_dict = post(app, 'group_activity_list', status=403,
                id='invalid_id')
        assert error_dict['message'] == 'Access Denied'

    :param app: the test app to post to
    :type app: paste.fixture.TestApp

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
    response = app.post('/api/action/{0}'.format(action), params=params,
            extra_environ={'Authorization': str(apikey)}, status=status)

    if status in (200,):
        assert response.json['success'] is True
        return response.json['result']
    else:
        assert response.json['success'] is False
        return response.json['error']


class MockHarvesterForActionTests(p.SingletonPlugin):
    p.implements(IHarvester)
    def info(self):
        return {'name': 'test-for-action', 'title': 'Test for action', 'description': 'test'}

    def validate_config(self,config):
        if not config:
            return config

        try:
            config_obj = json.loads(config)

            if 'custom_option' in config_obj:
                if not isinstance(config_obj['custom_option'],list):
                    raise ValueError('custom_option must be a list')

        except ValueError,e:
            raise e

        return config

    def gather_stage(self, harvest_job):
        return []

    def fetch_stage(self, harvest_object):
        return True

    def import_stage(self, harvest_object):
        return True

class HarvestSourceActionBase(object):

    @classmethod
    def setup_class(cls):
        harvest_model.setup()
        CreateTestData.create()

        sysadmin_user = ckan.model.User.get('testsysadmin')
        cls.sysadmin = {
                'id': sysadmin_user.id,
                'apikey': sysadmin_user.apikey,
                'name': sysadmin_user.name,
                }


        cls.app = paste.fixture.TestApp(pylons.test.pylonsapp)

        cls.default_source_dict =  {
          "url": "http://test.action.com",
          "name": "test-source-action",
          "title": "Test source action",
          "notes": "Test source action desc",
          "source_type": "test-for-action",
          "frequency": "MANUAL",
          "config": json.dumps({"custom_option":["a","b"]})
        }

        if not p.plugin_loaded('test_action_harvester'):
            p.load('test_action_harvester')


    @classmethod
    def teardown_class(cls):
        ckan.model.repo.rebuild_db()

        p.unload('test_action_harvester')

    def teardown(self):
        pass

    def test_invalid_missing_values(self):

        source_dict = {}
        if 'id' in self.default_source_dict:
            source_dict['id'] = self.default_source_dict['id']

        result = call_action_api(self.app, self.action,
                                apikey=self.sysadmin['apikey'], status=409, **source_dict)

        for key in ('name','title','url','source_type'):
            assert result[key] == [u'Missing value']

    def test_invalid_unknown_type(self):

        source_dict = copy.deepcopy(self.default_source_dict)
        source_dict['source_type'] = 'unknown'

        result = call_action_api(self.app, self.action,
                                apikey=self.sysadmin['apikey'], status=409, **source_dict)

        assert 'source_type' in result
        assert u'Unknown harvester type' in result['source_type'][0]

    def test_invalid_unknown_frequency(self):
        wrong_frequency = 'ANNUALLY'
        source_dict = copy.deepcopy(self.default_source_dict)
        source_dict['frequency'] = wrong_frequency

        result = call_action_api(self.app, self.action,
                                apikey=self.sysadmin['apikey'], status=409, **source_dict)

        assert 'frequency' in result
        assert u'Frequency {0} not recognised'.format(wrong_frequency) in result['frequency'][0]

    def test_invalid_wrong_configuration(self):

        source_dict = copy.deepcopy(self.default_source_dict)
        source_dict['config'] = 'not_json'

        result = call_action_api(self.app, self.action,
                                apikey=self.sysadmin['apikey'], status=409, **source_dict)

        assert 'config' in result
        assert u'Error parsing the configuration options: No JSON object could be decoded' in result['config'][0]

        source_dict['config'] = json.dumps({'custom_option': 'not_a_list'})

        result = call_action_api(self.app, self.action,
                                apikey=self.sysadmin['apikey'], status=409, **source_dict)

        assert 'config' in result
        assert u'Error parsing the configuration options: custom_option must be a list' in result['config'][0]


class TestHarvestSourceActionCreate(HarvestSourceActionBase):

    def __init__(self):
        self.action = 'harvest_source_create'



    def test_create(self):

        source_dict = self.default_source_dict

        result = call_action_api(self.app, 'harvest_source_create',
                                apikey=self.sysadmin['apikey'], **source_dict)

        for key in source_dict.keys():
            assert source_dict[key] == result[key]

        # Check that source was actually created
        source = harvest_model.HarvestSource.get(result['id'])
        assert source.url == source_dict['url']
        assert source.type == source_dict['source_type']


        # Trying to create a source with the same URL fails

        source_dict = copy.deepcopy(self.default_source_dict)
        source_dict['name'] = 'test-source-action-new'

        result = call_action_api(self.app, 'harvest_source_create',
                                apikey=self.sysadmin['apikey'], status=409, **source_dict)

        assert 'url' in result
        assert u'There already is a Harvest Source for this URL' in result['url'][0]

class TestHarvestSourceActionUpdate(HarvestSourceActionBase):

    @classmethod
    def setup_class(cls):

        cls.action = 'harvest_source_update'

        super(TestHarvestSourceActionUpdate, cls).setup_class()

        # Create a source to udpate
        source_dict = cls.default_source_dict
        result = call_action_api(cls.app, 'harvest_source_create',
                                apikey=cls.sysadmin['apikey'], **source_dict)

        cls.default_source_dict['id'] = result['id']

    def test_update(self):

        source_dict = self.default_source_dict
        source_dict.update({
          "url": "http://test.action.updated.com",
          "name": "test-source-action-updated",
          "title": "Test source action updated",
          "notes": "Test source action desc updated",
          "source_type": "test",
          "frequency": "MONTHLY",
          "config": json.dumps({"custom_option":["c","d"]})
          })

        result = call_action_api(self.app, 'harvest_source_update',
                                apikey=self.sysadmin['apikey'], **source_dict)

        for key in source_dict.keys():
            assert source_dict[key] == result[key]

        # Check that source was actually updated
        source = harvest_model.HarvestSource.get(result['id'])
        assert source.url == source_dict['url']
        assert source.type == source_dict['source_type']

class TestHarvestObject(unittest.TestCase):
    @classmethod
    def setup_class(cls):
        harvest_model.setup()

    @classmethod
    def teardown_class(cls):
        ckan.model.repo.rebuild_db()

    def test_create(self):
        job = factories.HarvestJobFactory()
        job.save()

        context = {
            'model' : ckan.model,
            'session': ckan.model.Session,
            'ignore_auth': True,
        }
        data_dict = {
            'guid' : 'guid',
            'content' : 'content',
            'job_id' : job.id,
            'extras' : { 'a key' : 'a value' },
        }
        harvest_object = toolkit.get_action('harvest_object_create')(
            context, data_dict)

        # fetch the object from database to check it was created
        created_object = harvest_model.HarvestObject.get(harvest_object['id'])
        assert created_object.guid == harvest_object['guid'] == data_dict['guid']

    def test_create_bad_parameters(self):
        source_a = factories.HarvestSourceFactory()
        source_a.save()
        job = factories.HarvestJobFactory()
        job.save()

        context = {
            'model' : ckan.model,
            'session': ckan.model.Session,
            'ignore_auth': True,
        }
        data_dict = {
            'job_id' : job.id,
            'source_id' : source_a.id,
            'extras' : 1
        }
        harvest_object_create = toolkit.get_action('harvest_object_create')
        self.assertRaises(ckan.logic.ValidationError, harvest_object_create,
            context, data_dict)

        data_dict['extras'] = {'test': 1 }

        self.assertRaises(ckan.logic.ValidationError, harvest_object_create,
            context, data_dict)
