'''Tests elements of queue.py, but doesn't use the queue subsystem
(redis/rabbitmq)
'''
import json

from nose.tools import assert_equal

try:
    from ckan.tests.helpers import reset_db
except ImportError:
    from ckan.new_tests.helpers import reset_db
from ckan import model
from ckan import plugins as p
from ckan.plugins import toolkit

from ckanext.harvest.tests.factories import (HarvestObjectObj)
from ckanext.harvest.interfaces import IHarvester
import ckanext.harvest.model as harvest_model
from ckanext.harvest.tests.lib import run_harvest


class MockHarvester(p.SingletonPlugin):
    p.implements(IHarvester)

    @classmethod
    def _set_test_params(cls, guid, **test_params):
        cls._guid = guid
        cls._test_params = test_params

    def info(self):
        return {'name': 'test', 'title': 'test', 'description': 'test'}

    def gather_stage(self, harvest_job):
        obj = HarvestObjectObj(guid=self._guid, job=harvest_job)
        return [obj.id]

    def fetch_stage(self, harvest_object):
        harvest_object.content = json.dumps({'name': harvest_object.guid})
        harvest_object.save()
        return True

    def import_stage(self, harvest_object):
        user = toolkit.get_action('get_site_user')(
            {'model': model, 'ignore_auth': True}, {}
        )['name']

        package = json.loads(harvest_object.content)
        name = package['name']

        package_object = model.Package.get(name)
        if package_object:
            logic_function = 'package_update'
        else:
            logic_function = 'package_create'

        package_dict = toolkit.get_action(logic_function)(
            {'model': model, 'session': model.Session,
             'user': user},
            json.loads(harvest_object.content)
        )

        if self._test_params.get('object_error'):
            return False

        # successful, so move 'current' to this object
        previous_object = model.Session.query(harvest_model.HarvestObject) \
                               .filter_by(guid=harvest_object.guid) \
                               .filter_by(current=True) \
                               .first()
        if previous_object:
            previous_object.current = False
            previous_object.save()
        harvest_object.package_id = package_dict['id']
        harvest_object.current = True

        if self._test_params.get('delete'):
            # 'current=False' is the key step in getting report_status to be
            # set as 'deleted'
            harvest_object.current = False
            package_object.save()

        harvest_object.save()

        if self._test_params.get('object_unchanged'):
            return 'unchanged'
        return True


class TestEndStates(object):
    @classmethod
    def setup_class(cls):
        reset_db()
        harvest_model.setup()

    def test_create_dataset(self):
        guid = 'obj-create'
        MockHarvester._set_test_params(guid=guid)

        results_by_guid = run_harvest(
            url='http://some-url.com',
            harvester=MockHarvester())

        result = results_by_guid[guid]
        assert_equal(result['state'], 'COMPLETE')
        assert_equal(result['report_status'], 'added')
        assert_equal(result['errors'], [])

    def test_update_dataset(self):
        guid = 'obj-update'
        MockHarvester._set_test_params(guid=guid)

        # create the original harvest_object and dataset
        run_harvest(
            url='http://some-url.com',
            harvester=MockHarvester())
        # update it
        results_by_guid = run_harvest(
            url='http://some-url.com',
            harvester=MockHarvester())

        result = results_by_guid[guid]
        assert_equal(result['state'], 'COMPLETE')
        assert_equal(result['report_status'], 'updated')
        assert_equal(result['errors'], [])

    def test_delete_dataset(self):
        guid = 'obj-delete'
        MockHarvester._set_test_params(guid=guid)
        # create the original harvest_object and dataset
        run_harvest(
            url='http://some-url.com',
            harvester=MockHarvester())
        MockHarvester._set_test_params(guid=guid, delete=True)

        # delete it
        results_by_guid = run_harvest(
            url='http://some-url.com',
            harvester=MockHarvester())

        result = results_by_guid[guid]
        assert_equal(result['state'], 'COMPLETE')
        assert_equal(result['report_status'], 'deleted')
        assert_equal(result['errors'], [])

    def test_obj_error(self):
        guid = 'obj-error'
        MockHarvester._set_test_params(guid=guid, object_error=True)

        results_by_guid = run_harvest(
            url='http://some-url.com',
            harvester=MockHarvester())

        result = results_by_guid[guid]
        assert_equal(result['state'], 'ERROR')
        assert_equal(result['report_status'], 'errored')
        assert_equal(result['errors'], [])

    def test_unchanged(self):
        guid = 'obj-error'
        MockHarvester._set_test_params(guid=guid, object_unchanged=True)

        results_by_guid = run_harvest(
            url='http://some-url.com',
            harvester=MockHarvester())

        result = results_by_guid[guid]
        assert_equal(result['state'], 'COMPLETE')
        assert_equal(result['report_status'], 'not modified')
        assert_equal(result['errors'], [])
