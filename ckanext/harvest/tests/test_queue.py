import pytest
import six
from mock import patch

from ckanext.harvest.model import HarvestObject, HarvestObjectExtra
from ckanext.harvest.interfaces import IHarvester
import ckanext.harvest.queue as queue
from ckan.plugins.core import SingletonPlugin, implements
import json
import ckan.logic as logic
from ckan import model
from ckan.lib.base import config
import uuid


class MockHarvester(SingletonPlugin):
    implements(IHarvester)

    def info(self):
        return {'name': 'test', 'title': 'test', 'description': 'test'}

    def gather_stage(self, harvest_job):

        if harvest_job.source.url.startswith('basic_test'):
            obj = HarvestObject(guid='test1', job=harvest_job)
            obj.extras.append(HarvestObjectExtra(key='key', value='value'))
            obj2 = HarvestObject(guid='test2', job=harvest_job)
            obj3 = HarvestObject(guid='test_to_delete', job=harvest_job)
            obj.add()
            obj2.add()
            obj3.save()  # this will commit both
            return [obj.id, obj2.id, obj3.id]

        return []

    def fetch_stage(self, harvest_object):
        assert harvest_object.state == "FETCH"
        assert harvest_object.fetch_started is not None
        harvest_object.content = json.dumps({'name': harvest_object.guid})
        harvest_object.save()
        return True

    def import_stage(self, harvest_object):
        assert harvest_object.state == "IMPORT"
        assert harvest_object.fetch_finished is not None
        assert harvest_object.import_started is not None

        user = logic.get_action('get_site_user')(
            {'model': model, 'ignore_auth': True}, {}
        )['name']

        package = json.loads(harvest_object.content)
        name = package['name']

        package_object = model.Package.get(name)
        if package_object:
            logic_function = 'package_update'
        else:
            logic_function = 'package_create'

        package_dict = logic.get_action(logic_function)(
            {'model': model, 'session': model.Session,
             'user': user, 'api_version': 3, 'ignore_auth': True},
            json.loads(harvest_object.content)
        )

        # set previous objects to not current
        previous_object = model.Session.query(HarvestObject) \
            .filter(HarvestObject.guid == harvest_object.guid) \
            .filter(
            HarvestObject.current == True  # noqa: E712
        ).first()
        if previous_object:
            previous_object.current = False
            previous_object.save()

        # delete test_to_delete package on second run
        harvest_object.package_id = package_dict['id']
        harvest_object.current = True
        if package_dict['name'] == 'test_to_delete' and package_object:
            harvest_object.current = False
            package_object.state = 'deleted'
            package_object.save()

        harvest_object.save()
        return True


@pytest.mark.usefixtures('with_plugins', 'clean_db', 'harvest_setup', 'clean_queues')
@pytest.mark.ckan_config('ckan.plugins', 'harvest test_harvester')
class TestHarvestQueue(object):

    def test_01_basic_harvester(self):

        # make sure queues/exchanges are created first and are empty
        consumer = queue.get_gather_consumer()
        consumer_fetch = queue.get_fetch_consumer()
        consumer.queue_purge(queue=queue.get_gather_queue_name())
        consumer_fetch.queue_purge(queue=queue.get_fetch_queue_name())

        user = logic.get_action('get_site_user')(
            {'model': model, 'ignore_auth': True}, {}
        )['name']

        context = {'model': model, 'session': model.Session,
                   'user': user, 'api_version': 3, 'ignore_auth': True}

        source_dict = {
            'title': 'Test Source',
            'name': 'test-source',
            'url': 'basic_test',
            'source_type': 'test',
        }

        harvest_source = logic.get_action('harvest_source_create')(
            context,
            source_dict
        )

        assert harvest_source['source_type'] == 'test', harvest_source
        assert harvest_source['url'] == 'basic_test', harvest_source

        harvest_job = logic.get_action('harvest_job_create')(
            context,
            {'source_id': harvest_source['id'], 'run': True}
        )

        job_id = harvest_job['id']

        assert harvest_job['source_id'] == harvest_source['id'], harvest_job

        assert harvest_job['status'] == u'Running'

        assert logic.get_action('harvest_job_show')(
            context,
            {'id': job_id}
        )['status'] == u'Running'

        # pop on item off the queue and run the callback
        reply = consumer.basic_get(queue='ckan.harvest.gather')

        queue.gather_callback(consumer, *reply)

        all_objects = model.Session.query(HarvestObject).all()

        assert len(all_objects) == 3
        assert all_objects[0].state == 'WAITING'
        assert all_objects[1].state == 'WAITING'
        assert all_objects[2].state == 'WAITING'

        assert len(model.Session.query(HarvestObject).all()) == 3
        assert len(model.Session.query(HarvestObjectExtra).all()) == 1

        # do three times as three harvest objects
        reply = consumer_fetch.basic_get(queue='ckan.harvest.fetch')
        queue.fetch_callback(consumer_fetch, *reply)
        reply = consumer_fetch.basic_get(queue='ckan.harvest.fetch')
        queue.fetch_callback(consumer_fetch, *reply)
        reply = consumer_fetch.basic_get(queue='ckan.harvest.fetch')
        queue.fetch_callback(consumer_fetch, *reply)

        count = model.Session.query(model.Package) \
            .filter(model.Package.type == 'dataset') \
            .count()
        assert count == 3
        all_objects = model.Session.query(HarvestObject).filter_by(current=True).all()

        assert len(all_objects) == 3
        assert all_objects[0].state == 'COMPLETE'
        assert all_objects[0].report_status == 'added'
        assert all_objects[1].state == 'COMPLETE'
        assert all_objects[1].report_status == 'added'
        assert all_objects[2].state == 'COMPLETE'
        assert all_objects[2].report_status == 'added'

        # fire run again to check if job is set to Finished
        logic.get_action('harvest_jobs_run')(
            context,
            {'source_id': harvest_source['id']}
        )

        harvest_job = logic.get_action('harvest_job_show')(
            context,
            {'id': job_id}
        )

        assert harvest_job['status'] == u'Finished'
        assert harvest_job['stats'] == {'added': 3, 'updated': 0, 'not modified': 0, 'errored': 0, 'deleted': 0}

        harvest_source_dict = logic.get_action('harvest_source_show')(
            context,
            {'id': harvest_source['id']}
        )

        assert harvest_source_dict['status']['last_job']['stats'] == {
            'added': 3, 'updated': 0, 'not modified': 0, 'errored': 0, 'deleted': 0}
        assert harvest_source_dict['status']['total_datasets'] == 3
        assert harvest_source_dict['status']['job_count'] == 1

        # Second run
        harvest_job = logic.get_action('harvest_job_create')(
            context,
            {'source_id': harvest_source['id'], 'run': True}
        )

        job_id = harvest_job['id']
        assert logic.get_action('harvest_job_show')(
            context,
            {'id': job_id}
        )['status'] == u'Running'

        # pop on item off the queue and run the callback
        reply = consumer.basic_get(queue='ckan.harvest.gather')
        queue.gather_callback(consumer, *reply)

        all_objects = model.Session.query(HarvestObject).all()

        assert len(all_objects) == 6

        reply = consumer_fetch.basic_get(queue='ckan.harvest.fetch')
        queue.fetch_callback(consumer_fetch, *reply)
        reply = consumer_fetch.basic_get(queue='ckan.harvest.fetch')
        queue.fetch_callback(consumer_fetch, *reply)
        reply = consumer_fetch.basic_get(queue='ckan.harvest.fetch')
        queue.fetch_callback(consumer_fetch, *reply)

        count = model.Session.query(model.Package) \
            .filter(model.Package.type == 'dataset') \
            .count()
        assert count == 3

        all_objects = model.Session.query(HarvestObject).filter_by(report_status='added').all()
        assert len(all_objects) == 3

        all_objects = model.Session.query(HarvestObject).filter_by(report_status='updated').all()
        assert len(all_objects) == 2

        all_objects = model.Session.query(HarvestObject).filter_by(report_status='deleted').all()
        assert len(all_objects) == 1

        # run to make sure job is marked as finshed
        logic.get_action('harvest_jobs_run')(
            context,
            {'source_id': harvest_source['id']}
        )

        harvest_job = logic.get_action('harvest_job_show')(
            context,
            {'id': job_id}
        )
        assert harvest_job['stats'] == {'added': 0, 'updated': 2, 'not modified': 0, 'errored': 0, 'deleted': 1}

        harvest_source_dict = logic.get_action('harvest_source_show')(
            context,
            {'id': harvest_source['id']}
        )

        assert harvest_source_dict['status']['last_job']['stats'] == {
            'added': 0, 'updated': 2, 'not modified': 0, 'errored': 0, 'deleted': 1}
        assert harvest_source_dict['status']['total_datasets'] == 2
        assert harvest_source_dict['status']['job_count'] == 2

    def test_redis_queue_purging(self):
        '''
        Test that Redis queue purging doesn't purge the wrong keys.
        '''
        if config.get('ckan.harvest.mq.type') != 'redis':
            pytest.skip()
        redis = queue.get_connection()
        try:
            redis.set('ckanext-harvest:some-random-key', 'foobar')

            # Create some fake jobs
            gather_publisher = queue.get_gather_publisher()
            gather_publisher.send({'harvest_job_id': str(uuid.uuid4())})
            gather_publisher.send({'harvest_job_id': str(uuid.uuid4())})
            fetch_publisher = queue.get_fetch_publisher()
            fetch_publisher.send({'harvest_object_id': str(uuid.uuid4())})
            fetch_publisher.send({'harvest_object_id': str(uuid.uuid4())})
            num_keys = redis.dbsize()

            # Create some fake objects
            gather_consumer = queue.get_gather_consumer()
            next(gather_consumer.consume(queue.get_gather_queue_name()))
            fetch_consumer = queue.get_fetch_consumer()
            next(fetch_consumer.consume(queue.get_fetch_queue_name()))

            assert redis.dbsize() > num_keys

            queue.purge_queues()

            assert redis.get('ckanext-harvest:some-random-key') == 'foobar'
            assert redis.dbsize() == num_keys
            assert redis.llen(queue.get_gather_routing_key()) == 0
            assert redis.llen(queue.get_fetch_routing_key()) == 0
        finally:
            redis.delete('ckanext-harvest:some-random-key')


class TestHarvestCorruptRedis(object):

    @patch('ckanext.harvest.queue.log.error')
    def test_redis_corrupt(self, mock_log_error):
        '''
        Test that corrupt Redis doesn't stop harvest process and still processes other jobs.
        '''
        if config.get('ckan.harvest.mq.type') != 'redis':
            pytest.skip()
        redis = queue.get_connection()
        try:
            redis.set('ckanext-harvest:some-random-key-2', 'foobar')

            # make sure queues/exchanges are created first and are empty
            gather_consumer = queue.get_gather_consumer()
            fetch_consumer = queue.get_fetch_consumer()
            gather_consumer.queue_purge(queue=queue.get_gather_queue_name())
            fetch_consumer.queue_purge(queue=queue.get_fetch_queue_name())

            # Create some fake jobs and objects with no harvest_job_id
            gather_publisher = queue.get_gather_publisher()
            gather_publisher.send({'harvest_job_id': str(uuid.uuid4())})
            fetch_publisher = queue.get_fetch_publisher()
            fetch_publisher.send({'harvest_object_id': None})
            h_obj_id = str(uuid.uuid4())
            fetch_publisher.send({'harvest_object_id': h_obj_id})

            # Create some fake objects
            next(gather_consumer.consume(queue.get_gather_queue_name()))
            _, _, body = next(fetch_consumer.consume(queue.get_fetch_queue_name()))

            json_obj = json.loads(body)
            assert json_obj['harvest_object_id'] == h_obj_id

            assert mock_log_error.call_count == 1
            args, _ = mock_log_error.call_args_list[0]
            assert "concatenate" in str(args[1])



        finally:
            redis.delete('ckanext-harvest:some-random-key-2')
