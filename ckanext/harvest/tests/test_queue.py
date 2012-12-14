import ckanext.harvest.model as harvest_model
from ckanext.harvest.model import HarvestObject, HarvestObjectExtra
from ckanext.harvest.interfaces import IHarvester
import ckanext.harvest.queue as queue
from ckan.plugins.core import SingletonPlugin, implements
import json
import ckan.logic as logic
from ckan import model


class TestHarvester(SingletonPlugin):
    implements(IHarvester)
    def info(self):
        return {'name': 'test', 'title': 'test', 'description': 'test'}

    def gather_stage(self, harvest_job):

        if harvest_job.source.url.startswith('basic_test'):
            obj = HarvestObject(guid = 'test1', job = harvest_job)
            obj.extras.append(HarvestObjectExtra(key='key', value='value'))
            obj2 = HarvestObject(guid = 'test2', job = harvest_job)
            obj.add()
            obj2.save() # this will commit both
            return [obj.id, obj2.id]

        return []

    def fetch_stage(self, harvest_object):
        assert harvest_object.state == "FETCH"
        assert harvest_object.fetch_started != None
        harvest_object.content = json.dumps({'name': harvest_object.guid})
        harvest_object.save()
        return True

    def import_stage(self, harvest_object):
        assert harvest_object.state == "IMPORT"
        assert harvest_object.fetch_finished != None
        assert harvest_object.import_started != None

        user = logic.get_action('get_site_user')(
            {'model': model, 'ignore_auth': True}, {}
        )['name']
        logic.get_action('package_create')(
            {'model': model, 'session': model.Session,
             'user': user, 'api_version': 3},
            json.loads(harvest_object.content)
        )
        return True


class TestHarvestQueue(object):
    @classmethod
    def setup_class(cls):
        harvest_model.setup()

    @classmethod
    def teardown_class(cls):
        model.repo.rebuild_db()


    def test_01_basic_harvester(cls):

        ### make sure queues/exchanges are created first and are empty
        consumer = queue.get_consumer('ckan.harvest.gather','harvest_job_id')
        consumer_fetch = queue.get_consumer('ckan.harvest.fetch','harvest_object_id')
        consumer.queue_purge(queue='ckan.harvest.gather')
        consumer_fetch.queue_purge(queue='ckan.harvest.fetch')


        user = logic.get_action('get_site_user')(
            {'model': model, 'ignore_auth': True}, {}
        )['name']

        context = {'model': model, 'session': model.Session,
                   'user': user, 'api_version': 3}

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
            {'source_id':harvest_source['id']}
        )

        job_id = harvest_job['id']

        assert harvest_job['source_id'] == harvest_source['id'], harvest_job

        assert harvest_job['status'] == u'New'

        logic.get_action('harvest_jobs_run')(
            context,
            {'source_id':harvest_source['id']}
        )

        assert logic.get_action('harvest_job_show')(
            context,
            {'id': job_id}
        )['status'] == u'Running'

        ## pop on item off the queue and run the callback
        reply = consumer.basic_get(queue='ckan.harvest.gather')
        queue.gather_callback(consumer, *reply)

        all_objects = model.Session.query(HarvestObject).all()

        assert len(all_objects) == 2
        assert all_objects[0].state == 'WAITING'
        assert all_objects[1].state == 'WAITING'


        assert len(model.Session.query(HarvestObject).all()) == 2
        assert len(model.Session.query(HarvestObjectExtra).all()) == 1

        ## do twice as two harvest objects
        reply = consumer_fetch.basic_get(queue='ckan.harvest.fetch')
        queue.fetch_callback(consumer, *reply)
        reply = consumer_fetch.basic_get(queue='ckan.harvest.fetch')
        queue.fetch_callback(consumer, *reply)

        count = model.Session.query(model.Package) \
                .filter(model.Package.type==None) \
                .count()
        assert count == 2

        all_objects = model.Session.query(HarvestObject).all()
        assert len(all_objects) == 2
        assert all_objects[0].state == 'COMPLETE'
        assert all_objects[1].state == 'COMPLETE'

        ## fire run again to check if job is set to Finished
        try:
            logic.get_action('harvest_jobs_run')(
                context,
                {'source_id':harvest_source['id']}
            )
        except Exception, e:
            assert 'There are no new harvesting jobs' in str(e)

        assert logic.get_action('harvest_job_show')(
            context,
            {'id': job_id}
        )['status'] == u'Finished'
