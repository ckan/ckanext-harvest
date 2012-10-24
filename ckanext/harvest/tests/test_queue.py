import ckanext.harvest.model as harvest_model
from ckanext.harvest.model import HarvestObject
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

        if harvest_job.source.url == 'basic_test':
            obj = HarvestObject(guid = 'test1', job = harvest_job)
            obj2 = HarvestObject(guid = 'test2', job = harvest_job)
            obj.add()
            obj2.save() # this will commit both
            return [obj.id, obj2.id]

        return []

    def fetch_stage(self, harvest_object):
        harvest_object.content = json.dumps({'name': harvest_object.guid})
        harvest_object.save()
        return True

    def import_stage(self, harvest_object):
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
        consumer.queue_purge(queue='ckan.harvest.fetch')


        user = logic.get_action('get_site_user')(
            {'model': model, 'ignore_auth': True}, {}
        )['name']

        context = {'model': model, 'session': model.Session, 
                   'user': user, 'api_version': 3}

        harvest_source = logic.get_action('harvest_source_create')(
            context,
            {'type':'test', 'url': 'basic_test'}
        )

        assert harvest_source['type'] == 'test', harvest_source
        assert harvest_source['url'] == 'basic_test', harvest_source

        
        harvest_job = logic.get_action('harvest_job_create')(
            context,
            {'source_id':harvest_source['id']}
        )
        assert harvest_job['source_id'] == harvest_source['id'], harvest_job

        harvest_job = logic.get_action('harvest_jobs_run')(
            context,
            {'source_id':harvest_source['id']}
        )

        ## pop on item off the queue and run the callback
        reply = consumer.basic_get(queue='ckan.harvest.gather')
        queue.gather_callback(consumer, *reply)

        assert len(model.Session.query(HarvestObject).all()) == 2
        
        ## do twice as two harvest objects
        reply = consumer.basic_get(queue='ckan.harvest.fetch')
        queue.fetch_callback(consumer, *reply)
        reply = consumer.basic_get(queue='ckan.harvest.fetch')
        queue.fetch_callback(consumer, *reply)

        assert len(model.Session.query(model.Package).all()) == 2
        





