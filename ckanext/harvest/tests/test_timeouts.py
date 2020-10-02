from datetime import datetime, timedelta
from nose.tools import assert_equal, assert_in
import pytest
from ckan.tests import factories as ckan_factories
from ckan import model
from ckan.lib.base import config
from ckan.plugins.toolkit import get_action
from ckanext.harvest.model import HarvestSource, HarvestJob, HarvestObject
from ckanext.harvest.tests import factories as harvest_factories
from ckanext.harvest.logic import HarvestJobExists


@pytest.mark.usefixtures('with_plugins', 'clean_db', 'harvest_setup', 'clean_queues')
@pytest.mark.ckan_config('ckan.plugins', 'harvest test_action_harvester')
class TestModelFunctions:
    
    def test_timeout_jobs(self):
        """ Create harvest spurce, job and objects
            Validate we read the last object fished time
            Validate we raise timeout in harvest_jobs_run_action
            """
        source, job = self.get_source()
        
        ob1 = self.add_object(job=job, source=source, state='COMPLETE', minutes_ago=10)
        ob2 = self.add_object(job=job, source=source, state='COMPLETE', minutes_ago=5)
        ob3 = self.add_object(job=job, source=source, state='COMPLETE', minutes_ago=15)
        
        assert_equal(job.get_last_finished_object(), ob2)
        assert_equal(job.get_last_action_time(), ob2.import_finished)

        gather_errors = self.run(timeout=3, source=source, job=job) 
        assert_equal(len(gather_errors), 1)
        assert_equal(job.status, 'Finished')
        gather_error = gather_errors[0]
        assert_in('timeout', gather_error.message)
    
    def test_no_timeout_jobs(self):
        """ Test a job that don't raise timeout """
        source, job = self.get_source()

        ob1 = self.add_object(job=job, source=source, state='COMPLETE', minutes_ago=10)
        ob2 = self.add_object(job=job, source=source, state='COMPLETE', minutes_ago=5)
        ob3 = self.add_object(job=job, source=source, state='COMPLETE', minutes_ago=15)
        
        assert_equal(job.get_last_finished_object(), ob2)
        assert_equal(job.get_last_action_time(), ob2.import_finished)

        gather_errors = self.run(timeout=7, source=source, job=job) 
        assert_equal(len(gather_errors), 0)
        assert_equal(job.status, 'Finished')
    
    def test_no_objects_job(self):
        """ Test a job that don't raise timeout """
        _, job = self.get_source()

        job.gather_finished = datetime.utcnow()
        job.save()

        assert_equal(job.get_last_finished_object(), None)
        assert_equal(job.get_last_action_time(), job.gather_finished)

    def test_no_gathered_job(self):
        """ Test a job that don't raise timeout """
        _, job = self.get_source()

        job.gather_finished = None
        job.save()

        assert_equal(job.get_last_finished_object(), None)
        assert_equal(job.get_last_action_time(), job.created)

    def run(self, timeout, source, job):
        """ Run the havester_job_run and return the errors """

        # check timeout
        context = {'model': model, 'session': model.Session,
                   'ignore_auth': True, 'user': ''}

        data_dict = {
            'guid': 'guid',
            'content': 'content',
            'job_id': job.id,
            'source_id': source.id
        }

        # prepare the job to run
        job.gather_finished = datetime.utcnow()
        job.save()

        # run (we expect a timeout)
        config['ckan.harvest.timeout'] = timeout
        harvest_jobs_run_action = get_action('harvest_jobs_run')
        harvest_jobs_run_action(context, data_dict)
        
        return job.get_gather_errors()

    def get_source(self):

        SOURCE_DICT = {
            "url": "http://test.timeout.com",
            "name": "test-source-timeout",
            "title": "Test source timeout",
            "notes": "Notes source timeout",
            "source_type": "test-for-action",
            "frequency": "MANUAL"
        }
        source = harvest_factories.HarvestSourceObj(**SOURCE_DICT)
        try:
            job = harvest_factories.HarvestJobObj(source=source)
        except HarvestJobExists: # not sure why
            job = source.get_jobs()[0]
        
        job.status = 'Running'
        job.save()
        
        jobs = source.get_jobs(status='Running')
        assert_in(job, jobs)

        return source, job
        
    def add_object(self, job, source, state, minutes_ago):
        now = datetime.utcnow()
        name = 'dataset-{}-{}'.format(state.lower(), minutes_ago)
        dataset = ckan_factories.Dataset(name=name)
        obj = harvest_factories.HarvestObjectObj(
            job=job,
            source=source,
            package_id=dataset['id'],
            guid=dataset['id'],
            content='{}',
            # always is WAITING state=state,
            )

        obj.state = state
        obj.import_finished = now - timedelta(minutes=minutes_ago)
        obj.save()
        return obj
