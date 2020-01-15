# -*- coding: utf-8 -*-

from __future__ import print_function
import click
import six

import sys
from pprint import pprint

from ckan import model
from ckan.logic import get_action, ValidationError
import ckantoolkit as tk

import ckanext.harvest.utils as utils

def get_commands():
    return [harvester]

@click.group()
def harvester():
    """Harvests remotely mastered metadata.
    """
    pass

@harvester.command()
def initdb():
    """Creates the necessary tables in the database.
    """
    utils.initdb()
    click.secho(u'DB tables created', fg=u'green')

@harvester.group()
def source():
    """Manage harvest sources
    """
    pass

@source.command()
@click.argument(u'name')
@click.argument(u'url')
@click.argument(u'type')
@click.argument(u'title', required=False)
@click.argument(u'active', type=tk.asbool, default=True)
@click.argument(u'owner_org', required=False)
@click.argument(u'frequency', default=u'MANUAL')
@click.argument(u'config', required=False)
def create(name, url, type, title, active, owner_org, frequency, config):
    """Create new harvest source.
    """
    result = utils.create_harvest_source(
        name, url, type, title, active, owner_org, frequency, config
    )
    click.echo(result)


@source.command()
@click.argument(u'id', metavar=u'SOURCE_ID_OR_NAME')
def show(id):
    """Shows a harvest source.
    """
    # harvester source {}
    try:
        result = utils.show_harvest_source(id)
    except tk.ObjectNotFound as e:
        tk.error_shout(u'Source <{}> not found.'.format(id))
        raise click.Abort()
    click.echo(result)



class Harvester(object):
    '''Harvests remotely mastered metadata
    Usage:


      harvester rmsource {source-id/name}
        - remove (deactivate) a harvester source, whilst leaving any related
          datasets, jobs and objects

      harvester clearsource {source-id/name}
        - clears all datasets, jobs and objects related to a harvest source,
          but keeps the source itself

      harvester clearsource_history [{source-id}]
        - If no source id is given the history for all harvest sources (maximum is 1000) will be cleared.
          Clears all jobs and objects related to a harvest source, but keeps the source itself.
          The datasets imported from the harvest source will NOT be deleted!!!
          If a source id is given, it only clears the history of the harvest source with the given source id.

      harvester sources [all]
        - lists harvest sources
          If 'all' is defined, it also shows the Inactive sources

      harvester job {source-id/name}
        - create new harvest job and runs it (puts it on the gather queue)

      harvester jobs
        - lists harvest jobs

      harvester job_abort {source-id/source-name/obj-id}
        - marks a job as "Aborted" so that the source can be restarted afresh.
          It ensures that the job's harvest objects status are also marked
          finished. You should ensure that neither the job nor its objects are
          currently in the gather/fetch queues.

      harvester run
        - starts any harvest jobs that have been created by putting them onto
          the gather queue. Also checks running jobs - if finished it
          changes their status to Finished.

      harvester run_test {source-id/name}
        - runs a harvest - for testing only.
          This does all the stages of the harvest (creates job, gather, fetch,
          import) without involving the web UI or the queue backends. This is
          useful for testing a harvester without having to fire up
          gather/fetch_consumer processes, as is done in production.

      harvester gather_consumer
        - starts the consumer for the gathering queue

      harvester fetch_consumer
        - starts the consumer for the fetching queue

      harvester purge_queues
        - removes all jobs from fetch and gather queue

      harvester clean_harvest_log
        - Clean-up mechanism for the harvest log table.
          You can configure the time frame through the configuration
          parameter `ckan.harvest.log_timeframe`. The default time frame is 30 days

      harvester [-j] [-o|-g|-p {id/guid}] [--segments={segments}] import [{source-id}]
        - perform the import stage with the last fetched objects, for a certain
          source or a single harvest object. Please note that no objects will
          be fetched from the remote server. It will only affect the objects
          already present in the database.

          To import a particular harvest source, specify its id as an argument.
          To import a particular harvest object use the -o option.
          To import a particular guid use the -g option.
          To import a particular package use the -p option.

          You will need to specify the -j flag in cases where the datasets are
          not yet created (e.g. first harvest, or all previous harvests have
          failed)

          The --segments flag allows to define a string containing hex digits that represent which of
          the 16 harvest object segments to import. e.g. 15af will run segments 1,5,a,f

      harvester job-all
        - create new harvest jobs for all active sources.

      harvester reindex
        - reindexes the harvest source datasets

    The commands should be run from the ckanext-harvest directory and expect
    a development.ini file to be present. Most of the time you will
    specify the config explicitly though::

        paster harvester sources --config=../ckan/development.ini

    '''

    def __init__(self, name):

        super(Harvester, self).__init__(name)

        self.parser.add_option('-j', '--no-join-datasets', dest='no_join_datasets',
                               action='store_true', default=False, help='Do not join harvest objects to existing datasets')

        self.parser.add_option('-o', '--harvest-object-id', dest='harvest_object_id',
                               default=False, help='Id of the harvest object to which perform the import stage')

        self.parser.add_option('-p', '--package-id', dest='package_id',
                               default=False, help='Id of the package whose harvest object to perform the import stage for')

        self.parser.add_option('-g', '--guid', dest='guid',
                               default=False, help='Guid of the harvest object to which perform the import stage for')

        self.parser.add_option('--segments', dest='segments',
                               default=False, help='''A string containing hex digits that represent which of
 the 16 harvest object segments to import. e.g. 15af will run segments 1,5,a,f''')

    def command(self):
        self._load_config()


        if cmd == 'rmsource':
            self.remove_harvest_source()
        elif cmd == 'clearsource':
            self.clear_harvest_source()
        elif cmd == 'clearsource_history':
            self.clear_harvest_source_history()
        elif cmd == 'sources':
            self.list_harvest_sources()
        elif cmd == 'job':
            self.create_harvest_job()
        elif cmd == 'jobs':
            self.list_harvest_jobs()
        elif cmd == 'job_abort':
            self.job_abort()
        elif cmd == 'run':
            self.run_harvester()
        elif cmd == 'run_test':
            self.run_test_harvest()
        elif cmd == 'gather_consumer':
            import logging
            from ckanext.harvest.queue import (get_gather_consumer,
                                               gather_callback, get_gather_queue_name)
            logging.getLogger('amqplib').setLevel(logging.INFO)
            consumer = get_gather_consumer()
            for method, header, body in consumer.consume(queue=get_gather_queue_name()):
                gather_callback(consumer, method, header, body)
        elif cmd == 'fetch_consumer':
            import logging
            logging.getLogger('amqplib').setLevel(logging.INFO)
            from ckanext.harvest.queue import (get_fetch_consumer, fetch_callback,
                                               get_fetch_queue_name)
            consumer = get_fetch_consumer()
            for method, header, body in consumer.consume(queue=get_fetch_queue_name()):
                fetch_callback(consumer, method, header, body)
        elif cmd == 'purge_queues':
            self.purge_queues()
        elif cmd == 'import':
            self.initdb()
            self.import_stage()
        elif cmd == 'job-all':
            self.create_harvest_job_all()
        elif cmd == 'harvesters-info':
            harvesters_info = get_action('harvesters_info_show')()
            pprint(harvesters_info)
        elif cmd == 'reindex':
            self.reindex()
        elif cmd == 'clean_harvest_log':
            self.clean_harvest_log()
        else:
            print('Command {0} not recognized'.format(cmd))

    def _load_config(self):
        super(Harvester, self)._load_config()



    def clear_harvest_source_history(self):
        source_id = None
        if len(self.args) >= 2:
            source_id = six.text_type(self.args[1])

        context = {
            'model': model,
            'user': self.admin_user['name'],
            'session': model.Session
        }
        if source_id is not None:
            get_action('harvest_source_job_history_clear')(context, {'id': source_id})
            print('Cleared job history of harvest source: {0}'.format(source_id))
        else:
            '''
            Purge queues, because we clean all harvest jobs and
            objects in the database.
            '''
            self.purge_queues()
            cleared_sources_dicts = get_action('harvest_sources_job_history_clear')(context, {})
            print('Cleared job history for all harvest sources: {0} source(s)'.format(len(cleared_sources_dicts)))

    def remove_harvest_source(self):
        if len(self.args) >= 2:
            source_id_or_name = six.text_type(self.args[1])
        else:
            print('Please provide a source id')
            sys.exit(1)
        context = {'model': model, 'session': model.Session,
                   'user': self.admin_user['name']}
        source = get_action('harvest_source_show')(
            context, {'id': source_id_or_name})
        get_action('harvest_source_delete')(context, {'id': source['id']})
        print('Removed harvest source: {0}'.format(source_id_or_name))

    def clear_harvest_source(self):
        if len(self.args) >= 2:
            source_id_or_name = six.text_type(self.args[1])
        else:
            print('Please provide a source id')
            sys.exit(1)
        context = {'model': model, 'session': model.Session,
                   'user': self.admin_user['name']}
        source = get_action('harvest_source_show')(
            context, {'id': source_id_or_name})
        get_action('harvest_source_clear')(context, {'id': source['id']})
        print('Cleared harvest source: {0}'.format(source_id_or_name))

    def list_harvest_sources(self):
        if len(self.args) >= 2 and self.args[1] == 'all':
            data_dict = {}
            what = 'harvest source'
        else:
            data_dict = {'only_active': True}
            what = 'active harvest source'

        context = {'model': model, 'session': model.Session, 'user': self.admin_user['name']}
        sources = get_action('harvest_source_list')(context, data_dict)
        self.print_harvest_sources(sources)
        _print_there_are(what=what, sequence=sources)

    def create_harvest_job(self):
        if len(self.args) >= 2:
            source_id_or_name = six.text_type(self.args[1])
        else:
            print('Please provide a source id')
            sys.exit(1)
        context = {'model': model, 'session': model.Session,
                   'user': self.admin_user['name']}
        source = get_action('harvest_source_show')(
            context, {'id': source_id_or_name})

        context = {'model': model, 'session': model.Session, 'user': self.admin_user['name']}
        job = get_action('harvest_job_create')(
            context, {'source_id': source['id'], 'run': True})

        self.print_harvest_job(job)
        jobs = get_action('harvest_job_list')(context, {'status': u'New'})
        _print_there_are('harvest job', jobs, condition=u'New')

    def list_harvest_jobs(self):
        context = {'model': model, 'user': self.admin_user['name'], 'session': model.Session}
        jobs = get_action('harvest_job_list')(context, {})

        self.print_harvest_jobs(jobs)
        _print_there_are(what='harvest job', sequence=jobs)

    def job_abort(self):
        if len(self.args) >= 2:
            job_or_source_id_or_name = six.text_type(self.args[1])
        else:
            print('Please provide a job id or source name/id')
            sys.exit(1)

        context = {'model': model, 'user': self.admin_user['name'],
                   'session': model.Session}
        job = get_action('harvest_job_abort')(
            context, {'id': job_or_source_id_or_name})
        print('Job status: {0}'.format(job['status']))

    def run_harvester(self):
        context = {'model': model, 'user': self.admin_user['name'],
                   'session': model.Session}
        get_action('harvest_jobs_run')(context, {})

    def run_test_harvest(self):
        from ckanext.harvest import queue
        from ckanext.harvest.tests import lib
        from ckanext.harvest.logic import HarvestJobExists
        from ckanext.harvest.model import HarvestJob

        # Determine the source
        if len(self.args) >= 2:
            source_id_or_name = six.text_type(self.args[1])
        else:
            print('Please provide a source id')
            sys.exit(1)
        context = {'model': model, 'session': model.Session,
                   'user': self.admin_user['name']}
        source = get_action('harvest_source_show')(
            context, {'id': source_id_or_name})

        # Determine the job
        try:
            job_dict = get_action('harvest_job_create')(
                context, {'source_id': source['id']})
        except HarvestJobExists:
            running_jobs = get_action('harvest_job_list')(
                context, {'source_id': source['id'], 'status': 'Running'})
            if running_jobs:
                print('\nSource "{0}" apparently has a "Running" job:\n{1}'
                      .format(source.get('name') or source['id'], running_jobs))
                resp = raw_input('Abort it? (y/n)')
                if not resp.lower().startswith('y'):
                    sys.exit(1)
                job_dict = get_action('harvest_job_abort')(
                    context, {'source_id': source['id']})
            else:
                print('Reusing existing harvest job')
                jobs = get_action('harvest_job_list')(
                    context, {'source_id': source['id'], 'status': 'New'})
                assert len(jobs) == 1, \
                    'Multiple "New" jobs for this source! {0}'.format(jobs)
                job_dict = jobs[0]
        job_obj = HarvestJob.get(job_dict['id'])

        harvester = queue.get_harvester(source['source_type'])
        assert harvester, \
            'No harvester found for type: {0}'.format(source['source_type'])
        lib.run_harvest_job(job_obj, harvester)

    def import_stage(self):

        if len(self.args) >= 2:
            source_id_or_name = six.text_type(self.args[1])
            context = {'model': model, 'session': model.Session,
                       'user': self.admin_user['name']}
            source = get_action('harvest_source_show')(
                context, {'id': source_id_or_name})
            source_id = source['id']
        else:
            source_id = None

        context = {'model': model, 'session': model.Session,
                   'user': self.admin_user['name'],
                   'join_datasets': not self.options.no_join_datasets,
                   'segments': self.options.segments}

        objs_count = get_action('harvest_objects_import')(context, {
                'source_id': source_id,
                'harvest_object_id': self.options.harvest_object_id,
                'package_id': self.options.package_id,
                'guid': self.options.guid,
                })

        print('{0} objects reimported'.format(objs_count))

    def create_harvest_job_all(self):
        context = {'model': model, 'user': self.admin_user['name'], 'session': model.Session}
        jobs = get_action('harvest_job_create_all')(context, {})
        print('Created {0} new harvest jobs'.format(len(jobs)))

    def reindex(self):
        context = {'model': model, 'user': self.admin_user['name']}
        get_action('harvest_sources_reindex')(context, {})

    def purge_queues(self):
        from ckanext.harvest.queue import purge_queues
        purge_queues()

    def print_harvest_sources(self, sources):
        if sources:
            print('')
        for source in sources:
            print(_print_harvest_source(source))


    def print_harvest_jobs(self, jobs):
        if jobs:
            print('')
        for job in jobs:
            self.print_harvest_job(job)

    def print_harvest_job(self, job):
        print('       Job id: {0}'.format(job.get('id')))
        print('       status: {0}'.format(job.get('status')))
        print('       source: {0}'.format(job.get('source_id')))
        print('      objects: {0}'.format(len(job.get('objects', []))))

        print('gather_errors: {0}'.format(len(job.get('gather_errors', []))))
        for error in job.get('gather_errors', []):
            print('               {0}'.format(error['message']))

        print('')



    def clean_harvest_log(self):
        from datetime import datetime, timedelta
        from ckantoolkit import config
        from ckanext.harvest.model import clean_harvest_log

        # Log time frame - in days
        log_timeframe = tk.asint(config.get('ckan.harvest.log_timeframe', 30))
        condition = datetime.utcnow() - timedelta(days=log_timeframe)

        # Delete logs older then the given date
        clean_harvest_log(condition=condition)
