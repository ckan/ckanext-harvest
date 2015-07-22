import sys
from pprint import pprint

from ckan import model
from ckan.logic import get_action, ValidationError
from ckanext.harvest.logic import NoNewHarvestJobError

from ckan.lib.cli import CkanCommand

class Harvester(CkanCommand):
    '''Harvests remotely mastered metadata

    Usage:

      harvester initdb
        - Creates the necessary tables in the database

      harvester source {name} {url} {type} [{title}] [{active}] [{owner_org}] [{frequency}] [{config}]
        - create new harvest source

      harvester rmsource {id}
        - remove (deactivate) a harvester source, whilst leaving any related datasets, jobs and objects

      harvester clearsource {id}
        - clears all datasets, jobs and objects related to a harvest source, but keeps the source itself

      harvester sources [all]
        - lists harvest sources
          If 'all' is defined, it also shows the Inactive sources

      harvester job {source-id}
        - create new harvest job

      harvester jobs
        - lists harvest jobs

      harvester run
        - runs harvest jobs

      harvester gather_consumer
        - starts the consumer for the gathering queue

      harvester fetch_consumer
        - starts the consumer for the fetching queue

      harvester purge_queues
        - removes all jobs from fetch and gather queue

      harvester [-j] [-o] [--segments={segments}] import [{source-id}]
        - perform the import stage with the last fetched objects, for a certain
          source or a single harvest object. Please note that no objects will
          be fetched from the remote server. It will only affect the objects
          already present in the database.

          To perform it on a particular object use the -o flag.

          If the -j flag is provided, the objects are not joined to existing datasets. This may be useful
          when importing objects for the first time.

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

    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 9
    min_args = 0

    def __init__(self,name):

        super(Harvester,self).__init__(name)

        self.parser.add_option('-j', '--no-join-datasets', dest='no_join_datasets',
            action='store_true', default=False, help='Do not join harvest objects to existing datasets')

        self.parser.add_option('-o', '--harvest-object-id', dest='harvest_object_id',
            default=False, help='Id of the harvest object to which perfom the import stage')

        self.parser.add_option('-p', '--package-id', dest='package_id',
            default=False, help='Id of the package whose harvest object to perfom the import stage for')

        self.parser.add_option('--segments', dest='segments',
            default=False, help=
'''A string containing hex digits that represent which of
 the 16 harvest object segments to import. e.g. 15af will run segments 1,5,a,f''')

    def command(self):
        self._load_config()

        # We'll need a sysadmin user to perform most of the actions
        # We will use the sysadmin site user (named as the site_id)
        context = {'model':model,'session':model.Session,'ignore_auth':True}
        self.admin_user = get_action('get_site_user')(context,{})


        print ''

        if len(self.args) == 0:
            self.parser.print_usage()
            sys.exit(1)
        cmd = self.args[0]
        if cmd == 'source':
            self.create_harvest_source()
        elif cmd == 'rmsource':
            self.remove_harvest_source()
        elif cmd == 'clearsource':
            self.clear_harvest_source()
        elif cmd == 'sources':
            self.list_harvest_sources()
        elif cmd == 'job':
            self.create_harvest_job()
        elif cmd == 'jobs':
            self.list_harvest_jobs()
        elif cmd == 'run':
            self.run_harvester()
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
            from ckanext.harvest.queue import purge_queues
            purge_queues()
        elif cmd == 'initdb':
            self.initdb()
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
        else:
            print 'Command %s not recognized' % cmd

    def _load_config(self):
        super(Harvester, self)._load_config()

    def initdb(self):
        from ckanext.harvest.model import setup as db_setup
        db_setup()

        print 'DB tables created'

    def create_harvest_source(self):

        if len(self.args) >= 2:
            name = unicode(self.args[1])
        else:
            print 'Please provide a source name'
            sys.exit(1)
        if len(self.args) >= 3:
            url = unicode(self.args[2])
        else:
            print 'Please provide a source URL'
            sys.exit(1)
        if len(self.args) >= 4:
            type = unicode(self.args[3])
        else:
            print 'Please provide a source type'
            sys.exit(1)

        if len(self.args) >= 5:
            title = unicode(self.args[4])
        else:
            title = None
        if len(self.args) >= 6:
            active = not(self.args[5].lower() == 'false' or \
                    self.args[5] == '0')
        else:
            active = True
        if len(self.args) >= 7:
            owner_org = unicode(self.args[6])
        else:
            owner_org = None
        if len(self.args) >= 8:
            frequency = unicode(self.args[7])
            if not frequency:
                frequency = 'MANUAL'
        else:
            frequency = 'MANUAL'
        if len(self.args) >= 9:
            config = unicode(self.args[8])
        else:
            config = None

        try:
            data_dict = {
                    'name': name,
                    'url': url,
                    'source_type': type,
                    'title': title,
                    'active':active,
                    'owner_org': owner_org,
                    'frequency': frequency,
                    'config': config,
                    }

            context = {
                'model':model,
                'session':model.Session,
                'user': self.admin_user['name'],
                'ignore_auth': True,
            }
            source = get_action('harvest_source_create')(context,data_dict)
            print 'Created new harvest source:'
            self.print_harvest_source(source)

            sources = get_action('harvest_source_list')(context,{})
            self.print_there_are('harvest source', sources)

            # Create a harvest job for the new source if not regular job.
            if not data_dict['frequency']:
                get_action('harvest_job_create')(context,{'source_id':source['id']})
                print 'A new Harvest Job for this source has also been created'

        except ValidationError,e:
           print 'An error occurred:'
           print str(e.error_dict)
           raise e

    def remove_harvest_source(self):
        if len(self.args) >= 2:
            source_id = unicode(self.args[1])
        else:
            print 'Please provide a source id'
            sys.exit(1)
        context = {'model': model, 'user': self.admin_user['name'], 'session':model.Session}
        get_action('harvest_source_delete')(context,{'id':source_id})
        print 'Removed harvest source: %s' % source_id

    def clear_harvest_source(self):
        if len(self.args) >= 2:
            source_id = unicode(self.args[1])
        else:
            print 'Please provide a source id'
            sys.exit(1)
        context = {'model': model, 'user': self.admin_user['name'], 'session':model.Session}
        get_action('harvest_source_clear')(context,{'id':source_id})
        print 'Cleared harvest source: %s' % source_id

    def list_harvest_sources(self):
        if len(self.args) >= 2 and self.args[1] == 'all':
            data_dict = {}
            what = 'harvest source'
        else:
            data_dict = {'only_active':True}
            what = 'active harvest source'

        context = {'model': model,'session':model.Session, 'user': self.admin_user['name']}
        sources = get_action('harvest_source_list')(context,data_dict)
        self.print_harvest_sources(sources)
        self.print_there_are(what=what, sequence=sources)

    def create_harvest_job(self):
        if len(self.args) >= 2:
            source_id = unicode(self.args[1])
        else:
            print 'Please provide a source id'
            sys.exit(1)

        context = {'model': model,'session':model.Session, 'user': self.admin_user['name']}
        job = get_action('harvest_job_create')(context,{'source_id':source_id})

        self.print_harvest_job(job)
        jobs = get_action('harvest_job_list')(context,{'status':u'New'})
        self.print_there_are('harvest job', jobs, condition=u'New')

    def list_harvest_jobs(self):
        context = {'model': model, 'user': self.admin_user['name'], 'session':model.Session}
        jobs = get_action('harvest_job_list')(context,{})

        self.print_harvest_jobs(jobs)
        self.print_there_are(what='harvest job', sequence=jobs)

    def run_harvester(self):
        context = {'model': model, 'user': self.admin_user['name'], 'session':model.Session}
        try:
            jobs = get_action('harvest_jobs_run')(context,{})
        except NoNewHarvestJobError:
            print 'There are no new harvest jobs to run.'

    def import_stage(self):

        if len(self.args) >= 2:
            source_id = unicode(self.args[1])
        else:
            source_id = None

        context = {'model': model, 'session':model.Session, 'user': self.admin_user['name'],
                   'join_datasets': not self.options.no_join_datasets,
                   'segments': self.options.segments}


        objs_count = get_action('harvest_objects_import')(context,{
                'source_id': source_id,
                'harvest_object_id': self.options.harvest_object_id,
                'package_id': self.options.package_id,
                })

        print '%s objects reimported' % objs_count

    def create_harvest_job_all(self):
        context = {'model': model, 'user': self.admin_user['name'], 'session':model.Session}
        jobs = get_action('harvest_job_create_all')(context,{})
        print 'Created %s new harvest jobs' % len(jobs)

    def reindex(self):
        context = {'model': model, 'user': self.admin_user['name']}
        get_action('harvest_sources_reindex')(context,{})


    def print_harvest_sources(self, sources):
        if sources:
            print ''
        for source in sources:
            self.print_harvest_source(source)

    def print_harvest_source(self, source):
        print 'Source id: %s' % source.get('id')
        print '      url: %s' % source.get('url')
        print '     type: %s' % source.get('type')
        print '   active: %s' % (source.get('active', source.get('state') == 'active'))
        print 'frequency: %s' % source.get('frequency')
        print '     jobs: %s' % source.get('status').get('job_count')
        print ''

    def print_harvest_jobs(self, jobs):
        if jobs:
            print ''
        for job in jobs:
            self.print_harvest_job(job)

    def print_harvest_job(self, job):
        print '       Job id: %s' % job.get('id')
        print '       status: %s' % job.get('status')
        print '       source: %s' % job.get('source_id')
        print '      objects: %s' % len(job.get('objects', []))

        print 'gather_errors: %s' % len(job.get('gather_errors', []))
        for error in job.get('gather_errors', []):
            print '               %s' % error['message']

        print ''

    def print_there_are(self, what, sequence, condition=''):
        is_singular = self.is_singular(sequence)
        print 'There %s %s %s%s%s' % (
            is_singular and 'is' or 'are',
            len(sequence),
            condition and ('%s ' % condition.lower()) or '',
            what,
            not is_singular and 's' or '',
        )

    def is_singular(self, sequence):
        return len(sequence) == 1

