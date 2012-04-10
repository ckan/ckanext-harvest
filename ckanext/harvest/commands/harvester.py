import sys
import re
from pprint import pprint

from ckan import model
from ckan.logic import get_action, ValidationError

from ckan.lib.cli import CkanCommand

class Harvester(CkanCommand):
    '''Harvests remotely mastered metadata

    Usage:

      harvester initdb
        - Creates the necessary tables in the database

      harvester source {url} {type} [{active}] [{user-id}] [{publisher-id}]
        - create new harvest source

      harvester rmsource {id}
        - remove (inactivate) a harvester source

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

      harvester import [{source-id}]
        - perform the import stage with the last fetched objects, optionally belonging to a certain source.
          Please note that no objects will be fetched from the remote server. It will only affect
          the last fetched objects already present in the database.

      harvester job-all
        - create new harvest jobs for all active sources.

    The commands should be run from the ckanext-harvest directory and expect
    a development.ini file to be present. Most of the time you will
    specify the config explicitly though::

        paster harvester sources --config=../ckan/development.ini

    '''

    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 6
    min_args = 0

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
        elif cmd == "rmsource":
            self.remove_harvest_source()
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
            from ckanext.harvest.queue import get_gather_consumer
            logging.getLogger('amqplib').setLevel(logging.INFO)
            consumer = get_gather_consumer()
            consumer.wait()
        elif cmd == 'fetch_consumer':
            import logging
            logging.getLogger('amqplib').setLevel(logging.INFO)
            from ckanext.harvest.queue import get_fetch_consumer
            consumer = get_fetch_consumer()
            consumer.wait()
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
            url = unicode(self.args[1])
        else:
            print 'Please provide a source URL'
            sys.exit(1)
        if len(self.args) >= 3:
            type = unicode(self.args[2])
        else:
            print 'Please provide a source type'
            sys.exit(1)
        if len(self.args) >= 4:
            config = unicode(self.args[3])
        else:
            config = None
        if len(self.args) >= 5:
            active = not(self.args[4].lower() == 'false' or \
                    self.args[4] == '0')
        else:
            active = True
        if len(self.args) >= 6:
            user_id = unicode(self.args[5])
        else:
            user_id = u''
        if len(self.args) >= 7:
            publisher_id = unicode(self.args[6])
        else:
            publisher_id = u''
        try:
            data_dict = {
                    'url':url,
                    'type':type,
                    'config':config,
                    'active':active,
                    'user_id':user_id,
                    'publisher_id':publisher_id}

            context = {'model':model, 'session':model.Session, 'user': self.admin_user['name']}
            source = get_action('harvest_source_create')(context,data_dict)
            print 'Created new harvest source:'
            self.print_harvest_source(source)

            sources = get_action('harvest_source_list')(context,{})
            self.print_there_are('harvest source', sources)

            # Create a harvest job for the new source
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
        self.print_there_are('harvest jobs', jobs, condition=u'New')

    def list_harvest_jobs(self):
        context = {'model': model, 'user': self.admin_user['name'], 'session':model.Session}
        jobs = get_action('harvest_job_list')(context,{})

        self.print_harvest_jobs(jobs)
        self.print_there_are(what='harvest job', sequence=jobs)

    def run_harvester(self):
        context = {'model': model, 'user': self.admin_user['name'], 'session':model.Session}
        jobs = get_action('harvest_jobs_run')(context,{})

        #print 'Sent %s jobs to the gather queue' % len(jobs)

    def import_stage(self):
        if len(self.args) >= 2:
            source_id = unicode(self.args[1])
        else:
            source_id = None
        context = {'model': model, 'session':model.Session, 'user': self.admin_user['name']}
        objs = get_action('harvest_objects_import')(context,{'source_id':source_id})

        print '%s objects reimported' % len(objs)

    def create_harvest_job_all(self):
        context = {'model': model, 'user': self.admin_user['name'], 'session':model.Session}
        jobs = get_action('harvest_job_create_all')(context,{})
        print 'Created %s new harvest jobs' % len(jobs)

    def print_harvest_sources(self, sources):
        if sources:
            print ''
        for source in sources:
            self.print_harvest_source(source)

    def print_harvest_source(self, source):
        print 'Source id: %s' % source['id']
        print '      url: %s' % source['url']
        print '     type: %s' % source['type']
        print '   active: %s' % source['active']
        print '     user: %s' % source['user_id']
        print 'publisher: %s' % source['publisher_id']
        print '     jobs: %s' % source['status']['job_count']
        print ''

    def print_harvest_jobs(self, jobs):
        if jobs:
            print ''
        for job in jobs:
            self.print_harvest_job(job)

    def print_harvest_job(self, job):
        print '       Job id: %s' % job['id']
        print '       status: %s' % job['status']
        print '       source: %s' % job['source']
        print '      objects: %s' % len(job['objects'])

        print 'gather_errors: %s' % len(job['gather_errors'])
        if (len(job['gather_errors']) > 0):
            for error in job['gather_errors']:
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

