import sys
import re
#import logging
from pprint import pprint

from ckan.lib.cli import CkanCommand
from ckan.model import repo
from ckanext.harvest.lib import *
from ckanext.harvest.queue import get_gather_consumer, get_fetch_consumer
#log = logging.getLogger(__name__)
#log.setLevel(logging.DEBUG)

class Harvester(CkanCommand):
    '''Harvests remotely mastered metadata

    Usage:
      harvester source {url} {type} [{active}] [{user-id}] [{publisher-id}] 
        - create new harvest source

      harvester rmsource {id}
        - remove a harvester source (and associated jobs)

      harvester sources                                 
        - lists harvest sources

      harvester job {source-id}
        - create new harvest job

      harvester rmjob {job-id}
        - remove a harvest job
  
      harvester jobs
        - lists harvest jobs

      harvester run
        - runs harvest jobs

      harvester gather_consumer
        - starts the consumer for the gathering queue

      harvester fetch_consumer
        - starts the consumer for the fetching queue

      harvester extents
        - creates or updates the extent geometry column for packages with
          a bounding box defined in extras
       
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
        print ''

        if len(self.args) == 0:
            self.parser.print_usage()
            sys.exit(1)
        cmd = self.args[0]
        #log.info(cmd)
        #sys.exit(0)
        if cmd == 'source':
            self.create_harvest_source()            
        elif cmd == "rmsource":
            self.remove_harvest_source()
        elif cmd == 'sources':
            self.list_harvest_sources()
        elif cmd == 'job':
            self.create_harvest_job()
        elif cmd == "rmjob":
            self.remove_harvest_job()
        elif cmd == 'jobs':
            self.list_harvest_jobs()
        elif cmd == 'run':
            self.run_harvester()
        elif cmd == 'extents':
            self.update_extents()
        elif cmd == 'gather_consumer':
            consumer = get_gather_consumer()
            consumer.wait()
        elif cmd == 'fetch_consumer':
            consumer = get_fetch_consumer()
            consumer.wait()

        else:
            print 'Command %s not recognized' % cmd

    def _load_config(self):
        super(Harvester, self)._load_config()

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
            active = not(self.args[3].lower() == 'false' or \
                    self.args[3] == '0')
        else:
            active = True
        if len(self.args) >= 5:
            user_id = unicode(self.args[4])
        else:
            user_id = u''
        if len(self.args) >= 6:
            publisher_id = unicode(self.args[5])
        else:
            publisher_id = u''
        
        source = create_harvest_source({
                'url':url,
                'type':type,
                'active':active,
                'user_id':user_id, 
                'publisher_id':publisher_id})

        print 'Created new harvest source:'
        self.print_harvest_source(source)

        sources = get_harvest_sources()
        self.print_there_are('harvest source', sources)
        
        # Create a Harvest Job for the new Source
        create_harvest_job(source['id'])
        print 'A new Harvest Job for this source has also been created'

    def remove_harvest_source(self):
        if len(self.args) >= 2:
            source_id = unicode(self.args[1])
        else:
            print 'Please provide a source id'
            sys.exit(1)

        delete_harvest_source(source_id)
        print 'Removed harvest source: %s' % source_id
    
    def list_harvest_sources(self):
        sources = get_harvest_sources()
        self.print_harvest_sources(sources)
        self.print_there_are(what="harvest source", sequence=sources)

    def create_harvest_job(self):
        if len(self.args) >= 2:
            source_id = unicode(self.args[1])
        else:
            print 'Please provide a source id'
            sys.exit(1)

        job = create_harvest_job(source_id)

        self.print_harvest_job(job)
        status = u'New'
        jobs = get_harvest_jobs(status=status)
        self.print_there_are('harvest jobs', jobs, condition=status)

    def remove_harvest_job(self):
        if len(self.args) >= 2:
            job_id = unicode(self.args[1])
        else:
            print 'Please provide a job id'
            sys.exit(1)

        delete_harvest_job(job_id)
        print 'Removed harvest job: %s' % job_id

    def list_harvest_jobs(self):
        jobs = get_harvest_jobs()
        self.print_harvest_jobs(jobs)
        self.print_there_are(what='harvest job', sequence=jobs)
    
    def run_harvester(self, *args, **kwds):

        jobs = run_harvest_jobs()

        print 'Sent %s jobs to the gather queue' % len(jobs)

        sys.exit(0)
        
        
        #TODO: move all this stuff to ckanext-inspire
        from pylons.i18n.translation import _get_translator
        import pylons
        pylons.translator._push_object(_get_translator(pylons.config.get('lang')))

        from ckanext.harvest.controllers.harvesting import HarvestingJobController        
        from ckanext.csw.validation import Validator

        jobs = HarvestingJob.filter(status=u"New").all()
        jobs_len = len(jobs)
        jobs_count = 0
        if jobs_len:
            print "Running %s harvesting jobs..." % jobs_len
            profiles = [
                x.strip() for x in
                pylons.config.get(
                    "ckan.harvestor.validator.profiles", 
                    "iso19139,gemini2",
                ).split(",")
            ]
            validator = Validator(profiles=profiles)
            print ""
            for job in jobs:
                jobs_count += 1
                if job.source is None:
                    print 'ERRROR: no source associated with this job'
                else:
                    print "Running job %s/%s: %s" % (jobs_count, jobs_len, job.id)
                    self.print_harvesting_job(job)
                    job_controller = HarvestingJobController(job, validator)
                    job_controller.harvest_documents()
                    pprint (job.report)
        else:
            print "There are no new harvesting jobs."

    #TODO: move to ckanext-?? for geo stuff
    def update_extents(self):
        from ckan.model import PackageExtra, Package, Session
        conn = Session.connection()
        packages = [extra.package \
                    for extra in \
                    Session.query(PackageExtra).filter(PackageExtra.key == 'bbox-east-long').all()]

        error = False
        for package in packages:
            try:
                save_extent(package)
            except:
                errors = True
 
        if error:
            msg = "There was an error saving the package extent. Have you set up the package_extent table in the DB?"
        else:
            msg = "Done. Extents generated for %i packages" % len(packages)

        print msg

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
        print '     jobs: %s' % len(source['jobs'])
        print '  objects: %s' % len(source['objects'])
        print ''

    def print_harvest_jobs(self, jobs):
        if jobs:
            print ''
        for job in jobs:
            self.print_harvest_job(job)

    def print_harvest_job(self, job):
        print '       Job id: %s' % job['id']
        print '       status: %s' % job['status']
        print '       source: %s' % job['source']['id']
        print '          url: %s' % job['source']['url']
        print '      objects: %s' % len(job['objects'])

        print 'gather_errors: %s' % len(job['gather_errors'])
        if (len(job['gather_errors']) > 0):
            for error in job['gather_errors']:
                print '               %s' % error['message']

        print 'object_errors: %s' % len(job['object_errors'])
        if (len(job['object_errors']) > 0):
            for error in job['object_errors']:
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

