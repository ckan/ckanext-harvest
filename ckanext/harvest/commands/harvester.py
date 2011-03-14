import sys
import re
from pprint import pprint

from ckan.lib.cli import CkanCommand
from ckan.model import repo
from ckanext.harvest.model import HarvestSource, HarvestingJob, HarvestedDocument

class Harvester(CkanCommand):
    '''Harvests remotely mastered metadata

    Usage:
      harvester source {url} [{user-ref} [{publisher-ref}]]     
        - create new harvest source

      harvester rmsource {url}
        - remove a harvester source (and associated jobs)

      harvester sources                                 
        - lists harvest sources

      harvester job {source-id} [{user-ref}]
        - create new harvesting job

      harvester rmjob {job-id}
        - remove a harvesting job
  
      harvester jobs
        - lists harvesting jobs

      harvester run
        - runs harvesting jobs
        
    The commands should be run from the ckanext-harvest directory and expect
    a development.ini file to be present. Most of the time you will
    specify the config explicitly though::

        paster harvester sources --config=../ckan/development.ini

    '''

    summary = __doc__.split('\n')[0]
    usage = __doc__
    max_args = 4
    min_args = 0

    def command(self):
        self._load_config()
        # Clear the 'No handlers could be found for logger "vdm"' warning message.
        print ""

        if len(self.args) == 0:
            self.parser.print_usage()
            sys.exit(1)
        cmd = self.args[0]
        if cmd == 'source':
            if len(self.args) >= 2:
                url = unicode(self.args[1])
            else:
                print self.usage
                print 'Error, source url is not given.'
                sys.exit(1)
            if len(self.args) >= 3:
                user_ref = unicode(self.args[2])
            else:
                user_ref = u''
            if len(self.args) >= 4:
                publisher_ref = unicode(self.args[3])
            else:
                publisher_ref = u''
            self.register_harvest_source(url, user_ref, publisher_ref)
        elif cmd == "rmsource":
            url = unicode(self.args[1])
            self.remove_harvest_source(url)
        elif cmd == 'sources':
            self.list_harvest_sources()
        elif cmd == 'job':
            if len(self.args) >= 2:
                source_id = unicode(self.args[1])
            else:
                print self.usage
                print 'Error, job source is not given.'
                sys.exit(1)
            if len(self.args) >= 3:
                user_ref = unicode(self.args[2])
            else:
                user_ref = u''
            self.register_harvesting_job(source_id, user_ref)
        elif cmd == "rmjob":
            job_id = unicode(self.args[1])
            self.remove_harvesting_job(job_id)
        elif cmd == 'jobs':
            self.list_harvesting_jobs()
        elif cmd == 'run':
            self.run_harvester()
        else:
            print 'Command %s not recognized' % cmd

    def _load_config(self):
        super(Harvester, self)._load_config()
        import logging
        logging.basicConfig()
        logger_vdm = logging.getLogger('vdm')
        logger_vdm.setLevel(logging.ERROR)

    def run_harvester(self, *args, **kwds):
        from pylons.i18n.translation import _get_translator
        import pylons
        pylons.translator._push_object(_get_translator(pylons.config.get('lang')))

        from ckan.controllers.harvesting import HarvestingJobController
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

    def remove_harvesting_job(self, job_id):
        try:
            job = HarvestingJob.get(job_id)
            job.delete()
            repo.commit_and_remove()
            print "Removed job: %s" % job_id
        except:
            print "No such job"

    def register_harvesting_job(self, source_id, user_ref):
        if re.match('(http|file)://', source_id):
            source_url = unicode(source_id)
            source_id = None
            sources = HarvestSource.filter(url=source_url).all()
            if sources:
                source = sources[0]
            else:
                source = self.create_harvest_source(url=source_url, user_ref=user_ref, publisher_ref=u'')
        else:
            source = HarvestSource.get(source_id)
        objects = HarvestingJob.filter(status='New', source=source)
        if objects.count():
            raise Exception('There is already an unrun job for the harvest source %r'%source.id)
        job = HarvestingJob(
            source=source,
            user_ref=user_ref,
            status=u"New",
        )
        job.save()
        print "Created new harvesting job:"
        self.print_harvesting_job(job)
        status = u"New"
        jobs = HarvestingJob.filter(status=status).all()
        self.print_there_are("harvesting job", jobs, condition=status)

    def register_harvest_source(self, url, user_ref, publisher_ref):
        existing = self.get_harvest_sources(url=url)
        if existing:
            print "Error, there is already a harvesting source for that URL"
            self.print_harvest_sources(existing)
            sys.exit(1)
        else:
            source = self.create_harvest_source(url=url, user_ref=user_ref, publisher_ref=publisher_ref)
            self.register_harvesting_job(source.id, user_ref)
            print "Created new harvest source:"
            self.print_harvest_source(source)
            sources = self.get_harvest_sources()
            self.print_there_are("harvest source", sources)

    def remove_harvest_source(self, url):
        repo.new_revision()
        sources = HarvestSource.filter(url=url)
        if sources.count() == 0:
            print "No such source"
        else:
            source = sources[0]
            jobs = HarvestingJob.filter(source=source)
            print "Removing %d jobs" % jobs.count()
            for job in jobs:
                job.delete()
            source.delete()
            repo.commit_and_remove()
            print "Removed harvest source: %s" % url

    def list_harvest_sources(self):
        sources = self.get_harvest_sources()
        self.print_harvest_sources(sources)
        self.print_there_are(what="harvest source", sequence=sources)
       
    def list_harvesting_jobs(self):
        jobs = self.get_harvesting_jobs()
        self.print_harvesting_jobs(jobs)
        self.print_there_are(what="harvesting job", sequence=jobs)

    def get_harvest_sources(self, **kwds):
        return HarvestSource.filter(**kwds).all()

    def get_harvesting_jobs(self, **kwds):
        return HarvestingJob.filter(**kwds).all()

    def create_harvest_source(self, **kwds):
        source = HarvestSource(**kwds)
        source.save()
        return source

    def create_harvesting_job(self, **kwds):
        job = HarvestingJob(**kwds)
        job.save()
        return job

    def print_harvest_sources(self, sources):
        if sources:
            print ""
        for source in sources:
            self.print_harvest_source(source)

    def print_harvest_source(self, source):
        print "Source id: %s" % source.id
        print "      url: %s" % source.url
        print "     user: %s" % source.user_ref
        print "publisher: %s" % source.publisher_ref
        print "     docs: %s" % len(source.documents)
        print ""

    def print_harvesting_jobs(self, jobs):
        if jobs:
            print ""
        for job in jobs:
            self.print_harvesting_job(job)

    def print_harvesting_job(self, job):
        print "Job id: %s" % job.id
        if job.user_ref:
            print "  user: %s" % job.user_ref
        print "status: %s" % job.status
        print "source: %s" % job.source.id
        print "   url: %s" % job.source.url
        #print "report: %s" % job.report
        if job.report and job.report['added']:
            for package_id in job.report['added']:
                print "   doc: %s" % package_id
        if job.report and job.report['errors']:
            for msg in job.report['errors']:
                print " error: %s" % msg
        print ""

    def print_there_are(self, what, sequence, condition=""):
        is_singular = self.is_singular(sequence)
        print "There %s %s %s%s%s" % (
            is_singular and "is" or "are",
            len(sequence),
            condition and ("%s " % condition.lower()) or "",
            what,
            not is_singular and "s" or "",
        )

    def is_singular(self, sequence):
        return len(sequence) == 1

