import urlparse
import re

from sqlalchemy import distinct,func
from ckan.model import Session, repo
from ckan.model import Package
from ckan.lib.navl.dictization_functions import validate
from ckan.logic import NotFound, ValidationError

from ckanext.harvest.logic.schema import harvest_source_form_schema

from ckan.plugins import PluginImplementations
from ckanext.harvest.model import HarvestSource, HarvestJob, HarvestObject, \
                                  HarvestGatherError, HarvestObjectError
from ckanext.harvest.queue import get_gather_publisher
from ckanext.harvest.interfaces import IHarvester

import logging
log = logging.getLogger('ckanext')


def _get_source_status(source, detailed=True):
    out = dict()
    job_count = HarvestJob.filter(source=source).count()
    if not job_count:
        out['msg'] = 'No jobs yet'
        return out
    out = {'next_harvest':'',
           'last_harvest_request':'',
           'last_harvest_statistics':{'added':0,'updated':0,'errors':0},
           'last_harvest_errors':{'gather':[],'object':[]},
           'overall_statistics':{'added':0, 'errors':0},
           'packages':[]}
    # Get next scheduled job
    next_job = HarvestJob.filter(source=source,status=u'New').first()
    if next_job:
        out['next_harvest'] = 'Within 15 minutes'
    else:
        out['next_harvest'] = 'Not yet scheduled'

    # Get the last finished job
    last_job = HarvestJob.filter(source=source,status=u'Finished') \
               .order_by(HarvestJob.created.desc()).first()

    if last_job:
        #TODO: Should we encode the dates as strings?
        out['last_harvest_request'] = str(last_job.gather_finished)

        #Get HarvestObjects from last job whit links to packages
        if detailed: 
            last_objects = [obj for obj in last_job.objects if obj.package is not None]

            if len(last_objects) == 0:
                # No packages added or updated
                out['last_harvest_statistics']['added'] = 0
                out['last_harvest_statistics']['updated'] = 0
            else:
                # Check wether packages were added or updated
                for last_object in last_objects:
                    # Check if the same package had been linked before
                    previous_objects = Session.query(HarvestObject) \
                                             .filter(HarvestObject.package==last_object.package) \
                                             .count()

                    if previous_objects == 1:
                        # It didn't previously exist, it has been added
                        out['last_harvest_statistics']['added'] += 1
                    else:
                        # Pacakge already existed, but it has been updated
                        out['last_harvest_statistics']['updated'] += 1

        # Last harvest errors
        # We have the gathering errors in last_job.gather_errors, so let's also
        # get also the object errors.
        object_errors = Session.query(HarvestObjectError).join(HarvestObject) \
                            .filter(HarvestObject.job==last_job)

        out['last_harvest_statistics']['errors'] = len(last_job.gather_errors) \
                                            + object_errors.count()
        if detailed: 
            for gather_error in last_job.gather_errors:
                out['last_harvest_errors']['gather'].append(gather_error.message)

            for object_error in object_errors:
                err = {'object_id':object_error.object.id,'object_guid':object_error.object.guid,'message': object_error.message}
                out['last_harvest_errors']['object'].append(err)

        # Overall statistics
        packages = Session.query(distinct(HarvestObject.package_id),Package.name) \
                .join(Package).join(HarvestJob).join(HarvestSource) \
                .filter(HarvestJob.source==source)

        out['overall_statistics']['added'] = packages.count()
        if detailed:
            for package in packages:
                out['packages'].append(package.name)

        gather_errors = Session.query(HarvestGatherError) \
                .join(HarvestJob).join(HarvestSource) \
                .filter(HarvestJob.source==source).count()

        object_errors = Session.query(HarvestObjectError) \
                .join(HarvestObject).join(HarvestJob).join(HarvestSource) \
                .filter(HarvestJob.source==source).count()
        out['overall_statistics']['errors'] = gather_errors + object_errors
    else:
        out['last_harvest_request'] = 'Not yet harvested'

    return out




def _source_as_dict(source, detailed=True):
    out = source.as_dict()
    out['jobs'] = []

    for job in source.jobs:
        out['jobs'].append(job.as_dict())

    out['status'] = _get_source_status(source, detailed=detailed)


    return out

def _job_as_dict(job):
    out = job.as_dict()
    out['source'] = job.source.as_dict()
    out['objects'] = []
    out['gather_errors'] = []

    for obj in job.objects:
        out['objects'].append(obj.as_dict())

    for error in job.gather_errors:
        out['gather_errors'].append(error.as_dict())

    return out

def _object_as_dict(obj):
    out = obj.as_dict()
    out['source'] = obj.source.as_dict()
    out['job'] = obj.job.as_dict()

    if obj.package:
        out['package'] = obj.package.as_dict()

    out['errors'] = []

    for error in obj.errors:
        out['errors'].append(error.as_dict())

    return out

def _url_exists(url):
    new_url = _normalize_url(url)

    existing_sources = get_harvest_sources()

    for existing_source in existing_sources:
        existing_url = _normalize_url(existing_source['url'])
        if existing_url == new_url and existing_source['active'] == True:
            return existing_source
    return False

def _normalize_url(url):
    o = urlparse.urlparse(url)

    # Normalize port
    if ':' in o.netloc:
        parts = o.netloc.split(':')
        if (o.scheme == 'http' and parts[1] == '80') or \
           (o.scheme == 'https' and parts[1] == '443'):
            netloc = parts[0]
        else:
            netloc = ':'.join(parts)
    else:
        netloc = o.netloc

    # Remove trailing slash
    path = o.path.rstrip('/')

    check_url = urlparse.urlunparse((
            o.scheme,
            netloc,
            path,
            None,None,None))

    return check_url

def _prettify(field_name):
    field_name = re.sub('(?<!\w)[Uu]rl(?!\w)', 'URL', field_name.replace('_', ' ').capitalize())
    return field_name.replace('_', ' ')

def _error_summary(error_dict):
    error_summary = {}
    for key, error in error_dict.iteritems():
        error_summary[_prettify(key)] = error[0]
    return error_summary

def get_harvest_source(id,attr=None):
    source = HarvestSource.get(id,attr=attr)

    if not source:
        raise NotFound

    return _source_as_dict(source)

def get_harvest_sources(**kwds):
    sources = HarvestSource.filter(**kwds) \
                .order_by(HarvestSource.created.desc()) \
                .all()
    return [_source_as_dict(source, detailed=False) for source in sources]

def create_harvest_source(data_dict):

    schema = harvest_source_form_schema()
    data, errors = validate(data_dict, schema)

    if errors:
        Session.rollback()
        raise ValidationError(errors,_error_summary(errors))

    source = HarvestSource()
    source.url = data['url']
    source.type = data['type']

    opt = ['active','description','user_id','publisher_id','config']
    for o in opt:
        if o in data and data[o] is not None:
            source.__setattr__(o,data[o])

    source.save()

    return _source_as_dict(source)

def edit_harvest_source(source_id,data_dict):
    schema = harvest_source_form_schema()

    source = HarvestSource.get(source_id)
    if not source:
        raise NotFound('Harvest source %s does not exist' % source_id)

    # Add source id to the dict, as some validators will need it
    data_dict['id'] = source.id

    data, errors = validate(data_dict, schema)
    if errors:
        Session.rollback()
        raise ValidationError(errors,_error_summary(errors))

    fields = ['url','type','active','description','user_id','publisher_id','config']
    for f in fields:
        if f in data_dict and data_dict[f] is not None and data_dict[f] != '':
            source.__setattr__(f,data_dict[f])

    source.save()

    return _source_as_dict(source)


def remove_harvest_source(source_id):

    source = HarvestSource.get(source_id)
    if not source:
        raise NotFound('Harvest source %s does not exist' % source_id)

    # Don't actually delete the record, just flag it as inactive
    source.active = False
    source.save()

    # Abort any pending jobs
    jobs = HarvestJob.filter(source=source,status=u'New')
    if jobs:
        for job in jobs:
            job.status = u'Aborted'
            job.save()

    return True

def get_harvest_job(id,attr=None):
    job = HarvestJob.get(id,attr=attr)
    if not job:
        raise NotFound

    return _job_as_dict(job)

def get_harvest_jobs(**kwds):
    jobs = HarvestJob.filter(**kwds).all()
    return [_job_as_dict(job) for job in jobs]

def create_harvest_job(source_id):
    # Check if source exists
    source = HarvestSource.get(source_id)
    if not source:
        raise NotFound('Harvest source %s does not exist' % source_id)

    # Check if the source is active
    if not source.active:
        raise Exception('Can not create jobs on inactive sources')

    # Check if there already is an unrun job for this source
    exists = get_harvest_jobs(source=source,status=u'New')
    if len(exists):
        raise Exception('There already is an unrun job for this source')

    job = HarvestJob()
    job.source = source

    job.save()

    return _job_as_dict(job)

def run_harvest_jobs():
    # Check if there are pending harvest jobs
    jobs = get_harvest_jobs(status=u'New')
    if len(jobs) == 0:
        raise Exception('There are no new harvesting jobs')

    # Send each job to the gather queue
    publisher = get_gather_publisher()
    sent_jobs = []
    for job in jobs:
        if job['source']['active']:
            publisher.send({'harvest_job_id': job['id']})
            log.info('Sent job %s to the gather queue' % job['id'])
            sent_jobs.append(job)

    publisher.close()
    return sent_jobs

def get_harvest_object(id,attr=None):
    obj = HarvestObject.get(id,attr=attr)
    if not obj:
        raise NotFound

    return _object_as_dict(obj)

def get_harvest_objects(**kwds):
    objects = HarvestObject.filter(**kwds).all()
    return [_object_as_dict(obj) for obj in objects]

def import_last_objects(source_id=None):
    if source_id:
        source = HarvestSource.get(source_id)
        if not source:
            raise NotFound('Harvest source %s does not exist' % source_id)

        last_objects_ids = Session.query(HarvestObject.id) \
                .join(HarvestJob) \
                .filter(HarvestJob.source==source) \
                .filter(HarvestObject.package!=None) \
                .order_by(HarvestObject.guid) \
                .order_by(HarvestObject.metadata_modified_date.desc()) \
                .order_by(HarvestObject.gathered.desc()) \
                .all()
    else:
        last_objects_ids = Session.query(HarvestObject.id) \
                .filter(HarvestObject.package!=None) \
                .order_by(HarvestObject.guid) \
                .order_by(HarvestObject.metadata_modified_date.desc()) \
                .order_by(HarvestObject.gathered.desc()) \
                .all()


    last_obj_guid = ''
    imported_objects = []
    for obj_id in last_objects_ids:
        obj = Session.query(HarvestObject).get(obj_id)
        if obj.guid != last_obj_guid:
            imported_objects.append(obj)
            for harvester in PluginImplementations(IHarvester):
                if harvester.info()['name'] == obj.job.source.type:
                    if hasattr(harvester,'force_import'):
                        harvester.force_import = True
                    harvester.import_stage(obj)
                    break
        last_obj_guid = obj.guid

    return imported_objects

def create_harvest_job_all():
    
    # Get all active sources
    sources = get_harvest_sources(active=True)
    jobs = []
    # Create a new job for each
    for source in sources:
        job = create_harvest_job(source['id'])
        jobs.append(job)

    return jobs

def get_registered_harvesters_info():
    available_harvesters = []
    for harvester in PluginImplementations(IHarvester):
        info = harvester.info()
        if not info or 'name' not in info:
            log.error('Harvester %r does not provide the harvester name in the info response' % str(harvester))
            continue
        info['show_config'] = (info.get('form_config_interface','') == 'Text')
        available_harvesters.append(info)

    return available_harvesters
