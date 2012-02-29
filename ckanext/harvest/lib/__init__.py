import urlparse
import re

from ckan import model
from ckan.model import Session, repo
from ckan.model import Package
from ckan.lib.navl.dictization_functions import validate
from ckan.logic import NotFound, ValidationError

from ckanext.harvest.logic.schema import harvest_source_form_schema
from ckanext.harvest.logic.dictization import (harvest_source_dictize, harvest_job_dictize, harvest_object_dictize)
from ckan.plugins import PluginImplementations
from ckanext.harvest.model import HarvestSource, HarvestJob, HarvestObject, \
                                  HarvestGatherError, HarvestObjectError
from ckanext.harvest.queue import get_gather_publisher
from ckanext.harvest.interfaces import IHarvester

import logging
log = logging.getLogger('ckanext')

#TODO: remove!
context = {'model':model}

def create_harvest_source(data_dict):

    schema = harvest_source_form_schema()
    data, errors = validate(data_dict, schema)

    if errors:
        Session.rollback()
        raise ValidationError(errors,_error_summary(errors))

    source = HarvestSource()
    source.url = data['url']
    source.type = data['type']

    opt = ['active','title','description','user_id','publisher_id','config']
    for o in opt:
        if o in data and data[o] is not None:
            source.__setattr__(o,data[o])

    if 'active' in data_dict:
        source.active = data['active']

    source.save()

    return harvest_source_dictize(source,context)

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

    fields = ['url','title','type','description','user_id','publisher_id']
    for f in fields:
        if f in data and data[f] is not None:
            source.__setattr__(f,data[f])

    if 'active' in data_dict:
        source.active = data['active']

    if 'config' in data_dict:
        source.config = data['config']

    source.save()
    # Abort any pending jobs
    if not source.active:
        jobs = HarvestJob.filter(source=source,status=u'New')
        if jobs:
            for job in jobs:
                job.status = u'Aborted'
                job.save()

    return harvest_source_dictize(source,context)


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

def create_harvest_job(source_id):
    # Check if source exists
    source = HarvestSource.get(source_id)
    if not source:
        raise NotFound('Harvest source %s does not exist' % source_id)

    # Check if the source is active
    if not source.active:
        raise Exception('Can not create jobs on inactive sources')

    # Check if there already is an unrun job for this source
    exists = get_action('harvest_job_list')(context,{'status':u'New'})
    if len(exists):
        raise Exception('There already is an unrun job for this source')

    job = HarvestJob()
    job.source = source

    job.save()
    return harvest_job_dictize(job,context)

def run_harvest_jobs():
    # Check if there are pending harvest jobs
    jobs = get_action('harvest_job_list')(context,{'status':u'New'})
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

def import_last_objects(source_id=None):
    if source_id:
        source = HarvestSource.get(source_id)
        if not source:
            raise NotFound('Harvest source %s does not exist' % source_id)

        if not source.active:
            raise Exception('This harvest source is not active')

        last_objects_ids = Session.query(HarvestObject.id) \
                .join(HarvestSource).join(Package) \
                .filter(HarvestObject.source==source) \
                .filter(HarvestObject.current==True) \
                .filter(Package.state==u'active') \
                .all()
    else:
        last_objects_ids = Session.query(HarvestObject.id) \
                .join(Package) \
                .filter(HarvestObject.current==True) \
                .filter(Package.state==u'active') \
                .all()

    last_objects = []
    for obj_id in last_objects_ids:
        obj = Session.query(HarvestObject).get(obj_id)
        for harvester in PluginImplementations(IHarvester):
            if harvester.info()['name'] == obj.source.type:
                if hasattr(harvester,'force_import'):
                    harvester.force_import = True
                harvester.import_stage(obj)
                break
        last_objects.append(obj)
    return last_objects

def create_harvest_job_all():

    # Get all active sources
    sources = harvest_sources_list(active=True)
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
