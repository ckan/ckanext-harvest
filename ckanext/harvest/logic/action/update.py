import hashlib

import logging

from ckan.plugins import PluginImplementations
from ckanext.harvest.interfaces import IHarvester

from ckan.model import Package

from ckan.logic import NotFound, ValidationError, check_access
from ckan.lib.navl.dictization_functions import validate

from ckanext.harvest.queue import get_gather_publisher

from ckanext.harvest.model import (HarvestSource, HarvestJob, HarvestObject)
from ckanext.harvest.logic.schema import default_harvest_source_schema
from ckanext.harvest.logic.dictization import (harvest_source_dictize,harvest_object_dictize)

from ckanext.harvest.logic.action.create import _error_summary
from ckanext.harvest.logic.action.get import harvest_source_show,harvest_job_list


log = logging.getLogger(__name__)

def harvest_source_update(context,data_dict):

    check_access('harvest_source_update',context,data_dict)

    model = context['model']
    session = context['session']

    source_id = data_dict.get('id')
    schema = context.get('schema') or default_harvest_source_schema()

    log.info('Harvest source %s update: %r', source_id, data_dict)
    source = HarvestSource.get(source_id)
    if not source:
        log.error('Harvest source %s does not exist', source_id)
        raise NotFound('Harvest source %s does not exist' % source_id)

    data, errors = validate(data_dict, schema)

    if errors:
        session.rollback()
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
        log.info('Harvest source %s not active, so aborting %i outstanding jobs', source_id, jobs.count())
        if jobs:
            for job in jobs:
                job.status = u'Aborted'
                job.save()

    return harvest_source_dictize(source,context)

def harvest_objects_import(context,data_dict):
    '''
        Reimports the current harvest objects
        It performs the import stage with the last fetched objects, optionally
        belonging to a certain source.
        Please note that no objects will be fetched from the remote server.
        It will only affect the last fetched objects already present in the
        database.
    '''
    log.info('Harvest objects import: %r', data_dict)
    check_access('harvest_objects_import',context,data_dict)

    model = context['model']
    session = context['session']
    source_id = data_dict.get('source_id',None)

    segments = context.get('segments',None)

    join_datasets = context.get('join_datasets',True)

    if source_id:
        source = HarvestSource.get(source_id)
        if not source:
            log.error('Harvest source %s does not exist', source_id)
            raise NotFound('Harvest source %s does not exist' % source_id)

        if not source.active:
            log.warn('Harvest source %s is not active.', source_id)
            raise Exception('This harvest source is not active')

        last_objects_ids = session.query(HarvestObject.id) \
                .join(HarvestSource) \
                .filter(HarvestObject.source==source) \
                .filter(HarvestObject.current==True)

    else:
        last_objects_ids = session.query(HarvestObject.id) \
                .filter(HarvestObject.current==True) \

    if join_datasets:
        last_objects_ids = last_objects_ids.join(Package) \
            .filter(Package.state==u'active')

    last_objects_ids = last_objects_ids.all()

    last_objects_count = 0

    for obj_id in last_objects_ids:
        if segments and str(hashlib.md5(obj_id[0]).hexdigest())[0] not in segments:
            continue

        obj = session.query(HarvestObject).get(obj_id)

        for harvester in PluginImplementations(IHarvester):
            if harvester.info()['name'] == obj.source.type:
                if hasattr(harvester,'force_import'):
                    harvester.force_import = True
                harvester.import_stage(obj)
                break
        last_objects_count += 1
    log.info('Harvest objects imported: %s', last_objects_count)
    return last_objects_count

def harvest_jobs_run(context,data_dict):
    log.info('Harvest job run: %r', data_dict)
    check_access('harvest_jobs_run',context,data_dict)

    source_id = data_dict.get('source_id',None)

    # Check if there are pending harvest jobs
    jobs = harvest_job_list(context,{'source_id':source_id,'status':u'New'})
    if len(jobs) == 0:
        log.info('No new harvest jobs.')
        raise Exception('There are no new harvesting jobs')

    # Send each job to the gather queue
    publisher = get_gather_publisher()
    sent_jobs = []
    for job in jobs:
        context['detailed'] = False
        source = harvest_source_show(context,{'id':job['source']})
        if source['active']:
            publisher.send({'harvest_job_id': job['id']})
            log.info('Sent job %s to the gather queue' % job['id'])
            sent_jobs.append(job)

    publisher.close()
    return sent_jobs

