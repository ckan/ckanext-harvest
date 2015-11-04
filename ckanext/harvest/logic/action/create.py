import logging

import ckan

from ckan.plugins import toolkit

from ckanext.harvest.logic import HarvestJobExists
from ckanext.harvest.plugin import DATASET_TYPE_NAME
from ckanext.harvest.model import (HarvestSource, HarvestJob, HarvestObject,
    HarvestObjectExtra)
from ckanext.harvest.logic.dictization import (harvest_job_dictize,
    harvest_object_dictize)
from ckanext.harvest.logic.schema import (harvest_source_show_package_schema,
    harvest_object_create_schema)
from ckanext.harvest.logic.action.get import harvest_source_list,harvest_job_list

log = logging.getLogger(__name__)

_validate = ckan.lib.navl.dictization_functions.validate
check_access = toolkit.check_access


class InactiveSource(Exception):
    pass

def harvest_source_create(context,data_dict):
    '''
    Creates a new harvest source

    This method just proxies the request to package_create,
    which will create a harvest_source dataset type and the
    HarvestSource object. All auth checks and validation will
    be done there .We only make sure to set the dataset type.

    Note that the harvest source type (ckan, waf, csw, etc)
    is now set via the source_type field.

    :param url: the URL for the harvest source
    :type url: string
    :param name: the name of the new harvest source, must be between 2 and 100
        characters long and contain only lowercase alphanumeric characters
    :type name: string
    :param title: the title of the dataset (optional, default: same as
        ``name``)
    :type title: string
    :param notes: a description of the harvest source (optional)
    :type notes: string
    :param source_type: the harvester type for this source. This must be one
        of the registerd harvesters, eg 'ckan', 'csw', etc.
    :type source_type: string
    :param frequency: the frequency in wich this harvester should run. See
        ``ckanext.harvest.model`` source for possible values. Default is
        'MANUAL'
    :type frequency: string
    :param config: extra configuration options for the particular harvester
        type. Should be a serialized as JSON. (optional)
    :type config: string


    :returns: the newly created harvest source
    :rtype: dictionary
    '''

    log.info('Creating harvest source: %r', data_dict)

    data_dict['type'] = DATASET_TYPE_NAME

    context['extras_as_string'] = True
    source = toolkit.get_action('package_create')(context, data_dict)

    return source


def harvest_job_create(context, data_dict):
    '''
    Creates a Harvest Job for a Harvest Source and runs it (by putting it on
    the gather queue)

    :param source_id:
    :type param: string
    :param run: whether to also run it or not (default: True)
    :type run: bool
    '''
    log.info('Harvest job create: %r', data_dict)
    check_access('harvest_job_create', context, data_dict)

    source_id = data_dict['source_id']
    run_it = data_dict.get('run', True)

    # Check if source exists
    source = HarvestSource.get(source_id)
    if not source:
        log.warn('Harvest source %s does not exist', source_id)
        raise toolkit.NotFound('Harvest source %s does not exist' % source_id)

    # Check if the source is active
    if not source.active:
        log.warn('Harvest job cannot be created for inactive source %s',
                 source_id)
        raise Exception('Can not create jobs on inactive sources')

    # Check if there already is an unrun or currently running job for this
    # source
    exists = _check_for_existing_jobs(context, source_id)
    if exists:
        log.warn('There is already an unrun job %r for this source %s',
                 exists, source_id)
        raise HarvestJobExists('There already is an unrun job for this source')

    job = HarvestJob()
    job.source = source
    job.save()
    log.info('Harvest job saved %s', job.id)

    if run_it:
        toolkit.get_action('harvest_send_job_to_gather_queue')(
            context, {'id': job.id})

    return harvest_job_dictize(job, context)


def harvest_job_create_all(context, data_dict):
    '''
    Creates a Harvest Job for all Harvest Sources and runs them (by
    putting them on the gather queue)

    :param source_id:
    :type param: string
    :param run: whether to also run the jobs or not (default: True)
    :type run: bool
    '''

    log.info('Harvest job create all: %r', data_dict)
    check_access('harvest_job_create_all',context,data_dict)

    run = data_dict.get('run', True)

    data_dict.update({'only_active':True})

    # Get all active sources
    sources = harvest_source_list(context,data_dict)
    jobs = []
    # Create a new job for each, if there isn't already one
    for source in sources:
        exists = _check_for_existing_jobs(context, source['id'])
        if exists:
            log.info('Skipping source %s as it already has a pending job', source['id'])
            continue

        job = harvest_job_create(
            context, {'source_id': source['id'], 'run': run})
        jobs.append(job)

    log.info('Created jobs for %s%i harvest sources',
             'and run ' if run else '', len(jobs))
    return jobs

def _check_for_existing_jobs(context, source_id):
    '''
    Given a source id, checks if there are jobs for this source
    with status 'New' or 'Running'

    rtype: boolean
    '''
    data_dict ={
        'source_id':source_id,
        'status':u'New'
    }
    exist_new = harvest_job_list(context,data_dict)
    data_dict ={
        'source_id':source_id,
        'status':u'Running'
    }
    exist_running = harvest_job_list(context,data_dict)
    exist = len(exist_new + exist_running) > 0

    return exist

def harvest_object_create(context, data_dict):
    ''' Create a new harvest object

    :type guid: string (optional)
    :type content: string (optional)
    :type job_id: string
    :type source_id: string (optional)
    :type package_id: string (optional)
    :type extras: dict (optional)
    '''
    check_access('harvest_object_create', context, data_dict)
    data, errors = _validate(data_dict, harvest_object_create_schema(), context)

    if errors:
        raise toolkit.ValidationError(errors)

    obj = HarvestObject(
        guid=data.get('guid'),
        content=data.get('content'),
        job=data['job_id'],
        harvest_source_id=data.get('source_id'),
        package_id=data.get('package_id'),
        extras=[ HarvestObjectExtra(key=k, value=v) 
            for k, v in data.get('extras', {}).items() ]
    )

    obj.save()
    return harvest_object_dictize(obj, context)
