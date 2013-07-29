import logging

from ckan import logic

from ckan.logic import NotFound, check_access
from ckanext.harvest.logic import HarvestJobExists

from ckanext.harvest.plugin import DATASET_TYPE_NAME
from ckanext.harvest.model import (HarvestSource, HarvestJob)
from ckanext.harvest.logic.dictization import harvest_job_dictize
from ckanext.harvest.logic.schema import harvest_source_show_package_schema
from ckanext.harvest.logic.action.get import harvest_source_list,harvest_job_list

log = logging.getLogger(__name__)

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
    package_dict = logic.get_action('package_create')(context, data_dict)

    context['schema'] = harvest_source_show_package_schema()
    source = logic.get_action('package_show')(context, package_dict)

    return source


def harvest_job_create(context,data_dict):
    log.info('Harvest job create: %r', data_dict)
    check_access('harvest_job_create',context,data_dict)

    source_id = data_dict['source_id']

    # Check if source exists
    source = HarvestSource.get(source_id)
    if not source:
        log.warn('Harvest source %s does not exist', source_id)
        raise NotFound('Harvest source %s does not exist' % source_id)

    # Check if the source is active
    if not source.active:
        log.warn('Harvest job cannot be created for inactive source %s', source_id)
        raise Exception('Can not create jobs on inactive sources')

    # Check if there already is an unrun or currently running job for this source
    exists = _check_for_existing_jobs(context, source_id)
    if exists:
        log.warn('There is already an unrun job %r for this source %s', exists, source_id)
        raise HarvestJobExists('There already is an unrun job for this source')

    job = HarvestJob()
    job.source = source

    job.save()
    log.info('Harvest job saved %s', job.id)
    return harvest_job_dictize(job,context)

def harvest_job_create_all(context,data_dict):
    log.info('Harvest job create all: %r', data_dict)
    check_access('harvest_job_create_all',context,data_dict)

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

        job = harvest_job_create(context,{'source_id':source['id']})
        jobs.append(job)

    log.info('Created jobs for %i harvest sources', len(jobs))
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
