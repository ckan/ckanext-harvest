import hashlib

import logging
import datetime

from sqlalchemy import and_

from ckan.lib.search.index import PackageSearchIndex
from ckan.plugins import PluginImplementations
from ckan.logic import get_action
from ckanext.harvest.interfaces import IHarvester

from ckan.model import Package
from ckan import logic

from ckan.logic import NotFound, check_access

from ckanext.harvest.plugin import DATASET_TYPE_NAME
from ckanext.harvest.queue import get_gather_publisher

from ckanext.harvest.model import HarvestSource, HarvestJob, HarvestObject
from ckanext.harvest.logic import HarvestJobExists
from ckanext.harvest.logic.schema import harvest_source_db_to_form_schema


from ckanext.harvest.logic.action.get import harvest_source_show, harvest_job_list, _get_sources_for_user


log = logging.getLogger(__name__)

def harvest_source_update(context,data_dict):
    '''
    Updates an existing harvest source

    This method just proxies the request to package_update,
    which will create a harvest_source dataset type and the
    HarvestSource object. All auth checks and validation will
    be done there .We only make sure to set the dataset type

    Note that the harvest source type (ckan, waf, csw, etc)
    is now set via the source_type field.

    :param id: the name or id of the harvest source to update
    :type id: string
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
    log.info('Updating harvest source: %r', data_dict)

    data_dict['type'] = DATASET_TYPE_NAME

    context['extras_as_string'] = True
    package_dict = logic.get_action('package_update')(context, data_dict)

    context['schema'] = harvest_source_db_to_form_schema()
    source = logic.get_action('package_show')(context, package_dict)

    return source



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

def _caluclate_next_run(frequency):

    now = datetime.datetime.utcnow()
    if frequency == 'ALWAYS':
        return now
    if frequency == 'WEEKLY':
        return now + datetime.timedelta(weeks=1)
    if frequency == 'BIWEEKLY':
        return now + datetime.timedelta(weeks=2)
    if frequency == 'DAILY':
        return now + datetime.timedelta(days=1)
    if frequency == 'MONTHLY':
        if now.month in (4,6,9,11):
            days = 30
        elif now.month == 2:
            if now.year % 4 == 0:
                days = 29
            else:
                days = 28
        else:
            days = 31
        return now + datetime.timedelta(days=days)
    raise Exception('Frequency {freq} not recognised'.format(freq=frequency))


def _make_scheduled_jobs(context, data_dict):

    data_dict = {'only_to_run': True,
                 'only_active': True}
    sources = _get_sources_for_user(context, data_dict)

    for source in sources:
        data_dict = {'source_id': source.id}
        try:
            get_action('harvest_job_create')(context, data_dict)
        except HarvestJobExists, e:
            log.info('Trying to rerun job for %s skipping' % source.id)

        source.next_run = _caluclate_next_run(source.frequency)
        source.save()

def harvest_jobs_run(context,data_dict):
    log.info('Harvest job run: %r', data_dict)
    check_access('harvest_jobs_run',context,data_dict)

    session = context['session']

    source_id = data_dict.get('source_id',None)

    if not source_id:
        _make_scheduled_jobs(context, data_dict)

    context['return_objects'] = False

    # Flag finished jobs as such
    jobs = harvest_job_list(context,{'source_id':source_id,'status':u'Running'})
    if len(jobs):
        package_index = PackageSearchIndex()
        for job in jobs:
            if job['gather_finished']:
                objects = session.query(HarvestObject.id) \
                          .filter(HarvestObject.harvest_job_id==job['id']) \
                          .filter(and_((HarvestObject.state!=u'COMPLETE'),
                                       (HarvestObject.state!=u'ERROR'))) \
                          .order_by(HarvestObject.import_finished.desc())

                if objects.count() == 0:
                    job_obj = HarvestJob.get(job['id'])
                    job_obj.status = u'Finished'

                    last_object = session.query(HarvestObject) \
                          .filter(HarvestObject.harvest_job_id==job['id']) \
                          .order_by(HarvestObject.import_finished.desc()) \
                          .first()
                    if last_object:
                        job_obj.finished = last_object.import_finished
                    job_obj.save()
                    # Reindex the harvest source dataset so it has the latest
                    # status
                    if 'extras_as_string'in context:
                        del context['extras_as_string']
                    context.update({'validate': False, 'ignore_auth': True})
                    package_dict = logic.get_action('package_show')(context,
                            {'id': job_obj.source.id})

                    if package_dict:
                        package_index.index_package(package_dict)


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
        source = harvest_source_show(context,{'id':job['source_id']})
        if source['active']:
            job_obj = HarvestJob.get(job['id'])
            job_obj.status = job['status'] = u'Running'
            job_obj.save()
            publisher.send({'harvest_job_id': job['id']})
            log.info('Sent job %s to the gather queue' % job['id'])
            sent_jobs.append(job)

    publisher.close()
    return sent_jobs

def harvest_sources_reindex(context, data_dict):
    '''
        Reindexes all harvest source datasets with the latest status
    '''
    log.info('Reindexing all harvest sources')
    check_access('harvest_sources_reindex', context, data_dict)

    model = context['model']

    packages = model.Session.query(model.Package) \
                            .filter(model.Package.type==DATASET_TYPE_NAME) \
                            .filter(model.Package.state==u'active') \
                            .all()

    package_index = PackageSearchIndex()
    for package in packages:
        if 'extras_as_string'in context:
            del context['extras_as_string']
        context.update({'validate': False, 'ignore_auth': True})
        package_dict = logic.get_action('package_show')(context,
            {'id': package.id})
        log.debug('Updating search index for harvest source {0}'.format(package.id))
        package_index.index_package(package_dict, defer_commit=True)

    package_index.commit()
    log.info('Updated search index for {0} harvest sources'.format(len(packages)))
