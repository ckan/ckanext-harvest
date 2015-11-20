import hashlib
import json

import logging
import datetime

from pylons import config
from paste.deploy.converters import asbool
from sqlalchemy import and_, or_

from ckan.lib.search.index import PackageSearchIndex
from ckan.plugins import PluginImplementations
from ckan.logic import get_action
from ckanext.harvest.interfaces import IHarvester
from ckan.lib.search.common import SearchIndexError, make_connection


from ckan.model import Package
from ckan import logic
from ckan.plugins import toolkit


from ckan.logic import NotFound, check_access

from ckanext.harvest.plugin import DATASET_TYPE_NAME
from ckanext.harvest.queue import get_gather_publisher, resubmit_jobs

from ckanext.harvest.model import HarvestSource, HarvestJob, HarvestObject
from ckanext.harvest.logic import HarvestJobExists
from ckanext.harvest.logic.dictization import harvest_job_dictize

from ckanext.harvest.logic.action.get import (
    harvest_source_show, harvest_job_list, _get_sources_for_user)

log = logging.getLogger(__name__)


def harvest_source_update(context, data_dict):
    '''
    Updates an existing harvest source

    This method just proxies the request to package_update, which will create a
    harvest_source dataset type and the HarvestSource object. All auth checks
    and validation will be done there. We only make sure to set the dataset
    type

    Note that the harvest source type (ckan, waf, csw, etc) is now set via the
    source_type field.

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
    source = logic.get_action('package_update')(context, data_dict)

    return source


def harvest_source_clear(context, data_dict):
    '''
    Clears all datasets, jobs and objects related to a harvest source, but
    keeps the source itself.  This is useful to clean history of long running
    harvest sources to start again fresh.

    :param id: the id of the harvest source to clear
    :type id: string
    '''

    check_access('harvest_source_clear', context, data_dict)

    harvest_source_id = data_dict.get('id')

    source = HarvestSource.get(harvest_source_id)
    if not source:
        log.error('Harvest source %s does not exist', harvest_source_id)
        raise NotFound('Harvest source %s does not exist' % harvest_source_id)

    harvest_source_id = source.id

    # Clear all datasets from this source from the index
    harvest_source_index_clear(context, data_dict)

    model = context['model']

    sql = '''select id from related where id in (
              select related_id from related_dataset where dataset_id in (
                  select package_id from harvest_object
                  where harvest_source_id = '{harvest_source_id}'));'''.format(
        harvest_source_id=harvest_source_id)
    result = model.Session.execute(sql)
    ids = []
    for row in result:
        ids.append(row[0])
    related_ids = "('" + "','".join(ids) + "')"

    sql = '''begin;
        update package set state = 'to_delete' where id in (
            select package_id from harvest_object
            where harvest_source_id = '{harvest_source_id}');'''.format(
        harvest_source_id=harvest_source_id)

    # CKAN-2.3 or above: delete resource views, resource revisions & resources
    if toolkit.check_ckan_version(min_version='2.3'):
        sql += '''
        delete from resource_view where resource_id in (
            select id from resource where package_id in (
                select id from package where state = 'to_delete'));
        delete from resource_revision where package_id in (
            select id from package where state = 'to_delete');
        delete from resource where package_id in (
            select id from package where state = 'to_delete');
        '''
    # Backwards-compatibility: support ResourceGroup (pre-CKAN-2.3)
    else:
        sql += '''
        delete from resource_revision where resource_group_id in (
            select id from resource_group where package_id in (
                select id from package where state = 'to_delete'));
        delete from resource where resource_group_id in (
            select id from resource_group where package_id in (
                select id from package where state = 'to_delete'));
        delete from resource_group_revision where package_id in (
            select id from package where state = 'to_delete');
        delete from resource_group where package_id in (
            select id from package where state = 'to_delete');
        '''
    # CKAN pre-2.5: authz models were removed in migration 078
    if toolkit.check_ckan_version(max_version='2.4.99'):
        sql += '''
        delete from package_role where package_id in (
            select id from package where state = 'to_delete');
        delete from user_object_role where id not in (
            select user_object_role_id from package_role)
            and context = 'Package';
        '''

    sql += '''
    delete from harvest_object_error where harvest_object_id in (
        select id from harvest_object
        where harvest_source_id = '{harvest_source_id}');
    delete from harvest_object_extra where harvest_object_id in (
        select id from harvest_object
        where harvest_source_id = '{harvest_source_id}');
    delete from harvest_object where harvest_source_id = '{harvest_source_id}';
    delete from harvest_gather_error where harvest_job_id in (
        select id from harvest_job where source_id = '{harvest_source_id}');
    delete from harvest_job where source_id = '{harvest_source_id}';
    delete from package_tag_revision where package_id in (
        select id from package where state = 'to_delete');
    delete from member_revision where table_id in (
        select id from package where state = 'to_delete');
    delete from package_extra_revision where package_id in (
        select id from package where state = 'to_delete');
    delete from package_revision where id in (
        select id from package where state = 'to_delete');
    delete from package_tag where package_id in (
        select id from package where state = 'to_delete');
    delete from package_extra where package_id in (
        select id from package where state = 'to_delete');
    delete from package_relationship_revision where subject_package_id in (
        select id from package where state = 'to_delete');
    delete from package_relationship_revision where object_package_id in (
        select id from package where state = 'to_delete');
    delete from package_relationship where subject_package_id in (
        select id from package where state = 'to_delete');
    delete from package_relationship where object_package_id in (
        select id from package where state = 'to_delete');
    delete from member where table_id in (
        select id from package where state = 'to_delete');
    delete from related_dataset where dataset_id in (
        select id from package where state = 'to_delete');
    delete from related where id in {related_ids};
    delete from package where id in (
        select id from package where state = 'to_delete');
    commit;
    '''.format(
        harvest_source_id=harvest_source_id, related_ids=related_ids)

    model.Session.execute(sql)

    # Refresh the index for this source to update the status object
    get_action('harvest_source_reindex')(context, {'id': harvest_source_id})

    return {'id': harvest_source_id}


def harvest_source_index_clear(context, data_dict):
    '''
    Clears all datasets, jobs and objects related to a harvest source, but
    keeps the source itself.  This is useful to clean history of long running
    harvest sources to start again fresh.

    :param id: the id of the harvest source to clear
    :type id: string
    '''

    check_access('harvest_source_clear', context, data_dict)
    harvest_source_id = data_dict.get('id')

    source = HarvestSource.get(harvest_source_id)
    if not source:
        log.error('Harvest source %s does not exist', harvest_source_id)
        raise NotFound('Harvest source %s does not exist' % harvest_source_id)

    harvest_source_id = source.id

    conn = make_connection()
    query = ''' +%s:"%s" +site_id:"%s" ''' % (
        'harvest_source_id', harvest_source_id, config.get('ckan.site_id'))
    try:
        conn.delete_query(query)
        if asbool(config.get('ckan.search.solr_commit', 'true')):
            conn.commit()
    except Exception, e:
        log.exception(e)
        raise SearchIndexError(e)
    finally:
        conn.close()

    return {'id': harvest_source_id}


def harvest_objects_import(context, data_dict):
    '''
    Reimports the existing harvest objects, specified by either source_id,
    harvest_object_id or package_id.

    It performs the import stage with the last fetched objects, optionally
    belonging to a certain source.

    Please note that no objects will be fetched from the remote server.

    It will only affect the last fetched objects already present in the
    database.

    :param source_id: the id of the harvest source to import
    :type source_id: string
    :param harvest_object_id: the id of the harvest object to import
    :type harvest_object_id: string
    :param package_id: the id or name of the package to import
    :type package_id: string
    '''
    log.info('Harvest objects import: %r', data_dict)
    check_access('harvest_objects_import', context, data_dict)

    model = context['model']
    session = context['session']
    source_id = data_dict.get('source_id')
    harvest_object_id = data_dict.get('harvest_object_id')
    package_id_or_name = data_dict.get('package_id')

    segments = context.get('segments')

    join_datasets = context.get('join_datasets', True)

    if source_id:
        source = HarvestSource.get(source_id)
        if not source:
            log.error('Harvest source %s does not exist', source_id)
            raise NotFound('Harvest source %s does not exist' % source_id)

        if not source.active:
            log.warn('Harvest source %s is not active.', source_id)
            raise Exception('This harvest source is not active')

        last_objects_ids = \
            session.query(HarvestObject.id) \
                   .join(HarvestSource) \
                   .filter(HarvestObject.source == source) \
                   .filter(HarvestObject.current == True)

    elif harvest_object_id:
        last_objects_ids = \
            session.query(HarvestObject.id) \
                   .filter(HarvestObject.id == harvest_object_id)
    elif package_id_or_name:
        last_objects_ids = \
            session.query(HarvestObject.id) \
                   .join(Package) \
                   .filter(HarvestObject.current == True) \
                   .filter(Package.state == u'active') \
                   .filter(or_(Package.id == package_id_or_name,
                               Package.name == package_id_or_name))
        join_datasets = False
    else:
        last_objects_ids = \
            session.query(HarvestObject.id) \
                   .filter(HarvestObject.current == True)

    if join_datasets:
        last_objects_ids = last_objects_ids.join(Package) \
            .filter(Package.state == u'active')

    last_objects_ids = last_objects_ids.all()

    last_objects_count = 0

    for obj_id in last_objects_ids:
        if segments and \
                str(hashlib.md5(obj_id[0]).hexdigest())[0] not in segments:
            continue

        obj = session.query(HarvestObject).get(obj_id)

        for harvester in PluginImplementations(IHarvester):
            if harvester.info()['name'] == obj.source.type:
                if hasattr(harvester, 'force_import'):
                    harvester.force_import = True
                harvester.import_stage(obj)
                break
        last_objects_count += 1
    log.info('Harvest objects imported: %s', last_objects_count)
    return last_objects_count


def _calculate_next_run(frequency):

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
        if now.month in (4, 6, 9, 11):
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
        data_dict = {'source_id': source.id, 'run': True}
        try:
            get_action('harvest_job_create')(context, data_dict)
        except HarvestJobExists:
            log.info('Trying to rerun job for %s skipping', source.id)

        source.next_run = _calculate_next_run(source.frequency)
        source.save()


def harvest_jobs_run(context, data_dict):
    '''
    Runs scheduled jobs, checks if any jobs need marking as finished, and
    resubmits queue items if needed.

    This should be called every few minutes (e.g. by a cron), or else jobs
    will never show as finished.

    This used to also 'run' new jobs created by the web UI, putting them onto
    the gather queue, but now this is done by default when you create a job. If
    you need to send do this explicitly, then use
    ``harvest_send_job_to_gather_queue``.

    :param source_id: the id of the harvest source, if you just want to check
                      for its finished jobs (optional)
    :type source_id: string
    '''
    log.info('Harvest job run: %r', data_dict)
    check_access('harvest_jobs_run', context, data_dict)

    session = context['session']

    source_id = data_dict.get('source_id')

    # Scheduled jobs
    if not source_id:
        _make_scheduled_jobs(context, data_dict)

    context['return_objects'] = False

    # Flag finished jobs as such
    jobs = harvest_job_list(
        context, {'source_id': source_id, 'status': u'Running'})
    if len(jobs):
        for job in jobs:
            if job['gather_finished']:
                objects = \
                    session.query(HarvestObject.id) \
                           .filter(HarvestObject.harvest_job_id == job['id']) \
                           .filter(and_((HarvestObject.state != u'COMPLETE'),
                                        (HarvestObject.state != u'ERROR'))) \
                           .order_by(HarvestObject.import_finished.desc())

                if objects.count() == 0:
                    job_obj = HarvestJob.get(job['id'])
                    job_obj.status = u'Finished'

                    last_object = session.query(HarvestObject) \
                        .filter(HarvestObject.harvest_job_id == job['id']) \
                        .filter(HarvestObject.import_finished != None) \
                        .order_by(HarvestObject.import_finished.desc()) \
                        .first()
                    if last_object:
                        job_obj.finished = last_object.import_finished
                    else:
                        job_obj.finished = job['gather_finished']
                    job_obj.save()
                    log.info('Marking job as finished: %s', job_obj)
                    # Reindex the harvest source dataset so it has the latest
                    # status
                    get_action('harvest_source_reindex')(
                        context, {'id': job_obj.source.id})
                else:
                    log.debug('Ongoing job:%s source:%s',
                              job['id'], job['source_id'])

    # resubmit old redis tasks
    resubmit_jobs()

    return []  # merely for backwards compatibility


def harvest_send_job_to_gather_queue(context, data_dict):
    '''
    Sends a harvest job to the gather queue.

    :param id: the id of the harvest job
    :type id: string
    '''
    log.info('Send job to gather queue: %r', data_dict)
    check_access('harvest_send_job_to_gather_queue', context, data_dict)

    job_id = logic.get_or_bust(data_dict, 'id')

    # gather queue
    publisher = get_gather_publisher()

    job = logic.get_action('harvest_job_show')(
        context, {'id': job_id})

    # Check the source is active
    context['detailed'] = False
    source = harvest_source_show(context, {'id': job['source_id']})
    if not source['active']:
        raise toolkit.ValidationError('Source is not active')

    job_obj = HarvestJob.get(job['id'])
    job_obj.status = job['status'] = u'Running'
    job_obj.save()
    publisher.send({'harvest_job_id': job['id']})
    log.info('Sent job %s to the gather queue', job['id'])

    return harvest_job_dictize(job_obj, context)


def harvest_job_abort(context, data_dict):
    '''
    Aborts a harvest job. Given a harvest source_id, it looks for the latest
    one and (assuming it not already Finished) marks it as Finished. It also
    marks any of that source's harvest objects and (if not complete or error)
    marks them "ERROR", so any left in limbo are cleaned up. Does not actually
    stop running any queued harvest fetchs/objects.

    :param source_id: the name or id of the harvest source with a job to abort
    :type source_id: string
    '''

    check_access('harvest_job_abort', context, data_dict)

    model = context['model']

    source_id = data_dict.get('source_id')
    source = harvest_source_show(context, {'id': source_id})

    # HarvestJob set status to 'Finished'
    # Don not use harvest_job_list since it can use a lot of memory
    last_job = model.Session.query(HarvestJob) \
                    .filter_by(source_id=source['id']) \
                    .order_by(HarvestJob.created.desc()).first()
    if not last_job:
        raise NotFound('Error: source has no jobs')
    job = get_action('harvest_job_show')(context,
                                         {'id': last_job.id})

    if job['status'] != 'Finished':
        # i.e. New or Running
        job_obj = HarvestJob.get(job['id'])
        job_obj.status = new_status = 'Finished'
        model.repo.commit_and_remove()
        log.info('Harvest job changed status from "%s" to "%s"',
                 job['status'], new_status)
    else:
        log.info('Harvest job unchanged. Source %s status is: "%s"',
                 job['id'], job['status'])

    # HarvestObjects set to ERROR
    job_obj = HarvestJob.get(job['id'])
    objs = job_obj.objects
    for obj in objs:
        if obj.state not in ('COMPLETE', 'ERROR'):
            old_state = obj.state
            obj.state = 'ERROR'
            log.info('Harvest object changed state from "%s" to "%s": %s',
                     old_state, obj.state, obj.id)
        else:
            log.info('Harvest object not changed from "%s": %s',
                     obj.state, obj.id)
    model.repo.commit_and_remove()

    job_obj = HarvestJob.get(job['id'])
    return harvest_job_dictize(job_obj, context)


@logic.side_effect_free
def harvest_sources_reindex(context, data_dict):
    '''
        Reindexes all harvest source datasets with the latest status
    '''
    log.info('Reindexing all harvest sources')
    check_access('harvest_sources_reindex', context, data_dict)

    model = context['model']

    packages = model.Session.query(model.Package) \
                            .filter(model.Package.type == DATASET_TYPE_NAME) \
                            .filter(model.Package.state == u'active') \
                            .all()

    package_index = PackageSearchIndex()

    reindex_context = {'defer_commit': True}
    for package in packages:
        get_action('harvest_source_reindex')(
            reindex_context, {'id': package.id})

    package_index.commit()

    return True


@logic.side_effect_free
def harvest_source_reindex(context, data_dict):
    '''Reindex a single harvest source'''

    harvest_source_id = logic.get_or_bust(data_dict, 'id')
    defer_commit = context.get('defer_commit', False)

    if 'extras_as_string'in context:
        del context['extras_as_string']
    context.update({'ignore_auth': True})
    package_dict = logic.get_action('harvest_source_show')(
        context, {'id': harvest_source_id})
    log.debug('Updating search index for harvest source: %s',
              package_dict.get('name') or harvest_source_id)

    # Remove configuration values
    new_dict = {}
    if package_dict.get('config'):
        config = json.loads(package_dict['config'])
        for key, value in package_dict.iteritems():
            if key not in config:
                new_dict[key] = value
    package_index = PackageSearchIndex()
    package_index.index_package(new_dict, defer_commit=defer_commit)

    return True
