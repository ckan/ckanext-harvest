# -*- coding: utf-8 -*-

import hashlib
import json
import six

import logging
import datetime

from ckantoolkit import config
from sqlalchemy import and_, or_
from six.moves.urllib.parse import urljoin

from ckan.lib.search.index import PackageSearchIndex
from ckan.plugins import toolkit, PluginImplementations, IActions
from ckan.logic import get_action
from ckanext.harvest.interfaces import IHarvester
from ckan.lib.search.common import SearchIndexError, make_connection
from ckan.lib.base import render_jinja2

from ckan.model import Package
from ckan import logic

from ckan.logic import NotFound, check_access

from ckanext.harvest.utils import (
    DATASET_TYPE_NAME
)
from ckanext.harvest.queue import (
    get_gather_publisher, resubmit_jobs, resubmit_objects)

from ckanext.harvest.model import HarvestSource, HarvestJob, HarvestObject, HarvestGatherError, HarvestObjectError
from ckanext.harvest.logic import HarvestJobExists
from ckanext.harvest.logic.dictization import harvest_job_dictize

from ckanext.harvest.logic.action.get import (
    harvest_source_show, harvest_job_list, _get_sources_for_user)

import ckan.lib.mailer as mailer
from itertools import islice

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

    # CKAN-2.6 or above: related don't exist any more
    if toolkit.check_ckan_version(max_version='2.5.99'):

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
     '''.format(
        harvest_source_id=harvest_source_id)

    if toolkit.check_ckan_version(max_version='2.5.99'):
        sql += '''
        delete from related_dataset where dataset_id in (
            select id from package where state = 'to_delete');
        delete from related where id in {related_ids};
        delete from package where id in (
            select id from package where state = 'to_delete');
        '''.format(related_ids=related_ids)
    else:
        # CKAN-2.6 or above: related don't exist any more
        sql += '''
        delete from package where id in (
            select id from package where state = 'to_delete');
        '''

    sql += '''
    commit;
    '''
    model.Session.execute(sql)

    # Refresh the index for this source to update the status object
    get_action('harvest_source_reindex')(context, {'id': harvest_source_id})

    return {'id': harvest_source_id}


def harvest_abort_failed_jobs(context, data_dict):
    session = context['session']

    try:
        life_span = int(data_dict.get('life_span'))
    except ValueError:
        life_span = 7

    include_sid = []
    exclude_sid = []
    include = data_dict.get('include')

    if include:
        include_sid = set(_id for _id in include.split(','))

    # TODO: if included sources provided do we want to use exclude?
    if data_dict.get('exclude') and not include:
        exclude_sid = set((_id for _id in data_dict.get('exclude').split(',')))

    # lifespan is based on source update frequency
    update_map = {
        'DAILY': 1,
        'WEEKLY': 7,
        'BIWEEKLY': 14,
        'MONTHLY': 30,
        'MANUAL': life_span,
        'ALWAYS': life_span
    }

    current_time = datetime.datetime.utcnow()

    # get all running jobs
    jobs_list = session.query(HarvestJob.id,
                              HarvestJob.source_id,
                              HarvestJob.created) \
        .filter(HarvestJob.status == 'Running') \
        .all()

    # filter out not included source job's
    if include_sid:
        jobs_list = [
            job for job in jobs_list
            if job.source_id in include_sid
        ]

    if not jobs_list:
        return 'There is no jobs to abort'

    aborted_counter = 0
    for job in jobs_list:
        harvest_source = session.query(HarvestSource.frequency) \
                                .filter(HarvestSource.id == job.source_id) \
                                .first()

        life_span = update_map.get(harvest_source.frequency)
        if not life_span:
            raise Exception('Frequency {freq} not recognised'.format(
                freq=harvest_source.frequency))

        expire_date = current_time - datetime.timedelta(days=life_span)

        if not include_sid and job.source_id in exclude_sid:
            log.info('Excluding source: {}'.format(job.source_id))
            continue
        # if job is running too long, abort it
        if job.created < expire_date:
            log.info(get_action('harvest_job_abort')(context, {'id': job.id}))
            aborted_counter += 1
        else:
            log.info('{} running less then {} days. Skipping...'.format(job.id, life_span))
    else:
        return 'Done. Aborted jobs: {}'.format(aborted_counter)


def harvest_sources_job_history_clear(context, data_dict):
    '''
    Clears the history for all active harvest sources. All jobs and objects related to a harvest source will
    be cleared, but keeps the source itself.
    This is useful to clean history of long running harvest sources to start again fresh.
    The datasets imported from the harvest source will NOT be deleted!!!

    '''
    check_access('harvest_sources_clear', context, data_dict)

    job_history_clear_results = []
    # We assume that the maximum of 1000 (hard limit) rows should be enough
    result = logic.get_action('package_search')(context, {'fq': '+dataset_type:harvest', 'rows': 1000})
    harvest_packages = result['results']
    if harvest_packages:
        for data_dict in harvest_packages:
            try:
                clear_result = get_action('harvest_source_job_history_clear')(context, {'id': data_dict['id']})
                job_history_clear_results.append(clear_result)
            except NotFound:
                # Ignoring not existent harvest sources because of a possibly corrupt search index
                # Logging was already done in called function
                pass

    return job_history_clear_results


def harvest_source_job_history_clear(context, data_dict):
    '''
    Clears all jobs and objects related to a harvest source, but keeps the source itself.
    This is useful to clean history of long running harvest sources to start again fresh.
    The datasets imported from the harvest source will NOT be deleted!!!

    :param id: the id of the harvest source to clear
    :type id: string

    '''
    check_access('harvest_source_clear', context, data_dict)

    harvest_source_id = data_dict.get('id', None)

    source = HarvestSource.get(harvest_source_id)
    if not source:
        log.error('Harvest source %s does not exist', harvest_source_id)
        raise NotFound('Harvest source %s does not exist' % harvest_source_id)

    harvest_source_id = source.id

    model = context['model']

    sql = '''begin;
    delete from harvest_object_error where harvest_object_id
     in (select id from harvest_object where harvest_source_id = '{harvest_source_id}');
    delete from harvest_object_extra where harvest_object_id
     in (select id from harvest_object where harvest_source_id = '{harvest_source_id}');
    delete from harvest_object where harvest_source_id = '{harvest_source_id}';
    delete from harvest_gather_error where harvest_job_id
     in (select id from harvest_job where source_id = '{harvest_source_id}');
    delete from harvest_job where source_id = '{harvest_source_id}';
    commit;
    '''.format(harvest_source_id=harvest_source_id)

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

    solr_commit = toolkit.asbool(config.get('ckan.search.solr_commit', 'true'))
    if toolkit.check_ckan_version(max_version='2.5.99'):
        # conn is solrpy
        try:
            conn.delete_query(query)
            if solr_commit:
                conn.commit()
        except Exception as e:
            log.exception(e)
            raise SearchIndexError(e)
        finally:
            conn.close()
    else:
        # conn is pysolr
        try:
            conn.delete(q=query, commit=solr_commit)
        except Exception as e:
            log.exception(e)
            raise SearchIndexError(e)

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
    :param guid: the guid of the harvest object to import
    :type guid: string
    :param harvest_object_id: the id of the harvest object to import
    :type harvest_object_id: string
    :param package_id: the id or name of the package to import
    :type package_id: string
    '''
    log.info('Harvest objects import: %r', data_dict)
    check_access('harvest_objects_import', context, data_dict)

    session = context['session']
    source_id = data_dict.get('source_id')
    guid = data_dict.get('guid')
    harvest_object_id = data_dict.get('harvest_object_id')
    package_id_or_name = data_dict.get('package_id')

    segments = context.get('segments')

    join_datasets = context.get('join_datasets', True)

    if guid:
        last_objects_ids = \
            session.query(HarvestObject.id) \
                   .filter(HarvestObject.guid == guid) \
                   .filter(HarvestObject.current == True)  # noqa: E712

    elif source_id:
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
                   .filter(HarvestObject.current == True)  # noqa: E712

    elif harvest_object_id:
        last_objects_ids = \
            session.query(HarvestObject.id) \
                   .filter(HarvestObject.id == harvest_object_id)
    elif package_id_or_name:
        last_objects_ids = \
            session.query(HarvestObject.id) \
                   .join(Package) \
                   .filter(
                HarvestObject.current == True  # noqa: E712
            ).filter(Package.state == u'active') \
                   .filter(or_(Package.id == package_id_or_name,
                               Package.name == package_id_or_name))
        join_datasets = False
    else:
        last_objects_ids = \
            session.query(HarvestObject.id) \
                   .filter(HarvestObject.current == True)  # noqa: E712

    if join_datasets:
        last_objects_ids = last_objects_ids.join(Package) \
            .filter(Package.state == u'active')

    last_objects_ids = last_objects_ids.all()

    last_objects_count = 0

    for obj_id in last_objects_ids:
        if segments and \
                str(hashlib.md5(six.ensure_binary(obj_id[0])).hexdigest())[0] not in segments:
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

    If ckanext.harvest.timeout is set:
    Check if the duration of the job is longer than ckanext.harvest.timeout, 
    then mark that job as finished as there is probably an underlying issue with the harvest process.

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
    timeout = config.get('ckan.harvest.timeout')

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
            job_obj = HarvestJob.get(job['id'])
            if timeout:
                last_time = job_obj.get_last_action_time()
                now = datetime.datetime.now()
                if now - last_time > datetime.timedelta(minutes=int(timeout)):
                    msg = 'Job {} timeout ({} minutes)\n'.format(job_obj.id, timeout)
                    msg += '\tJob created: {}\n'.format(job_obj.created)
                    msg += '\tJob gather finished: {}\n'.format(job_obj.created)
                    msg += '\tJob last action time: {}\n'.format(last_time)
                    
                    job_obj.status = u'Finished'
                    job_obj.finished = now
                    job_obj.save()

                    err = HarvestGatherError(message=msg, job=job_obj)
                    err.save()
                    log.info('Marking job as finished due to error: %s %s',
                            job_obj.source.url, job_obj.id)
                    continue

            if job['gather_finished']:
                num_objects_in_progress = \
                    session.query(HarvestObject.id) \
                           .filter(HarvestObject.harvest_job_id == job['id']) \
                           .filter(and_((HarvestObject.state != u'COMPLETE'),
                                        (HarvestObject.state != u'ERROR'))) \
                           .count()

                if num_objects_in_progress == 0:
                    
                    job_obj.status = u'Finished'
                    log.info('Marking job as finished %s %s',
                             job_obj.source.url, job_obj.id)

                    # save the time of finish, according to the last running
                    # object
                    last_object = session.query(HarvestObject) \
                        .filter(HarvestObject.harvest_job_id == job['id']) \
                        .filter(
                        HarvestObject.import_finished != None  # noqa: E711
                    ).order_by(HarvestObject.import_finished.desc()) \
                        .first()
                    if last_object:
                        job_obj.finished = last_object.import_finished
                    else:
                        job_obj.finished = job['gather_finished']
                    job_obj.save()

                    # Reindex the harvest source dataset so it has the latest
                    # status
                    get_action('harvest_source_reindex')(
                        context, {'id': job_obj.source.id})

                    status = get_action('harvest_source_show_status')(
                        context, {'id': job_obj.source.id})

                    notify_all = toolkit.asbool(config.get('ckan.harvest.status_mail.all'))
                    notify_errors = toolkit.asbool(config.get('ckan.harvest.status_mail.errored'))
                    last_job_errors = status['last_job']['stats'].get('errored', 0)
                    log.debug('Notifications: All:{} On error:{} Errors:{}'.format(notify_all, notify_errors, last_job_errors))
                    
                    if last_job_errors > 0 and (notify_all or notify_errors):
                        send_error_email(context, job_obj.source.id, status)
                    elif notify_all:
                        send_summary_email(context, job_obj.source.id, status)
                else:
                    log.debug('%d Ongoing jobs for %s (source:%s)',
                              num_objects_in_progress, job['id'], job['source_id'])
    log.debug('No jobs to send to the gather queue')

    # Resubmit old redis tasks
    resubmit_jobs()

    # Resubmit pending objects missing from Redis
    resubmit_objects()

    return []  # merely for backwards compatibility


def get_mail_extra_vars(context, source_id, status):
    last_job = status['last_job']
    
    source = get_action('harvest_source_show')(context, {'id': source_id})
    report = get_action(
        'harvest_job_report')(context, {'id': status['last_job']['id']})
    obj_errors = []
    job_errors = []
    
    for harvest_object_error_key in islice(report.get('object_errors'), 0, 20):
        harvest_object_error = report.get(
            'object_errors')[harvest_object_error_key]['errors']

        for error in harvest_object_error:
            obj_errors.append(error['message'])

    for harvest_gather_error in islice(report.get('gather_errors'), 0, 20):
        job_errors.append(harvest_gather_error['message'])

    if source.get('organization'):
        organization = source['organization']['name']
    else:
        organization = 'Not specified'

    harvest_configuration = source.get('config')

    if harvest_configuration in [None, '', '{}']:
        harvest_configuration = 'Not specified'

    errors = job_errors + obj_errors

    site_url = config.get('ckan.site_url')
    job_url = toolkit.url_for('harvest_job_show', source=source['id'], id=last_job['id'])
    full_job_url = urljoin(site_url, job_url)
    extra_vars = {
        'organization': organization,
        'site_title': config.get('ckan.site_title'),
        'site_url': site_url,
        'job_url': full_job_url,
        'harvest_source_title': source['title'],
        'harvest_configuration': harvest_configuration,
        'job_finished': last_job['finished'],
        'job_id': last_job['id'],
        'job_created': last_job['created'],
        'records_in_error': str(last_job['stats'].get('errored', 0)),
        'records_added': str(last_job['stats'].get('added', 0)),
        'records_deleted': str(last_job['stats'].get('deleted', 0)),
        'records_updated': str(last_job['stats'].get('updated', 0)),
        'error_summary_title': toolkit._('Error Summary'),
        'obj_errors_title': toolkit._('Document Error'),
        'job_errors_title': toolkit._('Job Errors'),
        'obj_errors': obj_errors,
        'job_errors': job_errors,
        'errors': errors,
    }

    return extra_vars

def prepare_summary_mail(context, source_id, status):
    extra_vars = get_mail_extra_vars(context, source_id, status)
    body = render_jinja2('emails/summary_email.txt', extra_vars)
    subject = '{} - Harvesting Job Successful - Summary Notification'\
                  .format(config.get('ckan.site_title'))
    
    return subject, body

def prepare_error_mail(context, source_id, status):
    extra_vars = get_mail_extra_vars(context, source_id, status)
    body = render_jinja2('emails/error_email.txt', extra_vars)
    subject = '{} - Harvesting Job - Error Notification'\
              .format(config.get('ckan.site_title'))

    return subject, body

def send_summary_email(context, source_id, status):
    subject, body = prepare_summary_mail(context, source_id, status)
    recipients = toolkit.get_action('harvest_get_notifications_recipients')(context, {'source_id': source_id})
    send_mail(recipients, subject, body)

def send_error_email(context, source_id, status):
    subject, body = prepare_error_mail(context, source_id, status)
    recipients = toolkit.get_action('harvest_get_notifications_recipients')(context, {'source_id': source_id})
    send_mail(recipients, subject, body)


def send_mail(recipients, subject, body):
    
    for recipient in recipients:
        email = {'recipient_name': recipient['name'],
                 'recipient_email': recipient['email'],
                 'subject': subject,
                 'body': body}

        try:
            mailer.mail_recipient(**email)
        except mailer.MailerException:
            log.error(
                'Sending Harvest-Notification-Mail failed. Message: ' + body)
        except Exception as e:
            log.error(e)
            raise


def harvest_send_job_to_gather_queue(context, data_dict):
    '''
    Sends a harvest job to the gather queue.

    :param id: the id of the harvest job
    :type id: string
    '''
    log.info('Send job to gather queue: %r', data_dict)

    job_id = logic.get_or_bust(data_dict, 'id')
    job = toolkit.get_action('harvest_job_show')(
        context, {'id': job_id})

    check_access('harvest_send_job_to_gather_queue', context, job)

    # gather queue
    publisher = get_gather_publisher()

    # Check the source is active
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

    Specify either id or source_id.

    :param id: the job id to abort, or the id or name of the harvest source
               with a job to abort
    :type id: string
    :param source_id: the name or id of the harvest source with a job to abort
    :type source_id: string
    '''

    check_access('harvest_job_abort', context, data_dict)

    model = context['model']

    source_or_job_id = data_dict.get('source_id') or data_dict.get('id')
    if source_or_job_id:
        try:
            source = harvest_source_show(context, {'id': source_or_job_id})
        except NotFound:
            job = get_action('harvest_job_show')(
                context, {'id': source_or_job_id})
        else:
            # HarvestJob set status to 'Aborted'
            # Do not use harvest_job_list since it can use a lot of memory
            # Get the most recent job for the source
            job = model.Session.query(HarvestJob) \
                       .filter_by(source_id=source['id']) \
                       .order_by(HarvestJob.created.desc()).first()
            if not job:
                raise NotFound('Error: source has no jobs')
            job_id = job.id
            job = get_action('harvest_job_show')(
                context, {'id': job_id})

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

    try:
        config = json.loads(package_dict.get('config', ''))
    except ValueError:
        config = {}
    for key, value in package_dict.items():
        if key not in config:
            new_dict[key] = value

    package_index = PackageSearchIndex()
    package_index.index_package(new_dict, defer_commit=defer_commit)

    return True
