import logging
from ckan.lib.base import config
from sqlalchemy import or_
from ckan.model import User, Package
import datetime

from ckan import logic
from ckan.plugins import PluginImplementations
from ckanext.harvest.interfaces import IHarvester

import ckan.plugins as p
from ckan.logic import NotFound, check_access, side_effect_free

from ckanext.harvest import model as harvest_model

from ckanext.harvest.model import (HarvestSource, HarvestJob, HarvestObject, HarvestLog)
from ckanext.harvest.logic.dictization import (harvest_source_dictize,
                                               harvest_job_dictize,
                                               harvest_object_dictize,
                                               harvest_log_dictize)

log = logging.getLogger(__name__)


@side_effect_free
def harvest_source_show(context, data_dict):
    '''
    Returns the metadata of a harvest source

    This method just proxies the request to package_show. All auth checks and
    validation will be done there.

    :param id: the id or name of the harvest source
    :type id: string

    :param url: url of the harvest source (as an alternative to the id)
    :type url: string

    :returns: harvest source metadata
    :rtype: dictionary
    '''
    model = context.get('model')

    # Find the source by URL
    if data_dict.get('url') and not data_dict.get('id'):
        source = model.Session.query(harvest_model.HarvestSource) \
                      .filter_by(url=data_dict.get('url')) \
                      .first()
        if not source:
            raise NotFound
        data_dict['id'] = source.id

    source_dict = logic.get_action('package_show')(context, data_dict)

    # For compatibility with old code, add the active field
    # based on the package state
    source_dict['active'] = (source_dict['state'] == 'active')

    return source_dict


@side_effect_free
def harvest_source_show_status(context, data_dict):
    '''
    Returns a status report for a harvest source

    Given a particular source, returns a dictionary containing information
    about the source jobs, datasets created, errors, etc.
    Note that this information is already included on the output of
    harvest_source_show, under the 'status' field.

    :param id: the id or name of the harvest source
    :type id: string

    :rtype: dictionary
    '''

    p.toolkit.check_access('harvest_source_show_status', context, data_dict)

    model = context.get('model')

    source = harvest_model.HarvestSource.get(data_dict['id'])
    if not source:
        raise p.toolkit.ObjectNotFound('Harvest source {0} does not exist'.format(data_dict['id']))

    out = {
           'job_count': 0,
           'last_job': None,
           'total_datasets': 0,
           }

    jobs = harvest_model.HarvestJob.filter(source=source).all()

    job_count = len(jobs)
    if job_count == 0:
        return out

    out['job_count'] = job_count

    # Get the most recent job
    last_job = harvest_model.HarvestJob.filter(source=source) \
        .order_by(harvest_model.HarvestJob.created.desc()).first()

    if not last_job:
        return out

    out['last_job'] = harvest_job_dictize(last_job, context)

    # Overall statistics
    packages = model.Session.query(model.Package) \
        .join(harvest_model.HarvestObject) \
        .filter(harvest_model.HarvestObject.harvest_source_id == source.id) \
        .filter(
        harvest_model.HarvestObject.current == True  # noqa: E711
    ).filter(model.Package.state == u'active') \
        .filter(model.Package.private == False)
    out['total_datasets'] = packages.count()

    return out


@side_effect_free
def harvest_source_list(context, data_dict):
    '''
    TODO: Use package search
    '''


    organization_id = data_dict.get('organization_id')
    limit = config.get('ckan.harvest.harvest_source_limit', 100)

    sources = _get_sources_for_user(context, data_dict, organization_id=organization_id, limit=limit)

    last_job_status = p.toolkit.asbool(data_dict.get('return_last_job_status', False))

    return [harvest_source_dictize(source, context, last_job_status) for source in sources]


@side_effect_free
def harvest_job_show(context, data_dict):

    check_access('harvest_job_show', context, data_dict)

    id = data_dict.get('id')
    attr = data_dict.get('attr', None)

    job = HarvestJob.get(id, attr=attr)
    if not job:
        raise NotFound

    return harvest_job_dictize(job, context)


@side_effect_free
def harvest_job_report(context, data_dict):

    check_access('harvest_job_show', context, data_dict)

    model = context['model']
    id = data_dict.get('id')

    job = HarvestJob.get(id)
    if not job:
        raise NotFound

    report = {
        'gather_errors': [],
        'object_errors': {}
    }

    # Gather errors
    q = model.Session.query(harvest_model.HarvestGatherError) \
        .join(harvest_model.HarvestJob) \
        .filter(harvest_model.HarvestGatherError.harvest_job_id == job.id) \
        .order_by(harvest_model.HarvestGatherError.created.desc())

    for error in q.all():
        report['gather_errors'].append({
            'message': error.message
        })

    # Object errors

    # Check if the harvester for this job's source has a method for returning
    # the URL to the original document
    original_url_builder = None
    for harvester in PluginImplementations(IHarvester):
        if harvester.info()['name'] == job.source.type:
            if hasattr(harvester, 'get_original_url'):
                original_url_builder = harvester.get_original_url

    q = model.Session.query(harvest_model.HarvestObjectError, harvest_model.HarvestObject.guid) \
        .join(harvest_model.HarvestObject) \
        .filter(harvest_model.HarvestObject.harvest_job_id == job.id) \
        .order_by(harvest_model.HarvestObjectError.harvest_object_id)

    for error, guid in q.all():
        if error.harvest_object_id not in report['object_errors']:
            report['object_errors'][error.harvest_object_id] = {
                'guid': guid,
                'errors': []
            }
            if original_url_builder:
                url = original_url_builder(error.harvest_object_id)
                if url:
                    report['object_errors'][error.harvest_object_id]['original_url'] = url

        report['object_errors'][error.harvest_object_id]['errors'].append({
            'message': error.message,
            'line': error.line,
            'type': error.stage
         })

    return report


@side_effect_free
def harvest_job_list(context, data_dict):
    '''Returns a list of jobs and details of objects and errors.

    :param status: filter by e.g. "New" or "Finished" jobs
    :param source_id: filter by a harvest source
    '''

    check_access('harvest_job_list', context, data_dict)

    session = context['session']

    source_id = data_dict.get('source_id', False)
    status = data_dict.get('status', False)

    query = session.query(HarvestJob)

    if source_id:
        query = query.filter(HarvestJob.source_id == source_id)

    if status:
        query = query.filter(HarvestJob.status == status)

    query = query.order_by(HarvestJob.created.desc())

    jobs = query.all()

    context['return_error_summary'] = False
    return [harvest_job_dictize(job, context) for job in jobs]


@side_effect_free
def harvest_object_show(context, data_dict):

    p.toolkit.check_access('harvest_object_show', context, data_dict)

    id = data_dict.get('id')
    dataset_id = data_dict.get('dataset_id')

    if id:
        attr = data_dict.get('attr', None)
        obj = HarvestObject.get(id, attr=attr)
    elif dataset_id:
        model = context['model']

        pkg = model.Package.get(dataset_id)
        if not pkg:
            raise p.toolkit.ObjectNotFound('Dataset not found')

        obj = model.Session.query(HarvestObject) \
            .filter(HarvestObject.package_id == pkg.id) \
            .filter(
            HarvestObject.current == True  # noqa: E711
        ).first()
    else:
        raise p.toolkit.ValidationError(
            'Please provide either an "id" or a "dataset_id" parameter')

    if not obj:
        raise p.toolkit.ObjectNotFound('Harvest object not found')

    return harvest_object_dictize(obj, context)


@side_effect_free
def harvest_object_list(context, data_dict):

    check_access('harvest_object_list', context, data_dict)

    session = context['session']

    only_current = data_dict.get('only_current', True)
    source_id = data_dict.get('source_id', False)

    query = session.query(HarvestObject)

    if source_id:
        query = query.filter(HarvestObject.harvest_source_id == source_id)

    if only_current:
        query = query.filter(
            HarvestObject.current == True  # noqa: E712
        )

    objects = query.all()

    return [getattr(obj, 'id') for obj in objects]


@side_effect_free
def harvesters_info_show(context, data_dict):
    '''Returns details of the installed harvesters.'''

    check_access('harvesters_info_show', context, data_dict)

    available_harvesters = []
    for harvester in PluginImplementations(IHarvester):
        info = harvester.info()
        if not info or 'name' not in info:
            log.error('Harvester %r does not provide the harvester name in the info response' % str(harvester))
            continue
        info['show_config'] = (info.get('form_config_interface', '') == 'Text')
        available_harvesters.append(info)

    return available_harvesters


@side_effect_free
def harvest_log_list(context, data_dict):
    '''Returns a list of harvester log entries.

    :param limit: number of logs to be shown default: 100
    :param offset: use with ``per_page`` default: 0
    :param level: filter log entries by level(debug, info, warning, error, critical)
    '''

    check_access('harvest_log_list', context, data_dict)

    session = context['session']

    try:
        limit = int(data_dict.get('limit', 100))
    except ValueError:
        limit = 100

    if data_dict.get('per_page', False):
        try:
            limit = int(data_dict.get('per_page', 100))
        except ValueError:
            limit = 100

    try:
        offset = int(data_dict.get('offset', 0))
    except ValueError:
        offset = 0

    level = data_dict.get('level', None)

    query = session.query(HarvestLog)

    if level is not None:
        query = query.filter(HarvestLog.level == level.upper())

    query = query.order_by(HarvestLog.created.desc())
    logs = query.offset(offset).limit(limit).all()

    out = [harvest_log_dictize(obj, context) for obj in logs]
    return out


def _get_sources_for_user(context, data_dict, organization_id=None, limit=None):

    session = context['session']
    user = context.get('user', '')

    only_active = data_dict.get('only_active', False)
    only_to_run = data_dict.get('only_to_run', False)

    query = session.query(HarvestSource) \
        .order_by(HarvestSource.created.desc())

    if organization_id:
        query = query.join(
            Package, HarvestSource.id == Package.id
        ).filter(Package.owner_org == organization_id)

    if only_active:
        query = query.filter(
            HarvestSource.active == True  # noqa: E712
        ) \

    if only_to_run:
        query = query.filter(HarvestSource.frequency != 'MANUAL')
        query = query.filter(or_(HarvestSource.next_run <= datetime.datetime.utcnow(),
                                 HarvestSource.next_run == None  # noqa: E711
                                 )
                            )

    user_obj = User.get(user)
    # Sysadmins will get all sources
    if user_obj and not user_obj.sysadmin:
        # This only applies to a non sysadmin user when using the
        # publisher auth profile. When using the default profile,
        # normal users will never arrive at this point, but even if they
        # do, they will get an empty list.

        publisher_filters = []
        publishers_for_the_user = user_obj.get_groups(u'publisher')
        for publisher_id in [g.id for g in publishers_for_the_user]:
            publisher_filters.append(HarvestSource.publisher_id == publisher_id)

        if len(publisher_filters):
            query = query.filter(or_(*publisher_filters))
        else:
            # This user does not belong to a publisher yet, no sources for him/her
            return []

        log.debug('User %s with publishers %r has Harvest Sources: %r',
                  user, publishers_for_the_user, [(hs.id, hs.url) for hs in query])

    sources = query.limit(limit).all() if limit else query.all()

    return sources

def harvest_get_notifications_recipients(context, data_dict):
    """ get all recipients for a harvest source
        Return a list of dicts like {'name': 'Jhon', 'email': jhon@source.com'} """
    
    check_access('harvest_get_notifications_recipients', context, data_dict)

    source_id = data_dict['source_id']
    source = p.toolkit.get_action('harvest_source_show')(context, {'id': source_id})
    recipients = []

    # gather sysadmins
    model = context['model']
    sysadmins = model.Session.query(model.User).filter(
        model.User.sysadmin == True  # noqa: E712
    ).all()

    for sysadmin in sysadmins:
        recipients.append({
            'name': sysadmin.name,
            'email': sysadmin.email
        })

    # gather organization-admins
    if source.get('organization'):
        members = p.toolkit.get_action('member_list')(context, {
            'id': source['organization']['id'],
            'object_type': 'user',
            'capacity': 'admin'
        })

        for member in members:
            member_details = p.toolkit.get_action(
                'user_show')(context, {'id': member[0]})

            if member_details['email']:
                recipients.append({
                    'name': member_details['name'],
                    'email': member_details['email']
                })
    
    return recipients