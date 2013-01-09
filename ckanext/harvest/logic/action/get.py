import logging
from sqlalchemy import or_
from ckan.model import User
from ckan.new_authz import is_sysadmin
from ckan.plugins import PluginImplementations
from ckanext.harvest.interfaces import IHarvester


from ckan.logic import NotFound, check_access

from ckanext.harvest.model import (HarvestSource, HarvestJob, HarvestObject)
from ckanext.harvest.logic.dictization import (harvest_source_dictize,
                                               harvest_job_dictize,
                                               harvest_object_dictize)

log = logging.getLogger(__name__)

def harvest_source_show(context,data_dict):
    check_access('harvest_source_show',context,data_dict)

    id = data_dict.get('id')
    attr = data_dict.get('attr',None)

    source = HarvestSource.get(id,attr=attr)

    if not source:
        raise NotFound

    return harvest_source_dictize(source,context)

def harvest_source_list(context, data_dict):

    check_access('harvest_source_list',context,data_dict)

    model = context['model']
    session = context['session']
    user = context.get('user','')

    sources = _get_sources_for_user(context, data_dict)

    context.update({'detailed':False})
    return [harvest_source_dictize(source, context) for source in sources]

def harvest_source_for_a_dataset(context, data_dict):
    '''For a given dataset, return the harvest source that
    created or last updated it, otherwise NotFound.'''

    model = context['model']
    session = context['session']

    dataset_id = data_dict.get('id')

    query = session.query(HarvestSource)\
            .join(HarvestObject)\
            .filter_by(package_id=dataset_id)\
            .order_by(HarvestObject.gathered.desc())
    source = query.first() # newest

    if not source:
        raise NotFound

    return harvest_source_dictize(source,context)

def harvest_job_show(context,data_dict):

    check_access('harvest_job_show',context,data_dict)

    id = data_dict.get('id')
    attr = data_dict.get('attr',None)

    job = HarvestJob.get(id,attr=attr)
    if not job:
        raise NotFound

    return harvest_job_dictize(job,context)

def harvest_job_list(context,data_dict):

    check_access('harvest_job_list',context,data_dict)

    model = context['model']
    session = context['session']

    source_id = data_dict.get('source_id',False)
    status = data_dict.get('status',False)

    query = session.query(HarvestJob)

    if source_id:
        query = query.filter(HarvestJob.source_id==source_id)

    if status:
        query = query.filter(HarvestJob.status==status)

    jobs = query.all()

    return [harvest_job_dictize(job,context) for job in jobs]

def harvest_object_show(context,data_dict):

    check_access('harvest_object_show',context,data_dict)

    id = data_dict.get('id')
    attr = data_dict.get('attr',None)
    obj = HarvestObject.get(id,attr=attr)
    if not obj:
        raise NotFound

    return harvest_object_dictize(obj,context)

def harvest_object_list(context,data_dict):

    check_access('harvest_object_list',context,data_dict)

    model = context['model']
    session = context['session']

    only_current = data_dict.get('only_current',True)
    source_id = data_dict.get('source_id',False)

    query = session.query(HarvestObject)

    if source_id:
        query = query.filter(HarvestObject.source_id==source_id)

    if only_current:
        query = query.filter(HarvestObject.current==True)

    objects = query.all()

    return [getattr(obj,'id') for obj in objects]

def harvesters_info_show(context,data_dict):

    check_access('harvesters_info_show',context,data_dict)

    available_harvesters = []
    for harvester in PluginImplementations(IHarvester):
        info = harvester.info()
        if not info or 'name' not in info:
            log.error('Harvester %r does not provide the harvester name in the info response' % str(harvester))
            continue
        info['show_config'] = (info.get('form_config_interface','') == 'Text')
        available_harvesters.append(info)

    return available_harvesters

def _get_sources_for_user(context,data_dict):

    model = context['model']
    session = context['session']
    user = context.get('user','')

    only_active = data_dict.get('only_active',False)

    query = session.query(HarvestSource) \
                .order_by(HarvestSource.created.desc())

    if only_active:
        query = query.filter(HarvestSource.active==True) \

    # Sysadmins will get all sources
    if not is_sysadmin(user):
        # This only applies to a non sysadmin user when using the
        # publisher auth profile. When using the default profile,
        # normal users will never arrive at this point, but even if they
        # do, they will get an empty list.
        user_obj = User.get(user)

        publisher_filters = []
        publishers_for_the_user = user_obj.get_groups(u'publisher')
        for publisher_id in [g.id for g in publishers_for_the_user]:
            publisher_filters.append(HarvestSource.publisher_id==publisher_id)

        if len(publisher_filters):
            query = query.filter(or_(*publisher_filters))
        else:
            # This user does not belong to a publisher yet, no sources for him/her
            return []

        log.debug('User %s with publishers %r has Harvest Sources: %r',
                  user, publishers_for_the_user, [(hs.id, hs.url) for hs in query])

    sources = query.all()

    return sources

