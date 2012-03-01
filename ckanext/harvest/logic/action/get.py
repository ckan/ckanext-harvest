from ckan.plugins import PluginImplementations
from ckanext.harvest.interfaces import IHarvester


from ckan.logic import NotFound, check_access

from ckanext.harvest.model import (HarvestSource, HarvestJob, HarvestObject)
from ckanext.harvest.logic.dictization import (harvest_source_dictize,
                                               harvest_job_dictize,
                                               harvest_object_dictize)

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

    only_active = data_dict.get('only_active',False)

    if only_active:
        sources = session.query(HarvestSource) \
                    .filter(HarvestSource.active==True) \
                    .order_by(HarvestSource.created.desc()) \
                    .all()
    else:
        sources = session.query(HarvestSource) \
                    .order_by(HarvestSource.created.desc()) \
                    .all()

    context.update({'detailed':False})
    return [harvest_source_dictize(source, context) for source in sources]

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

    if only_current:
        objects = session.query(HarvestObject) \
                    .filter(HarvestObject.current==True) \
                    .all()
    else:
        objects = session.query(HarvestObject).all()

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
