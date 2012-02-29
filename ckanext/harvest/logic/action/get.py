from ckan.logic import NotFound, ValidationError

from ckanext.harvest.model import (HarvestSource, HarvestJob, HarvestObject)
from ckanext.harvest.logic.dictization import (harvest_source_dictize,
                                               harvest_job_dictize,
                                               harvest_object_dictize)

def harvest_source_show(context,data_dict):

    id = data_dict.get('id')
    attr = data_dict.get('attr',None)

    source = HarvestSource.get(id,attr=attr)

    if not source:
        raise NotFound

    return harvest_source_dictize(source,context)

def harvest_source_list(context, data_dict):

    model = context['model']

    only_active = data_dict.get('only_active',False)

    if only_active:
        sources = model.Session.query(HarvestSource) \
                    .filter(HarvestSource.active==True) \
                    .order_by(HarvestSource.created.desc()) \
                    .all()
    else:
        sources = model.Session.query(HarvestSource) \
                    .order_by(HarvestSource.created.desc()) \
                    .all()

    context.update({'detailed':False})
    return [harvest_source_dictize(source, context) for source in sources]

def harvest_job_show(context,data_dict):

    id = data_dict.get('id')
    attr = data_dict.get('attr',None)

    job = HarvestJob.get(id,attr=attr)
    if not job:
        raise NotFound

    return harvest_job_dictize(job,context)

def harvest_job_list(context,data_dict):

    model = context['model']

    status = data_dict.get('status',False)

    if status:
        jobs = model.Session.query(HarvestJob) \
                    .filter(HarvestJob.status==status) \
                    .all()
    else:
        jobs = model.Session.query(HarvestJob).all()

    return [harvest_job_dictize(job,context) for job in jobs]

def harvest_object_show(context,data_dict):

    id = data_dict.get('id')
    attr = data_dict.get('attr',None)

    obj = HarvestObject.get(id,attr=attr)
    if not obj:
        raise NotFound

    return harvest_object_dictize(obj,context)

def harvest_object_list(context,data_dict):

    model = context['model']

    only_current = data_dict.get('only_current',True)

    if only_current:
        objects = model.Session.query(HarvestObject) \
                    .filter(HarvestObject.current==True) \
                    .all()
    else:
        objects = model.Session.query(HarvestObject).all()

    return [getattr(obj,'id') for obj in objects]
