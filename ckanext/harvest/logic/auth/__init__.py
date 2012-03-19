from ckan.logic import NotFound
from ckanext.harvest.model import HarvestSource, HarvestJob, HarvestObject


def get_source_object(context, data_dict = {}):
    if not 'source' in context:
        model = context['model']
        id = data_dict.get('id',None)
        source = HarvestSource.get(id)
        if not source:
            raise NotFound
    else:
        source = context['source']

    return source

def get_job_object(context, data_dict = {}):
    if not 'job' in context:
        model = context['model']
        id = data_dict.get('id',None)
        job = HarvestJob.get(id)
        if not job:
            raise NotFound
    else:
        job = context['job']

    return job

def get_obj_object(context, data_dict = {}):
    if not 'obj' in context:
        model = context['model']
        id = data_dict.get('id',None)
        obj = HarvestObject.get(id)
        if not obj:
            raise NotFound
    else:
        obj = context['obj']

    return obj
