from sqlalchemy import distinct, func, text

from ckan.model import Package, Group
from ckan import logic
from ckanext.harvest.model import (HarvestSource, HarvestJob, HarvestObject,
                                   HarvestGatherError, HarvestObjectError)


def harvest_source_dictize(source, context, last_job_status=False):
    out = source.as_dict()

    out['publisher_title'] = u''

    publisher_id = out.get('publisher_id')
    if publisher_id:
        group = Group.get(publisher_id)
        if group:
            out['publisher_title'] = group.title

    out['status'] = _get_source_status(source, context)

    if last_job_status:
        source_status = logic.get_action('harvest_source_show_status')(context, {'id': source.id})
        out['last_job_status'] = source_status.get('last_job', {})

    return out


def harvest_job_dictize(job, context):
    out = job.as_dict()

    model = context['model']

    if context.get('return_stats', True):
        stats = model.Session.query(
            HarvestObject.report_status,
            func.count(HarvestObject.id).label('total_objects'))\
            .filter_by(harvest_job_id=job.id)\
            .group_by(HarvestObject.report_status).all()
        out['stats'] = {'added': 0, 'updated': 0, 'not modified': 0,
                        'errored': 0, 'deleted': 0}
        for status, count in stats:
            out['stats'][status] = count

        # We actually want to check which objects had errors, because they
        # could have been added/updated anyway (eg bbox errors)
        count = model.Session.query(
            func.distinct(HarvestObjectError.harvest_object_id)) \
            .join(HarvestObject) \
            .filter(HarvestObject.harvest_job_id == job.id) \
            .count()
        if count > 0:
            out['stats']['errored'] = count

        # Add gather errors to the error count
        count = model.Session.query(HarvestGatherError) \
            .filter(HarvestGatherError.harvest_job_id == job.id) \
            .count()
        if count > 0:
            out['stats']['errored'] = out['stats'].get('errored', 0) + count

    if context.get('return_error_summary', True):
        q = model.Session.query(
            HarvestObjectError.message,
            func.count(HarvestObjectError.message).label('error_count')) \
            .join(HarvestObject) \
            .filter(HarvestObject.harvest_job_id == job.id) \
            .group_by(HarvestObjectError.message) \
            .order_by(text('error_count desc')) \
            .limit(context.get('error_summmary_limit', 20))
        out['object_error_summary'] = q.all()
        q = model.Session.query(
            HarvestGatherError.message,
            func.count(HarvestGatherError.message).label('error_count')) \
            .filter(HarvestGatherError.harvest_job_id == job.id) \
            .group_by(HarvestGatherError.message) \
            .order_by(text('error_count desc')) \
            .limit(context.get('error_summmary_limit', 20))
        out['gather_error_summary'] = q.all()
    return out


def harvest_object_dictize(obj, context):
    out = obj.as_dict()
    out['source'] = obj.harvest_source_id
    out['job'] = obj.harvest_job_id

    if obj.package:
        out['package'] = obj.package.id

    out['errors'] = []
    for error in obj.errors:
        out['errors'].append(error.as_dict())

    out['extras'] = {}
    for extra in obj.extras:
        out['extras'][extra.key] = extra.value

    return out


def harvest_log_dictize(obj, context):
    out = obj.as_dict()
    del out['id']

    return out


def _get_source_status(source, context):
    '''
    TODO: Deprecated, use harvest_source_show_status instead
    '''

    model = context.get('model')

    out = dict()

    job_count = HarvestJob.filter(source=source).count()

    out = {
        'job_count': 0,
        'next_harvest': '',
        'last_harvest_request': '',
        }

    if not job_count:
        out['msg'] = 'No jobs yet'
        return out
    else:
        out['job_count'] = job_count

    # Get next scheduled job
    next_job = HarvestJob.filter(source=source, status=u'New').first()
    if next_job:
        out['next_harvest'] = 'Scheduled'
    else:
        out['next_harvest'] = 'Not yet scheduled'

    # Get the last finished job
    last_job = HarvestJob.filter(source=source, status=u'Finished') \
        .order_by(HarvestJob.created.desc()).first()

    if last_job:
        out['last_harvest_request'] = str(last_job.gather_finished)
    else:
        out['last_harvest_request'] = 'Not yet harvested'

    return out
