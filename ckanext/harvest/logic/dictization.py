from sqlalchemy import distinct, func
import ckan.logic as logic

from ckan.model import Package,Group
from ckanext.harvest.model import HarvestSource, HarvestJob, HarvestObject, \
                                  HarvestGatherError, HarvestObjectError


def harvest_source_dictize(source, context):
    '''
    TODO: Deprecated
    '''

    out = source.as_dict()

    out['publisher_title'] = u''

    publisher_id = out.get('publisher_id')
    if publisher_id:
        group  = Group.get(publisher_id)
        if group:
            out['publisher_title'] = group.title

    out['status'] = _get_source_status(source, context)


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
                        'errors': 0, 'deleted': 0}
        for status, count in stats:
            out['stats'][status] = count

        # We actually want to check which objects had errors, because they
        # could have been added/updated anyway (eg bbox errors)
        count = model.Session.query(func.distinct(HarvestObjectError.harvest_object_id)) \
                          .join(HarvestObject) \
                          .filter(HarvestObject.harvest_job_id==job.id) \
                          .count()
        if count > 0:
          out['stats']['errored'] = count

        # Add gather errors to the error count
        count = model.Session.query(HarvestGatherError) \
                          .filter(HarvestGatherError.harvest_job_id==job.id) \
                          .count()
        if count > 0:
          out['stats']['errored'] = out['stats'].get('errored', 0) + count

    if context.get('return_error_summary', True):
        q = model.Session.query(HarvestObjectError.message, \
                                func.count(HarvestObjectError.message).label('error_count')) \
                          .join(HarvestObject) \
                          .filter(HarvestObject.harvest_job_id==job.id) \
                          .group_by(HarvestObjectError.message) \
                          .order_by('error_count desc') \
                          .limit(context.get('error_summmary_limit', 20))
        out['object_error_summary'] = q.all()
        q = model.Session.query(HarvestGatherError.message, \
                                func.count(HarvestGatherError.message).label('error_count')) \
                          .filter(HarvestGatherError.harvest_job_id==job.id) \
                          .group_by(HarvestGatherError.message) \
                          .order_by('error_count desc') \
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

def _get_source_status(source, context):
    '''
    TODO: Deprecated, use harvest_source_show_status instead
    '''

    model = context.get('model')
    detailed = context.get('detailed',True)

    out = dict()

    job_count = HarvestJob.filter(source=source).count()

    out = {
           'job_count': 0,
           'next_harvest':'',
           'last_harvest_request':'',
           'last_harvest_statistics':{'added':0,'updated':0,'errors':0,'deleted':0},
           'last_harvest_errors':{'gather':[],'object':[]},
           'overall_statistics':{'added':0, 'errors':0},
           'packages':[]}

    if not job_count:
        out['msg'] = 'No jobs yet'
        return out
    else:
        out['job_count'] = job_count

    # Get next scheduled job
    next_job = HarvestJob.filter(source=source,status=u'New').first()
    if next_job:
        out['next_harvest'] = 'Scheduled'
    else:
        out['next_harvest'] = 'Not yet scheduled'

    # Get the last finished job
    last_job = HarvestJob.filter(source=source,status=u'Finished') \
               .order_by(HarvestJob.created.desc()).first()

    if last_job:
        #TODO: Should we encode the dates as strings?
        out['last_harvest_request'] = str(last_job.gather_finished)

        if detailed:
            harvest_job_dict = harvest_job_dictize(last_job, context)
                # No packages added or updated
            statistics = out['last_harvest_statistics']
            statistics['added'] = harvest_job_dict['stats'].get('new',0)
            statistics['updated'] = harvest_job_dict['stats'].get('updated',0)
            statistics['deleted'] = harvest_job_dict['stats'].get('deleted',0)
            statistics['errors'] = (harvest_job_dict['stats'].get('errored',0) +
                                    len(last_job.gather_errors))

        if detailed:
            # We have the gathering errors in last_job.gather_errors, so let's also
            # get also the object errors.
            object_errors = model.Session.query(HarvestObjectError).join(HarvestObject) \
                                .filter(HarvestObject.job==last_job)
            for gather_error in last_job.gather_errors:
                out['last_harvest_errors']['gather'].append(gather_error.message)

            for object_error in object_errors:
                err = {'object_id':object_error.object.id,'object_guid':object_error.object.guid,'message': object_error.message}
                out['last_harvest_errors']['object'].append(err)

        # Overall statistics
        packages = model.Session.query(distinct(HarvestObject.package_id),Package.name) \
                .join(Package).join(HarvestSource) \
                .filter(HarvestObject.source==source) \
                .filter(HarvestObject.current==True) \
                .filter(Package.state==u'active')

        out['overall_statistics']['added'] = packages.count()
        if detailed:
            for package in packages:
                out['packages'].append(package.name)

        gather_errors = model.Session.query(HarvestGatherError) \
                .join(HarvestJob).join(HarvestSource) \
                .filter(HarvestJob.source==source).count()

        object_errors = model.Session.query(HarvestObjectError) \
                .join(HarvestObject).join(HarvestJob).join(HarvestSource) \
                .filter(HarvestJob.source==source).count()
        out['overall_statistics']['errors'] = gather_errors + object_errors
    else:
        out['last_harvest_request'] = 'Not yet harvested'

    return out

