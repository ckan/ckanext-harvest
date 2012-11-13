from sqlalchemy import distinct

from ckan.model import Package,Group
from ckanext.harvest.model import HarvestSource, HarvestJob, HarvestObject, \
                                  HarvestGatherError, HarvestObjectError


def harvest_source_dictize(source, context):
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
    out['source'] = job.source_id
    out['objects'] = []
    out['gather_errors'] = []

    for obj in job.objects:
        out['objects'].append(obj.as_dict())

    for error in job.gather_errors:
        out['gather_errors'].append(error.as_dict())

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

    model = context.get('model')
    detailed = context.get('detailed',True)

    out = dict()

    job_count = HarvestJob.filter(source=source).count()

    out = {
           'job_count': 0,
           'next_harvest':'',
           'last_harvest_request':'',
           'last_harvest_statistics':{'added':0,'updated':0,'errors':0},
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

        #Get HarvestObjects from last job whit links to packages
        if detailed:
            last_objects = [obj for obj in last_job.objects if obj.package is not None]

            if len(last_objects) == 0:
                # No packages added or updated
                out['last_harvest_statistics']['added'] = 0
                out['last_harvest_statistics']['updated'] = 0
            else:
                # Check wether packages were added or updated
                for last_object in last_objects:
                    # Check if the same package had been linked before
                    previous_objects = model.Session.query(HarvestObject) \
                                             .filter(HarvestObject.package==last_object.package) \
                                             .count()

                    if previous_objects == 1:
                        # It didn't previously exist, it has been added
                        out['last_harvest_statistics']['added'] += 1
                    else:
                        # Pacakge already existed, but it has been updated
                        out['last_harvest_statistics']['updated'] += 1

        # Last harvest errors
        # We have the gathering errors in last_job.gather_errors, so let's also
        # get also the object errors.
        object_errors = model.Session.query(HarvestObjectError).join(HarvestObject) \
                            .filter(HarvestObject.job==last_job)

        out['last_harvest_statistics']['errors'] = len(last_job.gather_errors) \
                                            + object_errors.count()
        if detailed:
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

