from ckan.logic import NotFound, check_access

from ckanext.harvest.model import (HarvestSource, HarvestJob)


def harvest_source_delete(context,data_dict):

    check_access('harvest_source_delete',context,data_dict)

    source_id = data_dict.get('id')
    source = HarvestSource.get(source_id)
    if not source:
        raise NotFound('Harvest source %s does not exist' % source_id)

    # Don't actually delete the record, just flag it as inactive
    source.active = False
    source.save()

    # Abort any pending jobs
    jobs = HarvestJob.filter(source=source,status=u'New')
    if jobs:
        for job in jobs:
            job.status = u'Aborted'
            job.save()

    return True
