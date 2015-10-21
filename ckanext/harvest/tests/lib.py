from ckanext.harvest.tests.factories import (HarvestSourceObj, HarvestJobObj,
                                             HarvestObjectObj)
import ckanext.harvest.model as harvest_model
from ckanext.harvest.logic import HarvestJobExists, NoNewHarvestJobError
from ckanext.harvest import queue
from ckan.plugins import toolkit


def run_harvest(url, harvester):
    '''Runs a harvest and returns the results.
    This allows you to test a harvester.
    Queues are avoided as they are a pain in tests.
    '''

    # User creates a harvest source
    source = HarvestSourceObj(url=url)

    # User triggers a harvest, which creates a harvest job
    job = HarvestJobObj(source=source, status='Running')

    # When 'paster harvest run' is called by the regular cron, it would change
    # the job status to Running (wheras here we just set it to that status on
    # creation) and put the job on the gather queue which is consumed by
    # queue.gather_callback, which determines the harvester and then calls
    # gather_stage. We simply call the gather_stage.
    obj_ids = queue.gather_stage(harvester, job)

    # The object ids are put onto the fetch queue, consumed by
    # queue.fetch_callback which calls queue.fetch_and_import_stages
    results_by_guid = {}
    for obj_id in obj_ids:
        harvest_object = harvest_model.HarvestObject.get(obj_id)
        guid = harvest_object.guid
        results_by_guid[guid] = {'obj_id': obj_id}

        queue.fetch_and_import_stages(harvester, harvest_object)
        results_by_guid[guid]['state'] = harvest_object.state
        results_by_guid[guid]['report_status'] = harvest_object.report_status
        if harvest_object.state == 'COMPLETE' and harvest_object.package_id:
            results_by_guid[guid]['dataset'] = \
                toolkit.get_action('package_show')(
                    {}, dict(id=harvest_object.package_id))
        results_by_guid[guid]['errors'] = harvest_object.errors

    # Do 'harvest_jobs_run' to change the job status to 'finished'
    try:
        toolkit.get_action('harvest_jobs_run')({'ignore_auth': True}, {})
    except NoNewHarvestJobError:
        pass

    return results_by_guid
