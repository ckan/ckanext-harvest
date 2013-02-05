from ckan.plugins import toolkit as pt

from ckanext.harvest.logic.auth import get_job_object


def harvest_source_show(context, data_dict):
    '''
        Authorization check for getting the details of a harvest source

        It forwards the checks to package_show, which will check for
        organization membership, whether if sysadmin, etc according to the
        instance configuration.
    '''
    model = context.get('model')
    user = context.get('user')
    source_id = data_dict['id']

    pkg = model.Package.get(source_id)
    if not pkg:
        raise pt.ObjectNotFound(pt._('Harvest source not found'))

    context['package'] = pkg

    try:
        pt.check_access('package_show', context, data_dict)
        return {'success': True}
    except pt.NotAuthorized:
        return {'success': False,
                'msg': pt._('User {0} not authorized to read harvest source {1}')
                .format(user, source_id)}


def harvest_source_list(context, data_dict):
    '''
        Authorization check for getting a list of harveste sources

        Everybody can do it
    '''
    return {'success': True}


def harvest_job_show(context, data_dict):
    '''
        Authorization check for getting the details of a harvest job

        It forwards the checks to harvest_source_update, ie if the user can
        update the parent source (eg create new jobs), she can get the details
        for the job, including the reports
    '''
    user = context.get('user')
    job = get_job_object(context, data_dict)

    try:
        pt.check_access('harvest_source_update',
                        context,
                        {'id': job.source.id})
        return {'success': True}
    except pt.NotAuthorized:
        return {'success': False,
                'msg': pt._('User {0} not authorized to see jobs from source {1}')
                .format(user, job.source.id)}


def harvest_job_list(context, data_dict):
    '''
        Authorization check for getting a list of jobs for a source

        It forwards the checks to harvest_source_update, ie if the user can
        update the parent source (eg create new jobs), she can get the list of
        jobs
    '''
    user = context.get('user')
    source_id = data_dict['source_id']

    try:
        pt.check_access('harvest_source_update',
                        context,
                        {'id': source_id})
        return {'success': True}
    except pt.NotAuthorized:
        return {'success': False,
                'msg': pt._('User {0} not authorized to list jobs for source {1}')
                .format(user, source_id)}



def harvest_object_show(context, data_dict):
    '''
        Authorization check for getting the contents of a harvest object

        Everybody can do it
    '''
    return {'success': True}


def harvest_object_list(context, data_dict):
    '''
    TODO: remove
    '''
    return {'success': True}


def harvesters_info_show(context, data_dict):
    '''
        Authorization check for getting information about the available
        harvesters

        Everybody can do it
    '''
    return {'success': True}
