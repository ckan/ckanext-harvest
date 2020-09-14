from ckan.plugins import toolkit as pt

from ckanext.harvest.logic.auth import get_job_object


def auth_allow_anonymous_access(auth_function):
    '''
        Local version of the auth_allow_anonymous_access decorator that only
        calls the actual toolkit decorator if the CKAN version supports it
    '''
    if pt.check_ckan_version(min_version='2.2'):
        auth_function = pt.auth_allow_anonymous_access(auth_function)

    return auth_function


@auth_allow_anonymous_access
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


@auth_allow_anonymous_access
def harvest_source_show_status(context, data_dict):
    '''
        Authorization check for getting the status of a harvest source

        It forwards the checks to harvest_source_show.
    '''
    return harvest_source_show(context, data_dict)


@auth_allow_anonymous_access
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


@auth_allow_anonymous_access
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


@auth_allow_anonymous_access
def harvesters_info_show(context, data_dict):
    '''
        Authorization check for getting information about the available
        harvesters

        Everybody can do it
    '''
    return {'success': True}


def harvest_get_notifications_recipients(context, data_dict):
   # Only sysadmins can access this
    return {'success': False}
