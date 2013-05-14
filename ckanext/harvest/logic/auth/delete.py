from ckan.plugins import toolkit as pt


def harvest_source_delete(context, data_dict):
    '''
        Authorization check for harvest source deletion

        It forwards the checks to package_delete, which will check for
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
        pt.check_access('package_delete', context, data_dict)
        return {'success': True}
    except pt.NotAuthorized:
        return {'success': False,
                'msg': pt._('User {0} not authorized to delete harvest source {1}').format(user, source_id)}
