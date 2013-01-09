from ckan.lib.base import _
from ckan.new_authz import is_sysadmin

def harvest_source_create(context, data_dict):
    model = context['model']
    user = context.get('user')

    if not is_sysadmin(user):
        return {'success': False, 'msg': _('User %s not authorized to create harvest sources') % str(user)}
    else:
        return {'success': True}

def harvest_job_create(context, data_dict):
    model = context['model']
    user = context.get('user')

    if not is_sysadmin(user):
        return {'success': False, 'msg': _('User %s not authorized to create harvest jobs') % str(user)}
    else:
        return {'success': True}

def harvest_job_create_all(context, data_dict):
    model = context['model']
    user = context.get('user')

    if not is_sysadmin(user):
        return {'success': False, 'msg': _('User %s not authorized to create harvest jobs for all sources') % str(user)}
    else:
        return {'success': True}

