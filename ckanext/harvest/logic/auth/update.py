from ckan.lib.base import _
from ckan.authz import Authorizer

def harvest_source_update(context,data_dict):
    model = context['model']
    user = context.get('user')

    if not Authorizer().is_sysadmin(user):
        return {'success': False, 'msg': _('User %s not authorized to update harvest sources') % str(user)}
    else:
        return {'success': True}

def harvest_objects_import(context,data_dict):
    model = context['model']
    user = context.get('user')

    if not Authorizer().is_sysadmin(user):
        return {'success': False, 'msg': _('User %s not authorized to reimport harvest objects') % str(user)}
    else:
        return {'success': True}

def harvest_jobs_run(context,data_dict):
    model = context['model']
    user = context.get('user')

    if not Authorizer().is_sysadmin(user):
        return {'success': False, 'msg': _('User %s not authorized to run the pending harvest jobs') % str(user)}
    else:
        return {'success': True}

