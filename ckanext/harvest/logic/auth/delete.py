from ckan.lib.base import _
from ckan.authz import Authorizer

def harvest_source_delete(context,data_dict):
    model = context['model']
    user = context.get('user')

    if not Authorizer().is_sysadmin(user):
        return {'success': False, 'msg': _('User %s not authorized to delete harvest sources') % str(user)}
    else:
        return {'success': True}


