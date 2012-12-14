from ckan.lib.base import _
from ckan.model import User

def harvest_source_delete(context,data_dict):
    model = context['model']
    user = context.get('user')
    user = User.get(user)
    if not user.sysadmin:
        return {'success': False, 'msg': _('User %s not authorized to delete harvest sources') % str(user)}
    else:
        return {'success': True}


