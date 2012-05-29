from ckan.lib.base import _
from ckan.authz import Authorizer
from ckan.model import User

from ckanext.harvest.logic.auth import get_source_object

def harvest_source_delete(context,data_dict):
    model = context['model']
    user = context.get('user','')
    
    source = get_source_object(context,data_dict)
    
    # Non-logged users cannot delete this source
    if not user:
        return {'success': False, 'msg': _('Non-logged in users are not authorized to delete harvest sources')} 

    # Sysadmins can delete the source
    if Authorizer().is_sysadmin(user):
        return {'success': True}
        
    # Check if the source publisher id exists on the user's groups
    user_obj = User.get(user)
    if not user_obj or not source.publisher_id in [g.id for g in user_obj.get_groups(u'publisher')]:
        return {'success': False, 'msg': _('User %s not authorized to delete harvest source %s') % (str(user),source.id)}
    else:
        return {'success': True}

