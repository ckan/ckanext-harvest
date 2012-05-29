from ckan.lib.base import _
from ckan.authz import Authorizer
from ckan.model import User

from ckanext.harvest.model import HarvestSource

def harvest_source_create(context,data_dict):
    model = context['model']
    user = context.get('user','')

    # Non-logged users can not create sources
    if not user:
        return {'success': False, 'msg': _('Non-logged in users are not authorized to create harvest sources')}

    # Sysadmins and the rest of logged users can create sources,
    # as long as they belong to a publisher
    user_obj = User.get(user)
    if not user_obj or not Authorizer().is_sysadmin(user) and len(user_obj.get_groups(u'publisher')) == 0:
        return {'success': False, 'msg': _('User %s must belong to a publisher to create harvest sources') % str(user)}
    else:
        return {'success': True}

def harvest_job_create(context,data_dict):
    model = context['model']
    user = context.get('user')

    source_id = data_dict['source_id']

    if not user:
        return {'success': False, 'msg': _('Non-logged in users are not authorized to create harvest jobs')}

    if Authorizer().is_sysadmin(user):
        return {'success': True}

    user_obj = User.get(user)
    source = HarvestSource.get(source_id)
    if not source:
        raise NotFound

    if not user_obj or not source.publisher_id in [g.id for g in user_obj.get_groups(u'publisher')]:
        return {'success': False, 'msg': _('User %s not authorized to create a job for source %s') % (str(user),source.id)}
    else:
        return {'success': True}

def harvest_job_create_all(context,data_dict):
    model = context['model']
    user = context.get('user')

    if not Authorizer().is_sysadmin(user):
        return {'success': False, 'msg': _('Only sysadmins can create harvest jobs for all sources') % str(user)}
    else:
        return {'success': True}

