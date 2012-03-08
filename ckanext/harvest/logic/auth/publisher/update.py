from ckan.lib.base import _
from ckan.authz import Authorizer
from ckan.model import User

from ckanext.harvest.logic.auth import get_source_object

def harvest_source_update(context,data_dict):
    model = context['model']
    user = context.get('user','')

    source = get_source_object(context,data_dict)

    # Non-logged users can not update this source
    if not user:
        return {'success': False, 'msg': _('Non-logged in users are not authorized to update harvest sources')}

    # Sysadmins can update the source
    if Authorizer().is_sysadmin(user):
        return {'success': True}

    # Check if the source publisher id exists on the user's groups
    user_obj = User.get(user)
    if not user_obj or not source.publisher_id in [g.id for g in user_obj.get_groups(u'publisher',u'admin')]:
        return {'success': False, 'msg': _('User %s not authorized to update harvest source %s') % (str(user),source.id)}
    else:
        return {'success': True}

def harvest_objects_import(context,data_dict):
    model = context['model']
    user = context.get('user')

    # Check user is logged in
    if not user:
        return {'success': False, 'msg': _('Only logged users are authorized to reimport harvest objects')}

    user_obj = User.get(user)

    # Checks for non sysadmin users
    if not Authorizer().is_sysadmin(user):
        if not user_obj or len(user_obj.get_groups(u'publisher',u'admin')) == 0:
            return {'success': False, 'msg': _('User %s must belong to a publisher to reimport harvest objects') % str(user)}

        source_id = data_dict.get('source_id',False)
        if not source_id:
            return {'success': False, 'msg': _('Only sysadmins can reimport all harvest objects') % str(user)}

        source = HarvestSource.get(source_id)
        if not source:
            raise NotFound

        if not source.publisher_id in [g.id for g in user_obj.get_groups(u'publisher',u'admin')]:
            return {'success': False, 'msg': _('User %s not authorized to reimport objects from source %s') % (str(user),source.id)}

    return {'success': True}

def harvest_jobs_run(context,data_dict):
    model = context['model']
    user = context.get('user')

    # Check user is logged in
    if not user:
        return {'success': False, 'msg': _('Only logged users are authorized to run harvest jobs')}

    user_obj = User.get(user)

    # Checks for non sysadmin users
    if not Authorizer().is_sysadmin(user):
        if not user_obj or len(user_obj.get_groups(u'publisher',u'admin')) == 0:
            return {'success': False, 'msg': _('User %s must belong to a publisher to run harvest jobs') % str(user)}

        source_id = data_dict.get('source_id',False)
        if not source_id:
            return {'success': False, 'msg': _('Only sysadmins can run all harvest jobs') % str(user)}

        source = HarvestSource.get(source_id)
        if not source:
            raise NotFound

        if not source.publisher_id in [g.id for g in user_obj.get_groups(u'publisher',u'admin')]:
            return {'success': False, 'msg': _('User %s not authorized to run jobs from source %s') % (str(user),source.id)}

    return {'success': True}

