from ckan.lib.base import _
from ckan.logic import NotFound
from ckan.authz import Authorizer
from ckan.model import User

from ckanext.harvest.model import HarvestSource
from ckanext.harvest.logic.auth import get_source_object, get_job_object, get_obj_object

def harvest_source_show(context,data_dict):
    model = context['model']
    user = context.get('user','')

    source = get_source_object(context,data_dict)

    # Non-logged users can not read the source
    if not user:
        return {'success': False, 'msg': _('Non-logged in users are not authorized to see harvest sources')}

    # Sysadmins can read the source
    if Authorizer().is_sysadmin(user):
        return {'success': True}

    # Check if the source publisher id exists on the user's groups
    user_obj = User.get(user)
    if not user_obj or not source.publisher_id in [g.id for g in user_obj.get_groups(u'publisher')]:
        return {'success': False, 'msg': _('User %s not authorized to read harvest source %s') % (str(user),source.id)}
    else:
        return {'success': True}

def harvest_source_list(context,data_dict):

    model = context['model']
    user = context.get('user')

    # Here we will just check that the user is logged in.
    # The logic action will return an empty list if the user does not
    # have permissons on any source.
    if not user:
        return {'success': False, 'msg': _('Only logged users are authorized to see their sources')}
    else:
        user_obj = User.get(user)
        assert user_obj
        
        # Only users belonging to a publisher can list sources,
        # unless they are sysadmins
        if Authorizer().is_sysadmin(user_obj):
            return {'success': True}
        if len(user_obj.get_groups(u'publisher')) > 0:
            return {'success': True}
        else:
            return {'success': False, 'msg': _('User %s must belong to a publisher to list harvest sources') % str(user)}

def harvest_job_show(context,data_dict):
    model = context['model']
    user = context.get('user')

    job = get_job_object(context,data_dict)

    if not user:
        return {'success': False, 'msg': _('Non-logged in users are not authorized to see harvest jobs')}

    if Authorizer().is_sysadmin(user):
        return {'success': True}

    user_obj = User.get(user)
    if not user_obj or not job.source.publisher_id in [g.id for g in user_obj.get_groups(u'publisher')]:
        return {'success': False, 'msg': _('User %s not authorized to read harvest job %s') % (str(user),job.id)}
    else:
        return {'success': True}

def harvest_job_list(context,data_dict):
    model = context['model']
    user = context.get('user')

    # Check user is logged in
    if not user:
        return {'success': False, 'msg': _('Only logged users are authorized to see their sources')}

    user_obj = User.get(user)

    # Checks for non sysadmin users
    if not Authorizer().is_sysadmin(user):
        if not user_obj or len(user_obj.get_groups(u'publisher')) == 0:
            return {'success': False, 'msg': _('User %s must belong to a publisher to list harvest jobs') % str(user)}

        source_id = data_dict.get('source_id',False)
        if not source_id:
            return {'success': False, 'msg': _('Only sysadmins can list all harvest jobs') % str(user)}

        source = HarvestSource.get(source_id)
        if not source:
            raise NotFound

        if not source.publisher_id in [g.id for g in user_obj.get_groups(u'publisher')]:
            return {'success': False, 'msg': _('User %s not authorized to list jobs from source %s') % (str(user),source.id)}

    return {'success': True}

def harvest_object_show(context,data_dict):
    model = context['model']
    user = context.get('user')

    obj = get_obj_object(context,data_dict)

    if context.get('ignore_auth', False):
        return {'success': True}

    if not user:
        return {'success': False, 'msg': _('Non-logged in users are not authorized to see harvest objects')}

    if Authorizer().is_sysadmin(user):
        return {'success': True}

    user_obj = User.get(user)
    if not user_obj or not obj.source.publisher_id in [g.id for g in user_obj.get_groups(u'publisher')]:
        return {'success': False, 'msg': _('User %s not authorized to read harvest object %s') % (str(user),obj.id)}
    else:
        return {'success': True}

def harvest_object_list(context,data_dict):
    model = context['model']
    user = context.get('user')

    # Check user is logged in
    if not user:
        return {'success': False, 'msg': _('Only logged users are authorized to see their sources')}

    user_obj = User.get(user)

    # Checks for non sysadmin users
    if not Authorizer().is_sysadmin(user):
        if not user_obj or len(user_obj.get_groups(u'publisher')) == 0:
            return {'success': False, 'msg': _('User %s must belong to a publisher to list harvest objects') % str(user)}

        source_id = data_dict.get('source_id',False)
        if not source_id:
            return {'success': False, 'msg': _('Only sysadmins can list all harvest objects') % str(user)}

        source = HarvestSource.get(source_id)
        if not source:
            raise NotFound

        if not source.publisher_id in [g.id for g in user_obj.get_groups(u'publisher')]:
            return {'success': False, 'msg': _('User %s not authorized to list objects from source %s') % (str(user),source.id)}

    return {'success': True}

def harvesters_info_show(context,data_dict):
    model = context['model']
    user = context.get('user','')

    # Non-logged users can not create sources
    if not user:
        return {'success': False, 'msg': _('Non-logged in users can not see the harvesters info')}

    # Sysadmins and the rest of logged users can see the harvesters info,
    # as long as they belong to a publisher
    user_obj = User.get(user)
    if not user_obj or not Authorizer().is_sysadmin(user) and len(user_obj.get_groups(u'publisher')) == 0:
        return {'success': False, 'msg': _('User %s must belong to a publisher to see the harvesters info') % str(user)}
    else:
        return {'success': True}

