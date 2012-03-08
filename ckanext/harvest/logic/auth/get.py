from ckan.lib.base import _
from ckan.authz import Authorizer

def harvest_source_show(context,data_dict):
    model = context['model']
    user = context.get('user')

    if not Authorizer().is_sysadmin(user):
        return {'success': False, 'msg': _('User %s not authorized to read this harvest source') % str(user)}
    else:
        return {'success': True}

def harvest_source_list(context,data_dict):
    model = context['model']
    user = context.get('user')

    if not Authorizer().is_sysadmin(user):
        return {'success': False, 'msg': _('User %s not authorized to see the harvest sources') % str(user)}
    else:
        return {'success': True}


def harvest_job_show(context,data_dict):
    model = context['model']
    user = context.get('user')

    if not Authorizer().is_sysadmin(user):
        return {'success': False, 'msg': _('User %s not authorized to read this harvest job') % str(user)}
    else:
        return {'success': True}

def harvest_job_list(context,data_dict):
    model = context['model']
    user = context.get('user')

    if not Authorizer().is_sysadmin(user):
        return {'success': False, 'msg': _('User %s not authorized to see the harvest jobs') % str(user)}
    else:
        return {'success': True}

def harvest_object_show(context,data_dict):
    model = context['model']
    user = context.get('user')

    if not Authorizer().is_sysadmin(user):
        return {'success': False, 'msg': _('User %s not authorized to read this harvest object') % str(user)}
    else:
        return {'success': True}

def harvest_object_list(context,data_dict):
    model = context['model']
    user = context.get('user')

    if not Authorizer().is_sysadmin(user):
        return {'success': False, 'msg': _('User %s not authorized to see the harvest objects') % str(user)}
    else:
        return {'success': True}

def harvesters_info_show(context,data_dict):
    model = context['model']
    user = context.get('user')

    if not Authorizer().is_sysadmin(user):
        return {'success': False, 'msg': _('User %s not authorized to see the harvesters information') % str(user)}
    else:
        return {'success': True}

