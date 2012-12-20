from ckan.lib.base import _

def harvest_source_show(context,data_dict):
    model = context['model']
    user = context.get('user')

    user_obj = model.User.get(user)
    if not user_obj or not user_obj.sysadmin:
        return {'success': False, 'msg': _('User %s not authorized to read this harvest source') % str(user)}
    else:
        return {'success': True}

def harvest_source_list(context,data_dict):
    model = context['model']
    user = context.get('user')

    user_obj = model.User.get(user)
    if not user_obj or not user_obj.sysadmin:
        return {'success': False, 'msg': _('User %s not authorized to see the harvest sources') % str(user)}
    else:
        return {'success': True}


def harvest_job_show(context,data_dict):
    model = context['model']
    user = context.get('user')

    user_obj = model.User.get(user)
    if not user_obj or not user_obj.sysadmin:
        return {'success': False, 'msg': _('User %s not authorized to read this harvest job') % str(user)}
    else:
        return {'success': True}

def harvest_job_list(context,data_dict):
    model = context['model']
    user = context.get('user')

    user_obj = model.User.get(user)
    if not user_obj or not user_obj.sysadmin:
        return {'success': False, 'msg': _('User %s not authorized to see the harvest jobs') % str(user)}
    else:
        return {'success': True}

def harvest_object_show(context,data_dict):
    model = context['model']
    user = context.get('user')

    return {'success': True}

def harvest_object_list(context,data_dict):
    model = context['model']
    user = context.get('user')

    user_obj = model.User.get(user)
    if not user_obj or not user_obj.sysadmin:
        return {'success': False, 'msg': _('User %s not authorized to see the harvest objects') % str(user)}
    else:
        return {'success': True}

def harvesters_info_show(context,data_dict):
    model = context['model']
    user = context.get('user')

    user_obj = model.User.get(user)
    if not user_obj or not user_obj.sysadmin:
        return {'success': False, 'msg': _('User %s not authorized to see the harvesters information') % str(user)}
    else:
        return {'success': True}

