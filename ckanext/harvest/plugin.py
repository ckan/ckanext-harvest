import os
from logging import getLogger

from pylons import config
from genshi.input import HTML
from genshi.filters import Transformer

import ckan.lib.helpers as h

import ckan.plugins as p

from ckanext.harvest.model import setup as model_setup

log = getLogger(__name__)
assert not log.disabled

class Harvest(p.SingletonPlugin):

    p.implements(p.IConfigurable)
    p.implements(p.IRoutes, inherit=True)
    p.implements(p.IConfigurer, inherit=True)
    p.implements(p.IActions)
    p.implements(p.IAuthFunctions)

    def configure(self, config):

        auth_profile = config.get('ckan.harvest.auth.profile',None)

        if auth_profile:
            # Check if auth profile exists
            module_root = 'ckanext.harvest.logic.auth'
            module_path = '%s.%s' % (module_root, auth_profile)
            try:
                module = __import__(module_path)
            except ImportError,e:
                raise ImportError('Unknown auth profile: %s' % auth_profile)

            # If we are using the publisher auth profile, make sure CKAN core
            # also uses it.
            if auth_profile == 'publisher' and \
                not config.get('ckan.auth.profile','') == 'publisher':
                raise Exception('You must enable the "publisher" auth profile'
                      +' in CKAN in order to use it on the harvest extension'
                      +' (adding "ckan.auth.profile=publisher" to your ini file)')

        # Setup harvest model
        model_setup()

    def before_map(self, map):

        controller = 'ckanext.harvest.controllers.view:ViewController'
        map.connect('harvest', '/harvest',controller=controller,action='index')

        map.connect('/harvest/new', controller=controller, action='new')
        map.connect('/harvest/edit/:id', controller=controller, action='edit')
        map.connect('/harvest/delete/:id',controller=controller, action='delete')
        map.connect('/harvest/:id', controller=controller, action='read')

        map.connect('harvesting_job_create', '/harvest/refresh/:id',controller=controller,
                action='create_harvesting_job')

        map.connect('/harvest/object/:id', controller=controller, action='show_object')

        return map

    def update_config(self, config):
        # check if new templates
        templates = 'templates'
        if p.toolkit.check_ckan_version(min_version='2.0'):
            if not p.toolkit.asbool(config.get('ckan.legacy_templates', False)):
                templates = 'templates_new'
        p.toolkit.add_template_directory(config, templates)
        p.toolkit.add_public_directory(config, 'public')

    def get_actions(self):
        from ckanext.harvest.logic.action.get import (harvest_source_show,
                                                      harvest_source_list,
                                                      harvest_source_for_a_dataset,
                                                      harvest_job_show,
                                                      harvest_job_list,
                                                      harvest_object_show,
                                                      harvest_object_list,
                                                      harvesters_info_show,)
        from ckanext.harvest.logic.action.create import (harvest_source_create,
                                                         harvest_job_create,
                                                         harvest_job_create_all,)
        from ckanext.harvest.logic.action.update import (harvest_source_update,
                                                         harvest_objects_import,
                                                         harvest_jobs_run)
        from ckanext.harvest.logic.action.delete import (harvest_source_delete,)

        return {
            'harvest_source_show': harvest_source_show,
            'harvest_source_list': harvest_source_list,
            'harvest_source_for_a_dataset': harvest_source_for_a_dataset,
            'harvest_job_show': harvest_job_show,
            'harvest_job_list': harvest_job_list,
            'harvest_object_show': harvest_object_show,
            'harvest_object_list': harvest_object_list,
            'harvesters_info_show': harvesters_info_show,
            'harvest_source_create': harvest_source_create,
            'harvest_job_create': harvest_job_create,
            'harvest_job_create_all': harvest_job_create_all,
            'harvest_source_update': harvest_source_update,
            'harvest_source_delete': harvest_source_delete,
            'harvest_objects_import': harvest_objects_import,
            'harvest_jobs_run':harvest_jobs_run
        }

    def get_auth_functions(self):

        module_root = 'ckanext.harvest.logic.auth'
        auth_profile = config.get('ckan.harvest.auth.profile', '')

        auth_functions = _get_auth_functions(module_root)
        if auth_profile:
            module_root = '%s.%s' % (module_root, auth_profile)
            auth_functions = _get_auth_functions(module_root,auth_functions)

        log.debug('Using auth profile at %s' % module_root)

        return auth_functions

def _get_auth_functions(module_root, auth_functions = {}):

    for auth_module_name in ['get', 'create', 'update','delete']:
        module_path = '%s.%s' % (module_root, auth_module_name,)
        try:
            module = __import__(module_path)
        except ImportError,e:
            log.debug('No auth module for action "%s"' % auth_module_name)
            continue

        for part in module_path.split('.')[1:]:
            module = getattr(module, part)

        for key, value in module.__dict__.items():
            if not key.startswith('_'):
                auth_functions[key] = value


    return auth_functions

