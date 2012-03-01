import os
from logging import getLogger

from genshi.input import HTML
from genshi.filters import Transformer

import ckan.lib.helpers as h

from ckan.plugins import implements, SingletonPlugin
from ckan.plugins import IRoutes, IConfigurer
from ckan.plugins import IConfigurable, IActions, IAuthFunctions
from ckanext.harvest.model import setup

log = getLogger(__name__)

class Harvest(SingletonPlugin):

    implements(IConfigurable)
    implements(IRoutes, inherit=True)
    implements(IConfigurer, inherit=True)
    implements(IActions)
    implements(IAuthFunctions)

    def configure(self, config):
        setup()

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
        here = os.path.dirname(__file__)
        template_dir = os.path.join(here, 'templates')
        public_dir = os.path.join(here, 'public')
        if config.get('extra_template_paths'):
            config['extra_template_paths'] += ',' + template_dir
        else:
            config['extra_template_paths'] = template_dir
        if config.get('extra_public_paths'):
            config['extra_public_paths'] += ',' + public_dir
        else:
            config['extra_public_paths'] = public_dir

    def get_actions(self):
        from ckanext.harvest.logic.action.get import (harvest_source_show,
                                                      harvest_source_list,
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
        from ckanext.harvest.logic.auth.get import (harvest_source_show,
                                                      harvest_source_list,
                                                      harvest_job_show,
                                                      harvest_job_list,
                                                      harvest_object_show,
                                                      harvest_object_list,
                                                      harvesters_info_show,)
        from ckanext.harvest.logic.auth.create import (harvest_source_create,
                                                         harvest_job_create,
                                                         harvest_job_create_all,)
        from ckanext.harvest.logic.auth.update import (harvest_source_update,
                                                         harvest_objects_import,
                                                         harvest_jobs_run)
        from ckanext.harvest.logic.auth.delete import (harvest_source_delete,)

        return {
            'harvest_source_show': harvest_source_show,
            'harvest_source_list': harvest_source_list,
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

