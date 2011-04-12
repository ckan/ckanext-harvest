import os
from logging import getLogger

from genshi.input import HTML
from genshi.filters import Transformer

import ckan.lib.helpers as h

from ckan.plugins import implements, SingletonPlugin
from ckan.plugins import IRoutes, IConfigurer
from ckan.plugins import IConfigurable, IGenshiStreamFilter

log = getLogger(__name__)

class Harvest(SingletonPlugin):
    
    implements(IConfigurable)
    implements(IRoutes, inherit=True)
    implements(IConfigurer, inherit=True)
    
    def configure(self, config):
        pass

    def before_map(self, map):
        map.connect('harvest', '/harvest',
            controller='ckanext.harvest.controllers.view:ViewController',
            action='index')
            
        map.connect('harvest_create_form', '/harvest/create',
            controller='ckanext.harvest.controllers.view:ViewController',
            conditions=dict(method=['GET']),
            action='create')

        map.connect('harvest_create', '/harvest/create',
            controller='ckanext.harvest.controllers.view:ViewController',
            conditions=dict(method=['POST']),
            action='create')

        map.connect('harvest_show', '/harvest/:id',
            controller='ckanext.harvest.controllers.view:ViewController',
            action='show')

        map.connect('harvest_edit', '/harvest/:id/edit',
            controller='ckanext.harvest.controllers.view:ViewController',
            action='edit')

        map.connect('harvest_delete', '/harvest/:id/delete',
            controller='ckanext.harvest.controllers.view:ViewController',
            action='delete')

        map.connect('harvesting_job_create', '/harvest/:id/refresh',
            controller='ckanext.harvest.controllers.view:ViewController',
            action='create_harvesting_job')
      
        return map

    def update_config(self, config):
        here = os.path.dirname(__file__)
        rootdir = os.path.dirname(os.path.dirname(here))

        template_dir = os.path.join(rootdir, 'templates')
        public_dir = os.path.join(rootdir, 'public')
        
        if config.get('extra_template_paths'):
            config['extra_template_paths'] += ','+template_dir
        else:
            config['extra_template_paths'] = template_dir
        if config.get('extra_public_paths'):
            config['extra_public_paths'] += ','+public_dir
        else:
            config['extra_public_paths'] = public_dir
