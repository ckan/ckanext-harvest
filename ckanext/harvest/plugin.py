import os
from logging import getLogger

from genshi.input import HTML
from genshi.filters import Transformer

import ckan.lib.helpers as h

from ckan.plugins import implements, SingletonPlugin
from ckan.plugins import IRoutes, IConfigurer
from ckan.plugins import IConfigurable, IGenshiStreamFilter
from ckanext.harvest.model import setup

log = getLogger(__name__)

class Harvest(SingletonPlugin):
    
    implements(IConfigurable)
    implements(IRoutes, inherit=True)
    implements(IConfigurer, inherit=True)
    
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
      
        return map

    def update_config(self, config):
        here = os.path.dirname(__file__)
        template_dir = os.path.join(here, 'templates')
        public_dir = os.path.join(here, 'public')
        if config.get('extra_template_paths'):
            config['extra_template_paths'] += ','+template_dir
        else:
            config['extra_template_paths'] = template_dir
        if config.get('extra_public_paths'):
            config['extra_public_paths'] += ','+public_dir
        else:
            config['extra_public_paths'] = public_dir
